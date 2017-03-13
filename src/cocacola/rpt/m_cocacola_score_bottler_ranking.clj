;#*********************************
;# [intro]
;#   author=larluo@spiderdt.com
;#   func=partition algorithm for data warehouse
;#=================================
;# [param]
;#   tabname=staging table name
;#   prt_cols_str=ods partition cols
;#=================================
;# [caller]
;#   [PORG] bolome.dau
;#   [PORG] bolome.event
;#   [PORG] bolome.inventory
;#   [PORG] bolome.order
;#   [PORG] bolome.product_category
;#   [PORG] bolome.show
;#=================================
;# [version]
;#   v1_0=2017-01-24@chong {modify namespace}
;#*********************************

(ns cocacola.rpt.m-cocacola-score_bottler_ranking
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]]
            [common.trgx :refer :all])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "channel_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-dt-kv
  (<- [?bg ?bottler ?channel !code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?dw-dt-kv]
      (score-tap-in :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort)
      (collect-kv ?dw-dt ?value :> ?dw-dt-kv)))

(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?max-value !pp-value !last-dec-value]
      (score-dt-kv :> ?bg ?bottler ?channel !code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?dw-dt-kv)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month) ?dw-dt :> !prev-last-day !prev-last-month)
      ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value)
      (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month :> ?value !pp-value !last-dec-value)))

(defn score-trgx [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?bg_sort ?bottler_sort ?trgx-data]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?max-value !pp-value !last-dec-value)
      ((vars->kv [:value :max_value :pp_value :last_dec_value]) ?value ?max-value !pp-value !last-dec-value :> ?value-tuple-kv)
      (str "[" !code "]" ?item :> ?code-item)
      (collect-kv ?code-item ?value-tuple-kv :> ?code-item-kv)
      ((kv->trgx (latest-trgx-merge)) ?code-item-kv :> ?trgx-data)))

(defn score-bottler_ranking-report [[start-dt end-dt]]
  #_(??- (c/first-n (score-bottler_ranking-report ["2016-01-01" "2017-01-01"]) 10))
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-trgx [start-dt end-dt]) :> ?dw-dt ?src-bg ?src-bottler ?bg_sort ?bottler_sort ?src-trgx-data)
      (identity ["cocacola" "score" "bottler_ranking" ""] :> ?project ?category ?report ?selector-desc)
      (trgx-take-last 3 ?src-trgx-data :> ?trgx-L3)
      ((c/comp split-rows (mapfn [x] (->> x (trgx-split-at 4) vector))) ?trgx-L3 :> ?node-tkv)
      ((tkv-select [:PATH]) ?node-tkv :> ?trgx-path)
      (pad 4 "TOTAL" ?trgx-path :> _ ?channel ?kpi ?metrics)
      (not= ?channel "TOTAL")
      ((c/partial get #{"全体-HMKT / 大卖场"
                        "全体-SMKT / 超市"
                        "全体-GT / 传统食杂"
                        "全体-E&D M/H / 中高档餐饮"
                        "全体-E&D Trad / 传统餐饮"}) ?kpi :> !kpi-exclude)
      (not !kpi-exclude)
      ((mapfn [channel kpi] (if (= channel "全体-Total / 所有渠道") (first (clojure.string/split kpi #"-")) kpi)) ?channel ?kpi :> ?kpis)
      ((vars->pair [:period :channel :kpi :metrics]) ?dw-dt ?channel ?kpis ?metrics :> ?selector-edn)
      ((tkv-select [:SUBTREE]) ?node-tkv :> ?trgx-subtree)
      (vals ?trgx-subtree :> ?node-value)
      ((kv->tuple [:DATA]) ?node-value :> ?node-data)
      ((kv->tuple [:c_total_score :c_weight :value :pp_value :last_dec_value]) ?node-data :> ?c_total_score ?c_weight ?value !pp-value !last-dec-value)
      ((tr-dimension-metrics [:bg :bottler] [:c_total_score :c_weight :value :pp_value :last_dec_value :abbr]) ?bg_sort ?src-bottler ?c_total_score ?c_weight ?value !pp-value !last-dec-value ?bottler_sort :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

(defn -main []
  (def dt-rng (report->next-dt report-tap-out "bottler_ranking"))
  (prn {:dt-rng dt-rng} "running...")
  (try (?- report-tap-out (score-bottler_ranking-report dt-rng)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!")
  )

(comment
  (map prn
       (trgx-split-at 4 (trgx-take-last 3 (latest-trgx-merge)))
       )
  (def kv {"全体-Total / 所有渠道" {:val 33}
           "[G22]HMKT / 大卖场" {:val 44}})
  kv->trgx
  (prn ((kv->trgx  (latest-trgx-merge)) kv))
  
  )
