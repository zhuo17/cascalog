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

(ns cocacola.rpt.m-cocacola-score_period
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

(defn score-sliding  [[start-dt end-dt]]
  #_(score-sliding ["1970-01-01" (future-dt)])
  (as->
      (<- [?src-bg ?src-bottler ?src-channel !src-code ?src-item ?src-fact ?bottler_sort ?bg_sort ?channel_sort ?dw-dt-kv ?max-value]
          (score-tap-in :> ?src-dw-dt ?src-period ?src-mbd ?src-bg ?src-bottler ?src-channel !src-code ?src-item ?src-fact ?src-value ?bottler_sort ?bg_sort ?channel_sort)
          (collect-kv ?src-dw-dt ?src-value :> ?dw-dt-kv)
          ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value) )
      new-generator
    (<- [?dw-dt ?src-bg ?src-bottler ?src-channel !src-code ?src-item ?src-fact ?value ?bottler_sort ?bg_sort ?channel_sort ?src-max-value !pp-value !last-dec-value !last-year-same-month-score]
        (new-generator :> ?src-bg ?src-bottler ?src-channel !src-code ?src-item ?src-fact ?bottler_sort ?bg_sort ?channel_sort ?dw-dt-kv ?src-max-value)
        ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
        ((c/juxt prev-last-day prev-last-month prev-same-month) ?dw-dt :> !prev-last-day !prev-last-month !last-year-same-month)
        (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month !last-year-same-month :> ?value !pp-value !last-dec-value !last-year-same-month-score) )))


#_(?<- (stdout)
     [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
     ((score-period-report) :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))
(defn score-period-report []
  #_(??- (c/first-n (score-period-report) 10))
  (as->
      (<- [?src-dw-dt ?src-bg ?src-bottler ?bg_sort ?bottler_sort ?selector-edn ?c_total_score ?c_weight ?value ?max-value !pp-value !last-dec-value !last-year-same-month-score]
          ((score-sliding ["1970-01-01" (future-dt)]) :> ?src-dw-dt ?src-bg ?src-bottler ?src-channel !src-code ?src-item ?src-fact ?src-value ?bottler_sort ?bg_sort ?channel_sort ?src-max-value !src-pp-value !src-last-dec-value !src-last-year-same-month-score)
          (str "[" !src-code "]" ?src-item :> ?code-item)
          ((vars->kv [:value :max_value :pp_value :last_dec_value :last_year_same_month_score])
                      ?src-value ?src-max-value !src-pp-value !src-last-dec-value !src-last-year-same-month-score :> ?value-tkv)
          (collect-kv ?code-item ?value-tkv :> ?code-item-kv)
          ((kv->trgx (latest-trgx-merge)) ?code-item-kv :> ?trgx-data)
          (trgx-take-last 3 ?trgx-data :> ?trgx-L3)
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
          ((vars->pair [:channel :kpi :metrics :bottler]) ?channel ?kpis ?metrics ?bottler_sort :> ?selector-edn)
          ((tkv-select [:SUBTREE]) ?node-tkv :> ?trgx-subtree)
          (vals ?trgx-subtree :> ?node-value)
          ((tkv-select [:DATA]) ?node-value :> ?node-data)
          ((tkv-select [:c_total_score :c_weight :value :max_value :pp_value :last_dec_value :last_year_same_month_score]) ?node-data
               :> ?c_total_score ?c_weight ?value ?max-value !pp-value !last-dec-value !last-year-same-month-score))
      rpt-data
    (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
        (rpt-data :> ?src-dw-dt ?src-bg ?src-bottler ?bg_sort ?bottler_sort ?selector-edn ?c_total_score ?c_weight ?value ?max-value !pp-value !last-dec-value !last-year-same-month-score)
        (identity ["cocacola" "score" "period" ""] :> ?project ?category ?report ?selector-desc)
        (identity "9999-12-31" :> ?dw-dt)
        ((tr-dimension-metrics [:bottler :period] [:c_total_score :c_weight :value :max_value :pp_value :last_dec_value :last_year_same_month_score])
             ?bottler_sort ?src-dw-dt ?c_total_score ?c_weight ?value ?max-value !pp-value !last-dec-value !last-year-same-month-score :> ?dimension-metrics-edn)
        ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics))) )

(defn -main []
  (def dt-rng ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "period" dt-rng)
  (prn {:dt-rng dt-rng} "running...")
  (try (?- report-tap-out (score-period-report)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))

(comment
  (def data [["aaa" "2015-01-01"]])

  (defn year [x] )
  (??<- [?c]
        (data :> ?a ?b)
        ((mapfn [x] (subs x 0 4)) ?b :> ?c))
  )
