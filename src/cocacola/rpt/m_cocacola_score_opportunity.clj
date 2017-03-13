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

(ns cocacola.rpt.m-cocacola-score_opportunity
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
           [cascading.jdbc JDBCTap JDBCScheme]) )

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "channel_sort" "kpi_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-dt-kv
  (<- [?bg ?bottler ?channel ?code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?dw-dt-kv]
   (score-tap-in :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort)
   (collect-kv ?dw-dt ?value :> ?dw-dt-kv)) )

(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?channel ?code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !last-dec-value !last_year_same_month_score]
      (score-dt-kv :> ?bg ?bottler ?channel ?code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?dw-dt-kv)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month prev-same-month) ?dw-dt :> !prev-last-day !prev-last-month !last_year_same_month)
      ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value) 
      (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month !last_year_same_month :> ?value !pp-value !last-dec-value !last_year_same_month_score)))

(defn score-trgx [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?bg_sort ?bottler_sort ?trgx-data]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?channel ?code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !last-dec-value !last_year_same_month_score )
      ((vars->kv [:value :max_value :pp_value :last_dec_value :last_year_same_month_score ]) ?value ?max-value !pp-value !last-dec-value !last_year_same_month_score :> ?value-tuple-kv)
      (str "[" ?code "]" ?item :> ?code-item)
      (collect-kv ?code-item ?value-tuple-kv :> ?code-item-kv)
      ((kv->trgx (latest-trgx-kpi)) ?code-item-kv :> ?trgx-data) ))

(defn score-opportunity-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-trgx [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?bg_sort ?bottler_sort ?trgx-data)
      (identity ["cocacola" "score" "opportunity" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :bg :bottler]) ?dw-dt ?bg_sort ?bottler_sort :> ?selector-edn)
      ((tr-dimension-metrics [] [:trgx-data]) ?trgx-data :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics) ))

#_(?<- (stdout)
     [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
     ((score-opportunity-report ["2014-12-31" "2016-12-31"]) ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))
(defn -main []
  (def dt-rng (report->next-dt report-tap-out "opportunity"))
  (prn {:dt-rng dt-rng :rpt "opportunity"} "running...")
  (try (?- report-tap-out (score-opportunity-report dt-rng)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))

(comment
  (??- (score-opportunity-report (report->next-dt report-tap-out "opportunity")))
  (map println (??- (score-opportunity-report ["2015-01-01" "2016-01-01"]))) 
  )
