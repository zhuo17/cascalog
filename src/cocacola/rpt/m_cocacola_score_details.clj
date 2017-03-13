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
;#   v1_0=2017-02-23@kevin {create}
;#*********************************

(ns cocacola.rpt.m-cocacola-score-details
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)
(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "channel_sort" "kpi_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))


(def score-details-report 
  (as->
      (<- [?bg ?bottler ?channel ?code ?item ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?period-value]
          (score-tap-in :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel ?code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort)
          ((tr-dimension-metrics [:period] [:value]) ?dw-dt ?value :> ?period-value))
      last-result
      (<- [?bg ?bottler ?bg_sort ?bottler_sort ?trgx-data]
        (last-result :> ?bg ?bottler ?channel ?code ?item ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?period-value)
        ((vars->kv [:period_value]) ?period-value :> ?value-tuple-kv)
        (str "[" ?code "]" ?item :> ?code-item)
        (collect-kv ?code-item ?value-tuple-kv :> ?code-item-kv)
        ((kv->trgx (latest-trgx-kpi)) ?code-item-kv :> ?trgx-data))
      
      (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
          (last-result :> ?bg ?bottler ?bg_sort ?bottler_sort ?trgx-data)
          (identity ["9999-12-31" "cocacola" "score" "details" ""] :> ?dw-dt ?project ?category ?report ?selector-desc)
          ((vars->pair [:bg :bottler]) ?bg_sort ?bottler_sort :> ?selector-edn)
          (identity ?trgx-data :> ?dimension-metrics-edn)
          ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics))))


#_(?<- (stdout)
     [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
     (score-details-report :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))



(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "details" dt)
  (def dt-rng (report->next-dt report-tap-out "details"))
  (prn {:dt-rng dt-rng :rpt "details"} "running...")
  (try (?- report-tap-out score-details-report) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


