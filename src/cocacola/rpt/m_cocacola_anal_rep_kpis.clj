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
;#   v1_0=2017-02-28@zhuo {create}
;#   v1.1=2017-03-07@kevin {modified}
;#*********************************

(ns cocacola.rpt.m-cocacola-anal-rep-kpis
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)


(def kpis-tap-in (pg-tap "dw" "model.d_cocacola_anal_rep_kpis" ["dw-dt" "market" "bg" "bottler" "channel" "kpi" "kpi_details" "item" "product" "value" "xorder" "level" "bg_sort" "abbrevation" "channel_sort"]))

(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def anal-rep-kpis-report
  (as->
      (<- [?bg ?bottler ?channel ?kpi ?kpi_details ?item ?product ?xorder ?level ?bg_sort ?bottler_sort ?channel_sort ?period-value]
          (kpis-tap-in :> ?dw-dt ?mbd ?bg ?bottler ?channel ?kpi ?kpi_details ?item ?product ?value ?xorder ?level ?bg_sort ?bottler_sort ?channel_sort)
          ((tr-dimension-metrics [:period] [:value]) ?dw-dt ?value :> ?period-value))
      result
      (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
          (result :> ?bg ?bottler ?channel ?kpi ?kpi_details ?item ?product ?xorder ?level ?bg_sort ?bottler_sort ?channel_sort ?period-value)
          (identity "9999-12-31" :> ?dw-dt)
          (identity ["cocacola" "score" "anal_rep_kpis" ""] :> ?project ?category ?report ?selector-desc)
          ((vars->pair [:bg :bottler :channel :kpi :kpi_details :item]) ?bg_sort ?bottler_sort ?channel_sort ?kpi ?kpi_details ?item :> ?selector-edn)
          ((tr-dimension-metrics [:product] [:c_sort :level :period_value]) ?product ?xorder ?level ?period-value :> ?dimension-metrics-edn)
          ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics))
        ))


#_(?<- (stdout)
       [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       ((c/first-n anal-rep-kpis-report 1) :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))



(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "anal_rep_kpis" dt)
  (def dt-rng (report->next-dt report-tap-out "anal_rep_kpis"))
  (prn {:dt-rng dt-rng :rpt "anal_rep_kpis"} "running...")
  (try (?- report-tap-out anal-rep-kpis-report) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


