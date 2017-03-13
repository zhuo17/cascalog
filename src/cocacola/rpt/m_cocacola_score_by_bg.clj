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
;#   v1_0=2017-02-22@chong {create}
;#*********************************

(ns cocacola.rpt.m-cocacola-score_by_bg
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "channel_sort" "kpi_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-tier-report 
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort)
      (identity "9999-12-31" :> ?dw-dt)
      (not !code)
      (identity ["cocacola" "score" "report_by_bg" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:channel :kpi]) ?channel_sort ?kpi_sort :> ?selector-edn)
      ((tr-dimension-metrics [:bg :bottler :period] [:score]) ?bg_sort ?bottler_sort ?src-dw-dt ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))


#_(?<- (stdout)
       [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (score-tier-report :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))



(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "report_by_bg" dt)
  (def dt-rng (report->next-dt report-tap-out "report_by_bg"))
  (prn {:dt-rng dt-rng :rpt "report_by_bg"} "running...")
  (try (?- report-tap-out score-tier-report) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


