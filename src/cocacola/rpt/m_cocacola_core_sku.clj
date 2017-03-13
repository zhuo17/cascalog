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
;#   v1_0=2017-02-22@kevin {create}
;#   v1.1=2017-03-07@kevin {modified} 
;#*********************************

(ns cocacola.rpt.m-cocacola-core-sku
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn deffilterfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_core_sku" ["dw-dt" "bg" "bottler" "channel" "product" "xorder" "kpi_type" "value" "bg_sort" "abbrevation" "channel_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(deffilterfn bg-all? [bottler]
  (or (= bottler "China Total / 全国总体")
      (= bottler "SBL Total / 全体")
      (= bottler "CBL Total / 全体")
      (= bottler "BIG Total / 全体")
      (= bottler "Zhuhai Total / 全体")))



(def core-sku-report 
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?bg ?bottler ?channel ?product ?xorder ?kpi_type ?value ?bg_sort ?bottler_sort ?channel_sort)
      (identity "9999-12-31" :> ?dw-dt)
      (identity ["cocacola" "score" "core_sku" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:channel :product :bg :kpi_type]) ?channel_sort ?product ?bg_sort ?kpi_type :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:value]) ?bottler_sort ?src-dw-dt ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

#_(?<- (stdout)
       [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (core-sku-report :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))

(def core-sku-report-bg-all 
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?bg ?bottler ?channel ?product ?xorder ?kpi_type ?value ?bg_sort ?bottler_sort ?channel_sort)
      (identity "9999-12-31" :> ?dw-dt)
      (identity ["cocacola" "score" "core_sku" ""] :> ?project ?category ?report ?selector-desc)
      (bg-all? ?bottler)
      ((vars->pair [:channel :product :bg :kpi_type]) ?channel_sort ?product "0.2_BGs"  ?kpi_type :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:value]) ?bottler_sort ?src-dw-dt ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

#_(?<- (stdout)
       [?count]
       #_[?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (core-sku-report-bg-all :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics)
       (c/count ?count))

(def core-sku-report-bottler-all 
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?bg ?bottler ?channel ?product ?xorder ?kpi_type ?value ?bg_sort ?bottler_sort ?channel_sort)
      (identity "9999-12-31" :> ?dw-dt)
      (identity ["cocacola" "score" "core_sku" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:channel :product :bg :kpi_type]) ?channel_sort ?product "0.1_Total" ?kpi_type :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:value]) ?bottler_sort ?src-dw-dt ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))


#_(?<- (stdout)
       [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (core-sku-report-bottler-all :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))


(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "core_sku" dt)
  (def dt-rng (report->next-dt report-tap-out "core_sku"))
  (prn {:dt-rng dt-rng :rpt "core_sku"} "running...")
  (try (?- report-tap-out core-sku-report) (catch Exception _))
  (try (?- report-tap-out core-sku-report-bg-all) (catch Exception _))
  (try (?- report-tap-out core-sku-report-bottler-all) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


