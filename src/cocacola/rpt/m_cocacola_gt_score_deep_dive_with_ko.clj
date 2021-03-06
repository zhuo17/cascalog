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
;#   v1_0=2017-02-28@kevin {create}
;#   v1.1=2017-03-01@kevin {modified}
;#*********************************

(ns cocacola.rpt.m-cocacola-gt-score-deep-dive-with-ko
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_gt_score_deep_dive_with_ko" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "p_item" "item_weight" "item_order" "total_score"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))


(defn get-item-json-string [item p_item item_order item_weight total_score]
    (str "{\"item_name\":\"" item "\",\"p_item\":\"" p_item "\",\"c_sort\":" item_order ",\"c_weight\":" item_weight ",\"c_total_score\":" total_score "}"))

(def m-cocacola-gt-score-deep-dive-with-ko
    (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
        (score-tap-in :> ?src-dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort !p_item ?item_weight ?item_order ?total_score)
        (identity ["9999-12-31" "cocacola" "score" "gt_score_deep_dive_with_ko" ""] :> ?dw-dt ?project ?category ?report ?selector-desc)
        ((vars->pair [:bg :bottler]) ?bg_sort ?bottler_sort :> ?selector-edn)
        (get-item-json-string ?item !p_item ?item_order ?item_weight ?total_score :> ?item_json_string)
        ((tr-dimension-metrics [:item :period] [:value]) ?item_json_string ?src-dw-dt ?value :> ?dimension-metrics-edn)
        ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

#_(?<- (stdout)
       [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
       (m-cocacola-gt-score-deep-dive :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))

(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "gt_score_deep_dive_with_ko" dt)
  (def dt-rng (report->next-dt report-tap-out "gt_score_deep_dive_with_ko"))
  (prn {:dt-rng dt-rng :rpt "gt_score_deep_dive_with_ko"} "running...")
  (try (?- report-tap-out m-cocacola-gt-score-deep-dive-with-ko) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


