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
;#   v1_0=2017-02-24@kevin {create}
;#   v1.1=2017-02-27@zhuo  {modified}
;#   v1.2=2017-02-28@kevin {modified}
;#*********************************

(ns cocacola.rpt.m-cocacola-score-achievement-rate
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout mapfn deffilterfn]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [common.trgx :refer :all]
            [clojure.string :as str]))

(set-level! :warn)

(def score-tap-in (pg-tap "dw" "model.d_cocacola_achievement_rate" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "c_total_score" "c_weight"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(deffilterfn bg-all? [bottler]
  (or (= bottler "China Total / 全国总体")
      (= bottler "SBL Total / 全体")
      (= bottler "CBL Total / 全体")
      (= bottler "BIG Total / 全体")
      (= bottler "Zhuhai Total / 全体")))

(def score-achievement-rate
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?c_total_score ?c_weight)
      (identity ["9999-12-31" "cocacola" "score" "achievement_rate" ""] :> ?dw-dt ?project ?category ?report ?selector-desc)
      ((vars->pair [:bg :channel :kpi]) ?bg_sort ?channel ?item :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:c_total_score :c_weight :value]) ?bottler_sort ?src-dw-dt ?c_total_score ?c_weight ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

#_ (?<- (stdout)
        [?count]
        #_[?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
        (score-achievement-rate :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics)
        (c/count ?count))

(def score-achievement-rate-bg-all
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      (score-tap-in :> ?src-dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?c_total_score ?c_weight)
      (identity ["9999-12-31" "cocacola" "score" "achievement_rate" ""] :> ?dw-dt ?project ?category ?report ?selector-desc)
      (bg-all? ?bottler)
      ((vars->pair [:bg :channel :kpi]) "0.2_BGs" ?channel ?item :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:c_total_score :c_weight :value]) ?bottler_sort ?src-dw-dt ?c_total_score ?c_weight ?value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))



#_ (?<- (stdout)
        [?count]
        #_[?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
        (score-achievement-rate-bg-all :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics)
        (c/count ?count))



(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "achievement_rate" dt)
  (def dt-rng (report->next-dt report-tap-out "achievement_rate"))
  (prn {:dt-rng dt-rng :rpt "achievement_rate"} "running...")
  (try (?- report-tap-out score-achievement-rate) (catch Exception _))
  (try (?- report-tap-out score-achievement-rate-bg-all) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))


