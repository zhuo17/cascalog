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
;#   v1_0=2017-01-20@zhuo {create}
;#*********************************

(ns cocacola.rpt.m-cocacola-sovi_period_trend6
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]]
            [common.trgx :refer :all]
            [clojure.string :as str])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

(def sovi-tap-in (pg-tap "dw" "model.d_cocacola_sovi" ["dw-dt" "bg" "bottler" "channel" "market" "vendor" "product_category" "sovi_type" "value" "bg_sort" "abbrevation" "channel_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-brand
  (<- [?dw-dt ?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?bg_sort ?bottler_sort ?channel_sort ?value]
      (sovi-tap-in :> ?dw-dt ?bg ?bottler ?channel ?market ?brands ?juice_type ?sovi_type ?score ?bg_sort ?bottler_sort ?channel_sort)
      (collect-kv ?brands ?score :> ?brands-score)
      ((mapfn [it] (assoc-in it ["Others"] (- 100 (reduce + (mapv second it))))) ?brands-score :> ?brand-score)
      ((mapcatfn [it] (mapv #(cons % (mapv it [%])) (keys it)))
       ?brand-score :> ?brand ?value)))

(def score-dt-kv
  (<- [?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?bg_sort ?bottler_sort ?channel_sort ?dw-dt-kv]
      (score-brand :> ?dw-dt ?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?bg_sort ?bottler_sort ?channel_sort ?value)
      (collect-kv ?dw-dt ?value :> ?dw-dt-kv)))



(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?value ?bg_sort ?bottler_sort ?channel_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value]
      (score-dt-kv :> ?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?bg_sort ?bottler_sort ?channel_sort ?dw-dt-kv)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month prev-same-month) ?dw-dt :> !prev-last-day !prev-last-month !last-year-same-month)
      ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value)
      (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month !last-year-same-month :> ?value !pp-value !last-dec-value !last-year-same-month-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !pp-value :> !vs-pp-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-dec-value :> !vs-last-dec-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-year-same-month-value :> !vs-last-year-same-month-value)))




(defn sovi_period_trend6-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?src-dw-dt ?bg ?bottler ?channel ?market ?brand ?juice_type ?sovi_type ?value ?bg_sort ?bottler_sort ?channel_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (identity "9999-12-31" :> ?dw-dt)
      (not (re-find #"^China Tier\d" ?market))
      (= ?sovi_type "Total")
      (identity ["cocacola" "score" "sovi_period_trend6" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:bg :bottler :channel]) ?bg_sort ?bottler_sort ?channel_sort :> ?selector-edn)
      ((tr-dimension-metrics [:period :brand :juice_type :sovi_type] [:score :pp_score :vs_pp_score :last_dec_score :vs_last_dec_score :last_year_same_month_score :vs_last_year_same_month_score]) ?src-dw-dt ?brand ?juice_type ?sovi_type ?value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))


#_ (?<- (stdout)
        [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
        ((sovi_period_trend6-report ["2014-01-01" "2016-12-31"]) ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))




(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "sovi_period_trend6" dt)
  (def dt-rng (report->next-dt report-tap-out "sovi_period_trend6"))
  (prn {:dt-rng dt-rng :rpt "sovi_period_trend6"} "running...")
  (try (?- report-tap-out (sovi_period_trend6-report dt-rng)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))
