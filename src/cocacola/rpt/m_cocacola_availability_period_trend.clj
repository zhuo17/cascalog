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

(ns cocacola.rpt.m-cocacola-availability_period_trend
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

(def availability-tap-in (pg-tap "dw" "model.d_cocacola_sku_mapping" ["dw-dt" "period" "market" "bg" "bottler" "channel" "sku" "brand" "sku_detail" "orders" "value" "bg_sort" "abbrevation" "channel_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-dt-kv
  (<- [?market ?bg ?bottler ?channel ?sku ?brand ?sku_detail ?orders ?bg_sort ?bottler_sort ?channel_sort ?dw-dt-kv]
      (availability-tap-in :> ?dw-dt ?period ?market ?bg ?bottler ?channel ?sku ?brand ?sku_detail ?orders ?value ?bg_sort ?bottler_sort ?channel_sort)
      (collect-kv ?dw-dt ?value :> ?dw-dt-kv)))


(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?market ?bg ?bottler ?channel ?sku ?brand ?sku_detail ?orders ?value ?bg_sort ?bottler_sort ?channel_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value]
      (score-dt-kv :> ?market ?bg ?bottler ?channel ?sku ?brand ?sku_detail ?orders ?bg_sort ?bottler_sort ?channel_sort ?dw-dt-kv)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month prev-same-month) ?dw-dt :> !prev-last-day !prev-last-month !last-year-same-month)
      ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value)
      (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month !last-year-same-month :> ?value !pp-value !last-dec-value !last-year-same-month-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !pp-value :> !vs-pp-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-dec-value :> !vs-last-dec-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-year-same-month-value :> !vs-last-year-same-month-value)))


(defn availability_period_trend-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?src-dw-dt ?market ?bg ?bottler ?channel ?sku ?brand ?sku_detail ?orders ?value ?bg_sort ?bottler_sort ?channel_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (identity "9999-12-31" :> ?dw-dt)
      (not (re-find #"^China Tier\d" ?market))
      ((mapfn [sku sku_detail] (str sku "_" sku_detail)) ?sku ?sku_detail :> ?sku_type)
      (identity ["cocacola" "score" "availability_period_trend" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:channel :kpi :sku_type :bottler]) ?channel_sort "AVAILABILITY" ?sku_type ?bottler_sort :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :period] [:bg :score :pp_score :vs_pp_score :last_dec_score :vs_last_dec_score :last_year_same_month_score :vs_last_year_same_month_score]) ?bottler_sort ?src-dw-dt ?bg ?value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

#_ (?<- (stdout)
        [?count]
        ((availability_period_trend-report ["2015-01-01" "2016-12-31"]) :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics)
        (c/count ?count))

(defn -main []
  (def dt ["9999-12-31" "9999-12-31"])
  (detele-report! report-tap-out "availability_period_trend" dt)
  (def dt-rng (report->next-dt report-tap-out "availability_period_trend"))
  (prn {:dt-rng dt-rng :rpt "availability_period_trend"} "running...")
  (try (?- report-tap-out (availability_period_trend-report dt-rng)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))
