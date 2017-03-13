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

(ns cocacola.rpt.m-cocacola-score_top_bottom5_ranking
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

(def score-tap-in (pg-tap "dw" "model.d_cocacola_score" ["dw-dt" "period" "mbd" "bg" "bottler" "channel" "code" "item" "fact" "value" "abbrevation" "bg_sort" "channel_sort" "kpi_sort"]))
(def report-tap-out (pg-tap "ms" "report" ["dw-dt" "project" "category" "report" "selector" "selector-desc" "dimension-metrics"]))

(def score-dt-kv
  (<- [?bg ?bottler ?channel !code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?dw-dt-kv]
      (score-tap-in :> ?dw-dt ?period ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort)
      (collect-kv ?dw-dt ?value :> ?dw-dt-kv)))


(defn score-sliding  [[start-dt end-dt]]
  (<- [?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value]
      (score-dt-kv :> ?bg ?bottler ?channel !code ?item ?fact ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?dw-dt-kv)
      ((c/comp split-rows mk-month-dts) start-dt end-dt :> ?dw-dt)
      ((c/juxt prev-last-day prev-last-month prev-same-month) ?dw-dt :> !prev-last-day !prev-last-month !last-year-same-month)
      ((mapfn [x]  (->> x vals (apply max))) ?dw-dt-kv :> ?max-value)
      (kv->lkp ?dw-dt-kv ?dw-dt !prev-last-day !prev-last-month !last-year-same-month :> ?value !pp-value !last-dec-value !last-year-same-month-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !pp-value :> !vs-pp-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-dec-value :> !vs-last-dec-value)
      ((mapfn [a b] (if (nil? b) nil (- a b))) ?value !last-year-same-month-value :> !vs-last-year-same-month-value)))


#_(defn score-top_bottom5_rank-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?mbd ?bg ?bottler ?channel !code ?item ?fact ?value ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (not !code)
      ((mapfn [it] (let [kpi (first (str/split it #"-"))]
                     (if (= kpi "产品铺货率") "产品铺货" kpi))) ?item :> ?kpi)
      ((mapfn [channel kpi bottler] (and (when-not (re-find #"\S*全体$" bottler) bottler)
                                         (when-not (re-find #"\S*全国总体$" bottler) bottler)
                                         (when-not (re-find #"Tier\d" bottler) bottler)
                                         (when-not (re-find #"\S*区域$" bottler) bottler)
                                         (when-not (re-find #"\S*辽宁$" bottler) bottler)
                                         (or (and (re-find #"Total / 所有渠道" channel) (re-find #"^全体$" kpi) bottler)
                                             (and (when-not (re-find #"Total / 所有渠道" channel) bottler) (when-not (re-find #"^全体$" kpi) bottler)))))
       ?channel ?kpi ?bottler :> ?bottlers)
      (identity ["cocacola" "score" "top_bottom5_rank" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :button]) ?dw-dt "test" :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :channel :kpi] [:score :pp_score :vs_pp_score :last_dec_score :vs_last_dec_score :last_year_same_month_score :vs_last_year_same_month_score]) ?bottler ?channel ?kpi ?value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn score-top_bottom5_rank_last_month-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (not !code)
      ((mapfn [it] (let [kpi (first (str/split it #"-"))]
                     (if (= kpi "产品铺货率") "产品铺货" kpi))) ?item :> ?kpi)
      ((mapfn [channel kpi bottler] (and (when-not (re-find #"\S*全体$" bottler) bottler)
                                         (when-not (re-find #"\S*全国总体$" bottler) bottler)
                                         (when-not (re-find #"Tier\d" bottler) bottler)
                                         (when-not (re-find #"\S*区域$" bottler) bottler)
                                         (when-not (re-find #"\S*辽宁$" bottler) bottler)
                                         (or (and (re-find #"Total / 所有渠道" channel) (re-find #"^全体$" kpi) bottler)
                                             (and (when-not (re-find #"Total / 所有渠道" channel) bottler) (when-not (re-find #"^全体$" kpi) bottler)))))
       ?channel ?kpi ?bottler :> ?bottlers)
      (identity ["cocacola" "score" "top_bottom5_ranking" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :button]) ?dw-dt "vs Last Month" :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :channel :kpi] [:score :pp_score :vs_pp_score :abbr]) ?bottlers ?channel_sort ?kpi_sort ?value !pp-value !vs-pp-value ?bottler_sort :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))

(defn score-top_bottom5_rank_last_dec-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (not !code)
      ((mapfn [it] (let [kpi (first (str/split it #"-"))]
                     (if (= kpi "产品铺货率") "产品铺货" kpi))) ?item :> ?kpi)
      ((mapfn [channel kpi bottler] (and (when-not (re-find #"\S*全体$" bottler) bottler)
                                         (when-not (re-find #"\S*全国总体$" bottler) bottler)
                                         (when-not (re-find #"Tier\d" bottler) bottler)
                                         (when-not (re-find #"\S*区域$" bottler) bottler)
                                         (when-not (re-find #"\S*辽宁$" bottler) bottler)
                                         (or (and (re-find #"Total / 所有渠道" channel) (re-find #"^全体$" kpi) bottler)
                                             (and (when-not (re-find #"Total / 所有渠道" channel) bottler) (when-not (re-find #"^全体$" kpi) bottler)))))
       ?channel ?kpi ?bottler :> ?bottlers)
      (identity ["cocacola" "score" "top_bottom5_ranking" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :button]) ?dw-dt "vs Last Dec." :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :channel :kpi] [:score :last_dec_score :vs_last_dec_score :abbr]) ?bottlers ?channel_sort ?kpi_sort ?value !last-dec-value !vs-last-dec-value ?bottler_sort :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))
#_(?<- (stdout)
     [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
     ((score-top_bottom5_rank_last_year-report ["2014-12-31" "2016-12-31"]) :> ?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics))
(defn score-top_bottom5_rank_last_year-report [[start-dt end-dt]]
  (<- [?dw-dt ?project ?category ?report ?selector ?selector-desc ?dimension-metrics]
      ((score-sliding [start-dt end-dt]) :> ?dw-dt ?bg ?bottler ?channel !code ?item ?fact ?value ?bottler_sort ?bg_sort ?channel_sort ?kpi_sort ?max-value !pp-value !vs-pp-value !last-dec-value !vs-last-dec-value !last-year-same-month-value !vs-last-year-same-month-value)
      (not !code)
      ((mapfn [it] (let [kpi (first (str/split it #"-"))]
                     (if (= kpi "产品铺货率") "产品铺货" kpi))) ?item :> ?kpi)
      ((mapfn [channel kpi bottler] (and (when-not (re-find #"\S*全体$" bottler) bottler)
                                         (when-not (re-find #"\S*全国总体$" bottler) bottler)
                                         (when-not (re-find #"Tier\d" bottler) bottler)
                                         (when-not (re-find #"\S*区域$" bottler) bottler)
                                         (when-not (re-find #"\S*辽宁$" bottler) bottler)
                                         (or (and (re-find #"Total / 所有渠道" channel) (re-find #"^全体$" kpi) bottler)
                                             (and (when-not (re-find #"Total / 所有渠道" channel) bottler) (when-not (re-find #"^全体$" kpi) bottler)))))
       ?channel ?kpi ?bottler :> ?bottlers)
      (identity ["cocacola" "score" "top_bottom5_ranking" ""] :> ?project ?category ?report ?selector-desc)
      ((vars->pair [:period :button]) ?dw-dt "vs Last Year" :> ?selector-edn)
      ((tr-dimension-metrics [:bottler :channel :kpi] [:score :last_year_same_month_score :vs_last_year_same_month_score :abbr]) ?bottlers ?channel_sort ?kpi_sort ?value !last-year-same-month-value !vs-last-year-same-month-value ?bottler_sort :> ?dimension-metrics-edn)
      ((c/each pair-edn->json) ?selector-edn ?dimension-metrics-edn :> ?selector ?dimension-metrics)))



(defn -main []
  (def dt-rng (report->next-dt report-tap-out "top_bottom5_ranking"))
  (prn {:dt-rng dt-rng :rpt "top_bottom5_ranking"} "running...")
  (try (?- report-tap-out (score-top_bottom5_rank_last_month-report dt-rng)) (catch Exception _))
  (try (?- report-tap-out (score-top_bottom5_rank_last_dec-report dt-rng)) (catch Exception _))
  (try (?- report-tap-out (score-top_bottom5_rank_last_year-report dt-rng)) (catch Exception _))
  (prn {:dt-rng dt-rng} "done!"))
