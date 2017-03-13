;#*********************************
;# [intro]
;#   author=larluo@spiderdt.com
;#   func=partition algorithm for data warehouse
;#=================================
;# [param]
;#   tabname=staging table name
;#   prt-cols-str=ods partition cols
;#=================================
;# [caller]
;#   [PORG] bolome.dau
;#   [PORG] bolome.event
;#   [PORG] bolome.inventory
;#   [PORG] bolome.order
;#   [PORG] bolome.product-category
;#   [PORG] bolome.show
;#=================================
;# [version]
;#   v1-0=2016-09-28@larluo{create}
;#*********************************

(ns bolome.mlvar.d-bolome-user_order
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]]
            [clojure.java.jdbc :as j]
            [common.trgx :exclude [str-min] :refer :all]
            [cascalog.cascading.operations :as ops]
            [clojure.math.combinatorics :as combo])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

#_(defn parse-dt [dt] (tf/parse (tf/formatter "yyyy-MM-dd") dt))
#_(defn unparse-dt [dt-obj] (tf/unparse (tf/formatter "yyyy-MM-dd") dt-obj))
(defn offset-dt [dt n] (-> dt parse-dt  (t/plus (t/days n)) unparse-dt))
(defn dt-ge-get [dt-1 dt-2] (if (<= (compare dt-2 dt-1) 0) dt-1))
(defn str-min [& coll] (-> coll sort first))

(defn dt-rebase [[new-start-dt new-end-dt] [start-dt end-dt]]
  (let [dt-interval (-> (t/interval (parse-dt start-dt) (parse-dt end-dt)) t/in-days)]
    [(or new-start-dt (offset-dt new-end-dt (- dt-interval)))
     (or new-end-dt (offset-dt new-start-dt dt-interval))]))

(defn take-train-dts [n back-step [dm-start-dt dm-end-dt] [dw-min-dt dw-max-dt] intervals]
  (let [his-max-dt (str-min dw-max-dt (offset-dt dm-start-dt -1))
        train-base-seq (->> (periodic-seq (parse-dt his-max-dt) (t/days (- back-step)))
                            (map unparse-dt) (take-while #(dt-ge-get % dw-min-dt)))
        cal-dts   (fn [dt] (let [[y-start-dt y-end-dt] (dt-rebase [nil dt] [dm-start-dt dm-end-dt])
                                 [x-max-dt _] (dt-rebase [nil y-end-dt] [his-max-dt dm-end-dt])
                                 x-dts (->> intervals sort
                                            (map #(dt-ge-get (offset-dt x-max-dt (- %)) dw-min-dt))
                                            (take-while identity)
                                            (#(map vector % (repeat x-max-dt))) 
                                            (zipmap (sort intervals)))]
                             (when (seq x-dts) {:y [y-start-dt y-end-dt] :xs x-dts })))
        train-seq (->> train-base-seq (map cal-dts) (take-while identity) )
        test-seq [(cal-dts dm-end-dt)]
        xs-count (count intervals)
        train-times-seq (if (< n (count train-seq))
                          (take n train-seq)
                          (take-while #(= (-> % :xs count) xs-count) train-seq))]
    {:train (vec train-times-seq) :test (vec test-seq)}))

(defn sort? [& coll] (= (sort coll) coll))
(defn subtree [edn filters]
  (if-let [origin-cur-filter (first filters)]
    (let [cur-filter (if (sequential? origin-cur-filter) origin-cur-filter (vector origin-cur-filter))
          edn-filter (if (= cur-filter ["*"]) edn
                         (let [[type & coll] cur-filter]
                           (condp = type
                             :range (->> edn (filter #(sort? (first coll) (first %) (second coll))) (into {}))
                             (select-keys edn cur-filter))))]
      (->> edn-filter
           (keep (fn [[k v]] (let [v-kv (subtree v (rest filters))] (if (seq v-kv) [k v-kv])) ))
           (into {})))
    edn))

(defn tree-nodes [edn filters]
  (if-let [origin-cur-filter (first filters)]
    (let [cur-filter (if (sequential? origin-cur-filter) origin-cur-filter (vector origin-cur-filter))
          edn-filter (if (= cur-filter ["*"]) edn
                         (let [[type & coll] cur-filter]
                           (condp = type
                             :range (->> edn (filter #(sort? (first coll) (first %) (second coll))) (into {}))
                             (select-keys edn cur-filter))))]
      (->> edn-filter (mapcat (fn [[k v]] (let [v-kv (tree-nodes v (rest filters))] v-kv))) vec))
    [edn]))

(defmapcatfn shift-cut-trgx [shift-cut trgx]
  (let [  product-group-var :product-category
          product-ids #{1125 1126}
          order-fields [:revenue :base-revenue :discount-amount :order-ste :order-pe :order-debut :order-replay]
          product-fields [:revenue :base-revenue :discount-amount :order-ste :order-pe :order-debut :order-replay :quantity]
          product-group-fields [:revenue :base-revenue :discount-amount :quantity]]
      (->
       (for [{[y-start-dt y-end-dt] :y xs :xs} shift-cut]
        (let [y-segment-data (subtree (:CHILDREN trgx) [[:range y-start-dt y-end-dt]])
            y? (true? y-segment-data)
            xs (->> (for [[x-interval [x-start-dt x-end-dt]] xs]
                      (let [order-cnt (-> (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*" :CHILDREN "*"]) count)
                            order-agg (->> (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*" :CHILDREN "*" :CHILDREN "*" :DATA order-fields])
                                                (map #(condp = % "false" 0 "true" 1 %))
                                                (partition (count order-fields))
                                                (apply map + (repeat (count order-fields) 0))
                                                (zipmap order-fields) )
                            product-agg (->> (for [product-id product-ids]
                                               (let [product-agg (->> (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN product-id :CHILDREN "*" :CHILDREN "*" :DATA product-fields])
                                                                      (map #(condp = % "false" 0 "true" 1 %))
                                                                      (partition (count product-fields))
                                                                      (apply map + (repeat (count product-fields) 0))
                                                                      (zipmap product-fields) )]
                                                 [product-id product-agg]))
                                             (into {}))
                            product-group-nodes (tree-nodes trgx [:CHILDREN [:range x-start-dt x-end-dt] :CHILDREN "*"])
                            product-groups-agg (->> (for [product-group-node product-group-nodes]
                                                      (let [product-group-val (first (tree-nodes product-group-node [:DATA product-group-var]))
                                                            product-group-agg (->> (tree-nodes product-group-node [:CHILDREN "*" :CHILDREN "*" :DATA product-group-fields])
                                                                                   (map #(condp = % "false" 0 "true" 1 %))
                                                                                   (partition (count product-group-fields))
                                                                                   (apply map + (repeat (count product-group-fields) 0))
                                                                                   (zipmap product-group-fields))]
                                                        {product-group-val product-group-agg}))
                                                    (apply merge-with (partial merge-with +)))]
                        [x-interval (merge {:segment [x-start-dt x-end-dt] :order-cnt order-cnt} order-agg {:products product-agg} #_{:product-groups product-groups-agg})]))
                    (into {}))]
          (merge {:y {:segment [y-start-dt y-end-dt] :y? y?}} xs) ))
       vec)) )

(def mlvar-delta-user_order-ti (pg-tap "dw" "mlvar.d_bolome_user_order" ["dw-src-id" "trgx"]))
(def mlvar-larluo_test (pg-tap "dw" "mlvar.larluo_test" ["dw-src-id" "json"]))
#_(def mlvar-delta-user_order_stat-ti
    (pg-tap "dw" "mlvar_d_bolome_user_order_stat"
            ["y-4d" "order-cnt-4d" "revenue_4d" "baserevenue_4d" "discount_4d" "ste_cnt_4d" "pe_cnt_4d" "coupon_cnt_4d" "preview_cnt_4d" "debut_cnt_4d" "replay_cnt_4d"
             "quantity" ]))

(comment
  (def test-shifts (-> (take-train-dts 30 3 ["2015-12-18" "2016-01-10"] ["2014-05-01" "2016-06-30"] #{3 6 13 20 27 59 29}) :test))
  (def train-shfits (-> (take-train-dts 30 3 ["2015-12-18" "2016-01-10"] ["2014-05-01" "2016-06-30"] #{3 6 13 20 27 59 29}) :train))

  
  (def trgx (first (??<- [?trgx] (mlvar-delta-user_order-ti :> _ ?trgx-pr) (clojure.edn/read-string ?trgx-pr :> ?trgx))) )
  (as->
      (<- [?dw-src-id ?json]
          (mlvar-delta-user_order-ti :> ?dw-src-id ?trgx-pr)
          (clojure.edn/read-string ?trgx-pr :> ?trgx)
          (shift-cut-trgx test-shifts ?trgx :> ?json))
      $
    (??- $)
    )
  )
