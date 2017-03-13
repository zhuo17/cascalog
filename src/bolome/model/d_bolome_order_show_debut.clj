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

(ns bolome.ods.d-bolome-order_show_debut
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
            [clojure.java.jdbc :as j]
            [cascalog.cascading.operations :as ops]
            [clojure.math.combinatorics :as combo])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

(defaggregatefn collect-set ([] #{}) ([acc x] (conj acc x)) ([x] [x]))
(defaggregatefn collect-kvs ([] {}) ([acc k v] (update acc k #(conj (or % #{}) v))) ([x] [x]))
(defmapfn sort-keys [m] (->> (doto m (prn "@sort-keys")) (into (sorted-map)) keys vec vector))

(defmapfn cif [test then else] (if test then else))
(defmapfn nvl2 [test then else] (if test then else))
(defn cor [& xs] (some identity xs))
(defn str-least [& ks] (->> ks (keep identity) sort first) )
(defn mark [xs x]  (mapv (partial = x) xs))
(defn compare-mark [x y] (if (and x y) (let [v (compare x y)][(neg? v) (= 0 v) (pos? v)]) [false false false] ))

(defn truncate-tab [tap]
  (.executeUpdate tap (format "TRUNCATE TABLE %s" (-> tap .getTableName))))

(defn switch-tab [src-tap tgt-tap]
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName) (-> tgt-tap .getTableName (str "_t"))) ) 
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> src-tap .getTableName) (-> tgt-tap .getTableName)) )
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName (str "_t")) (-> src-tap .getTableName)) )  )


(defn build-delta-tab [model-delta-order_show_debut-to model-present-order-ti model-show-ti]
  #_(ffirst (??- (build-delta-tab model-delta-order_show_debut-to model-present-order-ti model-show-ti)))
  (truncate-tab model-delta-order_show_debut-to)
  (as-> (<-  [?join-show-dw-id ?exist-preview ?pay-dt-kvs ?pay-dts ?min-pay-dt]
          ((select-fields model-present-order-ti ["?dw-dt" "?show-dw-id" "?show-kind" "?pay-date"]) :> ?order-dw-dt ?join-show-dw-id ?order-show-kind ?order-pay-date)
          (collect-set ?order-show-kind :> ?order-show-kinds)
          (contains? ?order-show-kinds "preview" :> ?exist-preview)
          (collect-kvs ?order-pay-date ?order-dw-dt :> ?pay-dt-kvs)
          (sort-keys ?pay-dt-kvs :> ?pay-dts)
          (first ?pay-dts :> ?min-pay-dt) )
      $
    (<- [?join-show-dw-id ?show-dw-src-id ?exist-preview ?pay-dt-kvs ?pay-dts ?show-begin-time ?debut-dt]
        ($ :> ?join-show-dw-id ?exist-preview ?pay-dt-kvs ?pay-dts ?min-pay-dt)
        ((select-fields model-show-ti ["?dw-id" "?show-id" "?begin-time"]) :> ?join-show-dw-id ?show-dw-src-id ?show-begin-time)
        (subs ?show-begin-time 0 10 :> ?show-begin-dt)
        (nvl2 ?exist-preview ?show-begin-dt ?min-pay-dt :> ?debut-dt) )
    (?- model-delta-order_show_debut-to
        (ops/rename* $ ["?dw-id" "?dw-src-id" "?exist-preview" "?pay-dt-kvs" "?pay-dts" "?begin-ts" "?debut-dt"]))  )
  )

(defn build-shadow-tab [model-shadow-order_show_debut-to model-order_show_debut-to model-delta-order_show_debut-to]
  (as-> (<- [?dw-id ?dw-src-id ?exist-preview ?pay-dt-kvs ?pay-dts ?begin-ts ?debut-dt]
            ((select-fields model-delta-order_show_debut-to ["?dw-id" "?dw-src-id" "?exist-preview" "?pay-dt-kvs" "?begin-ts"])
                 :> ?dw-id !!src-dw-src-id !!src-exist-preview !!src-pay-dt-kvs-str !!src-begin-ts)
            ((select-fields model-order_show_debut-to ["?dw-id" "?dw-src-id" "?exist-preview" "?pay-dt-kvs" "?begin-ts"])
                 :> ?dw-id !!tgt-dw-src-id !!tgt-exist-preview !!tgt-pay-dt-kvs-str !!tgt-begin-ts)
            (cor !!src-dw-src-id !!tgt-dw-src-id :> ?dw-src-id)
            (cor !!src-exist-preview !!tgt-exist-preview :> ?exist-preview)
            ((c/each clojure.edn/read-string) !!src-pay-dt-kvs-str !!tgt-pay-dt-kvs-str
                 :> !src-pay-dt-kvs !tgt-pay-dt-kvs)
            (merge-with clojure.set/union !src-pay-dt-kvs !tgt-pay-dt-kvs :> ?pay-dt-kvs)
            (sort-keys ?pay-dt-kvs :> ?pay-dts)
            (first ?pay-dts :> ?min-pay-dt)
            (str-least !!src-begin-ts !!tgt-begin-ts :> ?begin-ts)
            (subs ?begin-ts 0 10 :> ?begin-dt)
            (nvl2 ?exist-preview ?begin-dt ?min-pay-dt :> ?debut-dt))
      $
    (?- model-shadow-order_show_debut-to $))
  )

(comment
  (def data [[-1 "aa"]
             [1 "bb"]])
  (??<- [?c ?d]
        (data :> ?a ?b)
        (< ?a 1 :> ?c)
        (nvl2 ?c "aa" "bb" :> ?d)
        )
  )

(comment
  (as-> (<- [?src-user-id ?src-order-id ?j-product-dw-id ?src-product-dw-src-id ?src-pay-date ?product-category ?product-subcat
           ?src-quantity ?revenue ?base-revenue ?discount-amount
           ?b-order-ste ?b-order-pe ?b-order-preview ?b-order-debut ?b-order-replay
           ]
          (model-present-order-ti :> ?src-dw-dt ?j-product-dw-id ?src-product-dw-src-id ?j-show-dw-id ?src-show-dw-src-id ?src-show-kind
                                  ?src-pay-date ?src-user-id ?src-order-id ?src-quantity ?src-price
                                  ?src-warehouse-id ?src-coupon-id ?j-event-id
                                  ?src-copon-discount-amount ?src-system-discount-amount ?src-tax-amount ?src-logistics-amount)
          (* ?src-price ?src-quantity :> ?revenue)
          (+ ?revenue ?src-tax-amount ?src-logistics-amount :> ?base-revenue)
          (+ ?src-copon-discount-amount ?src-system-discount-amount :> ?discount-amount)
          ((select-fields model-event-ti ["?event-id" "?type-name"]) :> ?j-event-id !!type-name)
          (mark ["专题" "活动"] !!type-name :> ?b-order-ste ?b-order-pe)
          (> ?src-coupon-id 0 :> ?b-order-coupon)
          ((select-fields model-order_show_debut-to ["?dw-id" "?debut-dt"]) :> ?j-show-dw-id !!show-debut-dt)
          (compare-mark ?src-pay-date !!show-debut-dt :> ?b-order-preview ?b-order-debut ?b-order-replay)
          ((select-fields model-product_category-ti ["?dw-id" "?category-1" "?category-2"]) :> ?j-product-dw-id ?product-category ?product-subcat)
          )
    $
  (??- $))
  )


(def model-event-ti (pg-tap "dw" "model.d_bolome_event" ["dw-dt" "event-id" "type-name" "event-name" "create-date"]))
(def model-present-order-ti (pg-tap "dw" "model.dp_bolome_order" ["dw-dt" "product-dw-id" "product-dw-src-id" "show-dw-id" "show-dw-src-id" "show-kind"
                                                                  "pay-date" "user-id" "order-id" "quantity" "price"
                                                                  "warehouse-id" "coupon-id" "event-id"
                                                                  "copon-discount-amount" "system-discount-amount" "tax-amount" "logistics-amount"]))
(def model-show-ti (pg-tap "dw" "model.d_bolome_show" ["dw-dt" "dw-ts" "dw-id" "show-id" "show-name" "begin-time" "end-time"]))
(def model-delta-order_show_debut-to (pg-tap "dw" "model_dd_bolome_order_show_debut"
                                             ["dw-id" "dw-src-id" "exist-preview" "pay-dt-kvs" "pay-dts" "begin-ts" "debut-dt"]))
(def model-shadow-order_show_debut-to (pg-tap "dw" "model_ds_bolome_order_show_debut"
                                              ["dw-id" "dw-src-id" "exist-preview" "pay-dt-kvs" "pay-dts" "begin-ts" "debut-dt"]))
(def model-order_show_debut-to (pg-tap "dw" "model_d_bolome_order_show_debut"
                                       ["dw-id" "dw-src-id" "exist-preview" "pay-dt-kvs" "pay-dts" "begin-ts" "debut-dt"]))
(def model-product_category-ti (pg-tap "dw" "model.d_bolome_product_category"
                                       ["dw-dt" "dw-ts" "dw-id" "barcode" "product-name" "category-1" "category-2"] ))

(defn -main []
  (create-table-if model-delta-order_show_debut-to
                   [[:dw_id :INT]
                    [:dw_src_id :TEXT]
                    [:exist_preview :TEXT]
                    [:pay_dt_kvs :TEXT]
                    [:pay_dts :TEXT]
                    [:begin_ts :TEXT]
                    [:debut_dt :TEXT]] )

  (create-table-if model-shadow-order_show_debut-to
                   [[:dw_id :INT]
                    [:dw_src_id :TEXT]
                    [:exist_preview :TEXT]
                    [:pay_dt_kvs :TEXT]
                    [:pay_dts :TEXT]
                    [:begin_ts :TEXT]
                    [:debut_dt :TEXT]])
  
  (create-table-if model-order_show_debut-to
                   [[:dw_id :INT]
                    [:dw_src_id :TEXT]
                    [:exist_preview :TEXT]
                    [:pay_dt_kvs :TEXT]
                    [:pay_dts :TEXT]
                    [:begin_ts :TEXT]
                    [:debut_dt :TEXT]] )
  
  (build-delta-tab model-delta-order_show_debut-to model-present-order-ti model-show-ti)
  (build-shadow-tab model-shadow-order_show_debut-to model-order_show_debut-to model-delta-order_show_debut-to)
  #_(prn {:dt-rng (save-and-load-rng-dt! stg-tap-in ["?begin-time" "?end-time"] identity)} "running...")
  #_(try (?- ods-tmp-tap-out (merge-stg-ods-tmp stg-tap-in ods-tap-out)) (catch Exception _))
  #_(replace-into-ods ods-tap-out ods-tmp-tap-out)
  (switch-tab model-order_show_debut-to model-shadow-order_show_debut-to)

  
  )

(comment
  (-main)
  )
