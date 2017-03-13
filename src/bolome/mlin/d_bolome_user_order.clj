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

(ns bolome.mlin.d-bolome-user_order
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
(defn mark* [xs x]  (mapv (partial = x) xs))
(defn compare-mark* [x y] (if (and x y) (let [v (compare x y)][(neg? v) (= 0 v) (pos? v)]) [false false false] ))

(defn truncate-tab [tap]
  (.executeUpdate tap (format "TRUNCATE TABLE %s" (-> tap .getTableName))))

(defn delete-table [tap rng-tap]
  (.executeUpdate tap (format "DELETE FROM  %s WHERE " (-> tap .getTableName)))
  )

(defn switch-tab [src-tap tgt-tap]
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName) (-> tgt-tap .getTableName (str "_t"))) ) 
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> src-tap .getTableName) (-> tgt-tap .getTableName)) )
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName (str "_t")) (-> src-tap .getTableName)) )  )

(defn c> [& xs] (when (every? identity xs) (apply > xs)))
(defn c>* [& xs] (or (apply c> xs) false))

(defn build-present-tab [mlin-present-user_order-ti model-present-order-ti model-order_show_debut-to model-event-ti model-product_category-ti]
  (truncate-tab mlin-present-user_order-ti)
  (as-> (<- [?src-dw-dt ?dw-src-id ?src-order-id ?j-product-dw-id ?src-product-dw-src-id
             ?src-user-id ?src-pay-dt ?product-category ?product-subcat
             ?src-quantity ?revenue ?base-revenue ?discount-amount
             ?b-order-ste ?b-order-pe ?b-order-preview ?b-order-debut ?b-order-replay]
            (model-present-order-ti :> ?src-dw-dt ?j-product-dw-id ?src-product-dw-src-id !j-show-dw-id !src-show-dw-src-id !src-show-kind
                                       ?src-pay-dt ?src-user-id ?src-order-id ?src-quantity ?src-price
                                       ?src-warehouse-id !src-coupon-id ?j-event-id
                                       ?src-copon-discount-amount ?src-system-discount-amount ?src-tax-amount ?src-logistics-amount)
            (identity ?src-user-id :> ?dw-src-id)
            (* ?src-price ?src-quantity :> ?revenue)
            (+ ?revenue ?src-tax-amount ?src-logistics-amount :> ?base-revenue)
            (+ ?src-copon-discount-amount ?src-system-discount-amount :> ?discount-amount)
            ((select-fields model-event-ti ["?event-id" "?type-name"]) :> ?j-event-id !!type-name)
            (mark* ["专题" "活动"] !!type-name :> ?b-order-ste ?b-order-pe)
            (c>* !src-coupon-id 0 :> ?b-order-coupon)
            ((select-fields model-order_show_debut-to ["?dw-id" "?debut-dt"]) :> !j-show-dw-id !!show-debut-dt)
            (compare-mark* ?src-pay-dt !!show-debut-dt :> ?b-order-preview ?b-order-debut ?b-order-replay)
            ((select-fields model-product_category-ti ["?dw-id" "?category-1" "?category-2"]) :> ?j-product-dw-id ?product-category ?product-subcat))
      $
      (?- mlin-present-user_order-ti
        (ops/rename* $ ["?dw-dt" "?dw-src-id" "?order-dw-src-id" "?product-dw-id" "?product-dw-src-id"
                        "?user-id" "?pay-dt" "?product-category" "?product-subcat"
                        "?quantity" "?revenue" "?base-revenue" "?discount-amount"
                        "?order-ste" "?order-pe" "?order-preview" "?order-debut" "?order-replay"]))) )

(comment
  (count
   (??<- [?src-dw-dt ?dw-src-id ?src-order-id ?j-product-dw-id ?src-product-dw-src-id
          ?src-user-id ?src-pay-dt ?product-category ?product-subcat
          ?src-quantity ?revenue ?base-revenue ?discount-amount
            ?b-order-ste ?b-order-pe ?b-order-preview ?b-order-debut ?b-order-replay]
         (model-present-order-ti :> ?src-dw-dt ?j-product-dw-id ?src-product-dw-src-id !j-show-dw-id !src-show-dw-src-id !src-show-kind
          ?src-pay-dt ?src-user-id ?src-order-id ?src-quantity ?src-price
          ?src-warehouse-id !src-coupon-id ?j-event-id
          ?src-copon-discount-amount ?src-system-discount-amount ?src-tax-amount ?src-logistics-amount)
         (identity ?src-user-id :> ?dw-src-id)
         (* ?src-price ?src-quantity :> ?revenue)
         (+ ?revenue ?src-tax-amount ?src-logistics-amount :> ?base-revenue)
         (+ ?src-copon-discount-amount ?src-system-discount-amount :> ?discount-amount)
         ((select-fields model-event-ti ["?event-id" "?type-name"]) :> ?j-event-id !!type-name)
         (mark* ["专题" "活动"] !!type-name :> ?b-order-ste ?b-order-pe)
         (c>* !src-coupon-id 0 :> ?b-order-coupon)
         ((select-fields model-order_show_debut-to ["?dw-id" "?debut-dt"]) :> !j-show-dw-id !!show-debut-dt)
         (compare-mark* ?src-pay-dt !!show-debut-dt :> ?b-order-preview ?b-order-debut ?b-order-replay)
         ((select-fields model-product_category-ti ["?dw-id" "?category-1" "?category-2"]) :> ?j-product-dw-id ?product-category ?product-subcat))
   )
  )

(def mlin-present-user_order-ti (pg-tap "dw" "mlin_dp_bolome_user_order"
                                        ["dw-dt" "dw-src-id" "order-dw-src-id" "product-dw-id" "product-dw-src-id"
                                         "user-id" "pay-dt" "product-category" "product-subcat"
                                         "quantity" "revenue" "base-revenue" "discount-amount"
                                         "order-ste" "order-pe" "order-preview" "order-debut" "order-replay"]))
(def mlin-user_order-ti (pg-tap "dw" "mlin_d_bolome_user_order"
                                ["dw-dt" "dw-src-id" "order-dw-src-id" "product-dw-id" "product-dw-src-id"
                                 "user-id" "pay-dt" "product-category" "product-subcat"
                                 "quantity" "revenue" "base-revenue" "discount-amount"
                                 "order-ste" "order-pe" "order-preview" "order-debut" "order-replay"]))

(def model-event-ti (pg-tap "dw" "model.d_bolome_event" ["dw-dt" "event-id" "type-name" "event-name" "create-date"]))
(def model-present-order-ti (pg-tap "dw" "model.dp_bolome_order" ["dw-dt" "product-dw-id" "product-dw-src-id" "show-dw-id" "show-dw-src-id" "show-kind"
                                                                  "pay-dt" "user-id" "order-id" "quantity" "price"
                                                                  "warehouse-id" "coupon-id" "event-id"
                                                                  "copon-discount-amount" "system-discount-amount" "tax-amount" "logistics-amount"]))
(def model-product_category-ti (pg-tap "dw" "model.d_bolome_product_category"
                                       ["dw-dt" "dw-ts" "dw-id" "barcode" "product-name" "category-1" "category-2"]))
(def model-order_show_debut-to (pg-tap "dw" "model_d_bolome_order_show_debut"
                                       ["dw-id" "dw-src-id" "exist-preview" "pay-dt-kvs" "pay-dts" "begin-ts" "debut-dt"]))


(defn -main []
  (create-table-if mlin-present-user_order-ti
                   [[:dw_dt "CHAR(10)"]
                    [:dw_src_id :TEXT]
                    [:order_dw_src_id :TEXT]
                    [:product_dw_id :INT]
                    [:product_dw_src_id :TEXT]
                    [:user_id :TEXT]
                    [:pay_dt :TEXT]
                    [:product_category :TEXT]
                    [:product_subcat :TEXT]
                    [:quantity :INT]
                    [:revenue "NUMERIC(18,3)"]
                    [:base_revenue "NUMERIC(18,3)"]
                    [:discount_amount "NUMERIC(18,3)"]
                    [:order_ste :TEXT]
                    [:order_pe :TEXT]
                    [:order_preview :TEXT]
                    [:order_debut :TEXT]
                    [:order_replay :TEXT]])
  
  (create-table-if mlin-user_order-ti
                   [[:dw_dt "CHAR(10)"]
                    [:dw_src_id :TEXT]
                    [:order_dw_src_id :TEXT]
                    [:product_dw_id :INT]
                    [:product_dw_src_id :TEXT]
                    [:user_id :TEXT]
                    [:pay_dt :TEXT]
                    [:product_category :TEXT]
                    [:product_subcat :TEXT]
                    [:quantity :INT]
                    [:revenue "NUMERIC(18,3)"]
                    [:base_revenue "NUMERIC(18,3)"]
                    [:discount_amount "NUMERIC(18,3)"]
                    [:order_ste :TEXT]
                    [:order_pe :TEXT]
                    [:order_preview :TEXT]
                    [:order_debut :TEXT]
                    [:order_replay :TEXT]])
  
  (build-present-tab mlin-present-user_order-ti model-present-order-ti model-order_show_debut-to model-event-ti model-product_category-ti)
  (.executeUpdate mlin-user_order-ti (clojure.string/join "\n" ["DELETE FROM mlin_d_bolome_user_order tgt"
                                                                "USING stg.d_bolome_order_rng rng"
                                                                "WHERE rng.dw_in_use = '1'"
                                                                "AND tgt.dw_dt BETWEEN rng.dw_start_dt AND rng.dw_end_dt ;"]) )
  (?- mlin-user_order-ti mlin-present-user_order-ti)
  
  #_(prn {:dt-rng (save-and-load-rng-dt! stg-tap-in ["?begin-time" "?end-time"] identity)} "running...")
  #_(try (?- ods-tmp-tap-out (merge-stg-ods-tmp stg-tap-in ods-tap-out)) (catch Exception _))
  #_(replace-into-ods ods-tap-out ods-tmp-tap-out)
  #_(switch-tab model-order_show_debut-to model-shadow-order_show_debut-to)
  )

(comment
  (-main)
  )
