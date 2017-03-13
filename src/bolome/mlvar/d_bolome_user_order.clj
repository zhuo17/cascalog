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
            [common.trgx :refer :all]
            [clojure.java.jdbc :as j]
            [cascalog.cascading.operations :as ops]
            [clojure.math.combinatorics :as combo])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]))

(set-level! :warn)

#_(type (into (sorted-map) {:a "aa"})) 

(defaggregatefn collect-set ([] #{}) ([acc x] (conj acc x)) ([x] [x]))
(defaggregatefn collect-kvs ([] {}) ([acc k v] (update acc k #(conj (or % #{}) v))) ([x] [x]))
(defmapfn sort-keys [m] (->> (doto m (prn "@sort-keys")) (into (sorted-map)) keys vec vector))

(defmapfn cif [test then else] (if test then else))
(defmapfn nvl2 [test then else] (if test then else))
(defn cor [& xs] (some identity xs))
(defn str-least [& ks] (->> ks (keep identity) sort first) )
(defn mark* [xs x]  (mapv (partial = x) xs))
(defn compare-mark* [x y] (if (and x y) (let [v (compare x y)][(neg? v) (= 0 v) (pos? v)]) [false false false] ))



#_(defaggregatefn collect-kv ([] {}) ([acc k v] (assoc acc k v)) ([x] [x]))

(defn truncate-tab [tap]
  (.executeUpdate tap (format "TRUNCATE TABLE %s" (-> tap .getTableName))))

(defn switch-tab [src-tap tgt-tap]
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName) (-> tgt-tap .getTableName (str "_t"))) ) 
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> src-tap .getTableName) (-> tgt-tap .getTableName)) )
  (.executeUpdate tgt-tap (format "ALTER TABLE %s RENAME TO %s" (-> tgt-tap .getTableName (str "_t")) (-> src-tap .getTableName)) )  )

(defn c> [& xs] (when (every? identity xs) (apply > xs)))
(defn c>* [& xs] (or (apply c> xs) false))

(def mlin-present-user_order-ti (pg-tap "dw" "mlin.dp_bolome_user_order"
                                        ["dw-dt" "dw-src-id" "order-dw-src-id" "product-dw-id" "product-dw-src-id"
                                         "user-id" "pay-dt" "product-category" "product-subcat"
                                         "quantity" "revenue" "base-revenue" "discount-amount"
                                         "order-ste" "order-pe" "order-preview" "order-debut" "order-replay"]))

(def mlvar-delta-user_order-to (pg-tap "dw" "mlvar_dd_bolome_user_order" ["dw-src-id" "trgx"]))
(def mlvar-user_order-to (pg-tap "dw" "mlvar_d_bolome_user_order" ["dw-src-id" "trgx"]))
#_(def mlvar-present-user_order-to (pg-tap "dw" "mlvar_dp_bolome_user_order" ["dw-src-id" "trgx"]))
(defn ziptkv [header] (mapfn [& coll] (zipmap header coll)))

(defn build-delta-tab [mlvar-delta-user_order-to mlin-present-user_order-ti]
  (truncate-tab mlvar-delta-user_order-to)
  (as-> (<- [?user-id ?dw-dt ?product-dw-id ?product-dw-src-id ?product-category ?order-dw-src-id ?order-data-pr ?order-children]
             (mlin-present-user_order-ti :> ?dw-dt ?dw-src-id ?order-dw-src-id ?product-dw-id ?product-dw-src-id
                                         ?user-id ?pay-dt ?product-category ?product-subcat
                                         ?quantity ?revenue ?base-revenue ?discount-amount
                                         ?order-ste ?order-pe ?order-preview ?order-debut ?order-replay)
             ((ziptkv [:product-dw-id :product-dw-src-id :product-category :product-subcat
                       :quantity :revenue :base-revenue :discount-amount
                       :order-ste :order-pe :order-preview :order-debut :order-replay])
                       ?product-dw-id ?product-dw-src-id ?product-category ?product-subcat
                       ?quantity ?revenue ?base-revenue ?discount-amount
                       ?order-ste ?order-pe ?order-preview ?order-debut ?order-replay
                     :> ?order-item-data)
             (identity {} :> ?order-item-children)
             ((ziptkv [:DATA :CHILDREN]) ?order-item-data ?order-item-children :> ?order-item-trgx)
             (collect-kv ?product-dw-id ?order-item-trgx :> ?order-children)
             ((ziptkv [:order-dw-src-id ]) ?order-dw-src-id :> ?order-data)
             (pr-str ?order-data :> ?order-data-pr)
             )
        $
        (<- [?user-id ?dw-dt ?product-dw-id ?product-data-pr ?product-children]
            ($ :> ?user-id ?dw-dt ?product-dw-id ?product-dw-src-id ?product-category ?order-dw-src-id ?order-data-pr ?order-children)
            (clojure.edn/read-string ?order-data-pr :> ?order-data)
            ((ziptkv [:DATA :CHILDREN]) ?order-data ?order-children :> ?order-trgx)
            (collect-kv ?order-dw-src-id ?order-trgx :> ?product-children)
            ((ziptkv [:product-category :product-dw-id :product-dw-src-id])
                      ?product-category ?product-dw-id ?product-dw-src-id :> ?product-data)
            (pr-str ?product-data :> ?product-data-pr)
            )
        (<- [?user-id ?dw-dt ?dw-dt-data-pr ?dw-dt-children]
            ($ :> ?user-id ?dw-dt ?product-dw-id ?product-data-pr ?product-children)
            (clojure.edn/read-string ?product-data-pr :> ?product-data)
            ((ziptkv [:DATA :CHILDREN]) ?product-data ?product-children :> ?product-trgx)
            (collect-kv ?product-dw-id ?product-trgx :> ?dw-dt-children)
            ((ziptkv [:dw-dt]) ?dw-dt :> ?dw-dt-data)
            (pr-str ?dw-dt-data :> ?dw-dt-data-pr)
            )
        (<- [?user-id ?user-data-pr ?user-children]
            ($ :> ?user-id ?dw-dt ?dw-dt-data-pr ?dw-dt-children)
            (clojure.edn/read-string ?dw-dt-data-pr :> ?dw-dt-data)
            ((ziptkv [:DATA :CHILDREN]) ?dw-dt-data ?dw-dt-children :> ?dw-dt-trgx)
            (collect-kv ?dw-dt ?dw-dt-trgx :> ?user-children)
            ((ziptkv [:user-id]) ?user-id :> ?user-data)
            (pr-str ?user-data :> ?user-data-pr)
            )
        (<- [?user-id ?user-trgx]
            ($ :> ?user-id ?user-data-pr ?user-children)
            (clojure.edn/read-string ?user-data-pr :> ?user-data)
            ((ziptkv [:DATA :CHILDREN]) ?user-data ?user-children :> ?user-trgx))
        #_(??- $)
        (?- mlvar-delta-user_order-to
            (ops/rename* $ ["?dw-src-id" "?trgx"])))  )

(defn -main []
  (create-table-if mlvar-delta-user_order-to
                   [[:dw_src_id :TEXT]
                    [:trgx :TEXT]])

  (create-table-if mlvar-user_order-to
                   [[:dw_src_id :TEXT]
                    [:trgx :TEXT]])
  
  (build-delta-tab mlvar-delta-user_order-to mlin-present-user_order-ti)
  
  (comment
    )
  )

(comment
  (-main)
  )
