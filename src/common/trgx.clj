(ns common.trgx
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn
                                  cross-join select-fields num-out-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.logic.vars :as v]
            [taoensso.timbre :refer [info debug warn set-level!]]
            [clj-time.core :as t :refer [last-day-of-the-month-]]
            [clj-time.format :as tf]
            [clj-time.local :as tl]
            [clj-time.periodic :refer [periodic-seq]]
            [clojure.core.match :refer [match]]
            [cheshire.core :refer [generate-string]]
            [clojurewerkz.balagan.core :as tr :refer [extract-paths]]
            [clojure.java.jdbc :as j])
  (:import [cascading.tuple Fields]
           [cascading.jdbc JDBCTap JDBCScheme]) )

(set-level! :warn)

(defn convert-null [str] (if (and str (not= (clojure.string/lower-case str) "null")) str nil))
(defn or-tuple [& tuple] (->> tuple (partition 2) (mapv (partial some identity))))

(defn latest-ts [] (tf/unparse (->  (tf/formatter "yyyy-MM-dd'T'HH:mm:ssZ") (.withZone (t/default-time-zone)))  (t/now) ))
(defn parse-dt [dt] (tf/parse (tf/formatter "yyyy-MM-dd") dt) )
(defn unparse-dt [dt-obj] (tf/unparse (tf/formatter "yyyy-MM-dd") dt-obj))
(defn tomorrow-dt [dt] (-> (tf/parse (tf/formatter "yyyy-MM-dd") dt) (t/plus (t/days 1)) unparse-dt) )
(defn future-dt [] (-> (t/now) unparse-dt tomorrow-dt))
(defn simple-last-day [dt] (as-> (subs dt 0 6) it (tf/parse (tf/formatter "yyyyMM") it) (last-day-of-the-month- it) (unparse-dt it)))
(defn last-day [dt]        (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (last-day-of-the-month- it) (unparse-dt it)) )
(defn prev-last-day [dt]   (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (t/plus it (t/days -1)) (unparse-dt it)) ) 
(defn prev-last-month [dt] (as-> (subs dt 0 4) it (tf/parse (tf/formatter "yyyy") it) (t/plus it (t/days -1)) (unparse-dt it)) )
(defn prev-same-month [dt] (as-> (subs dt 0 7) it (tf/parse (tf/formatter "yyyy-MM") it) (t/plus it (t/months -12)) (last-day-of-the-month- it)  (unparse-dt it)) )

(comment
  (prev-same-month "2015-11-30") )

(defn mk-dts [start-dt end-dt]
  (let [[start-dt-obj end-dt-obj] (map #(->>  (subs % 0 10)  (tf/parse (tf/formatter "yyyy-MM-dd"))) [start-dt end-dt])]
    (vector (map #(->> % (tf/unparse (tf/formatter "yyyy-MM-dd")))
                 (periodic-seq start-dt-obj (t/plus end-dt-obj (t/days 1))  (t/days 1))) )) )
(defn mk-month-dts [start-dt end-dt]
  (vector (map #(->> % (tf/unparse (tf/formatter "yyyy-MM-dd")) last-day)
               (periodic-seq (parse-dt start-dt) (t/plus (parse-dt end-dt) (t/days 1)) (t/months 1)))) )

(defaggregatefn collect-kv ([] {}) ([acc k v] (assoc acc k v)) ([x] [x]) )
(defn vars->kv [header] (mapfn [& coll] (zipmap header coll)))
(defn vars->pair [header] (mapfn [& coll] [(map vector header coll)] ))

(defn node->id [x]  (or (some->> x str (re-find #"\[(\S+)]") second) (-> x str (clojure.string/replace #"^\[\]" "") )))
(defn map->id-map [x] (->> x (map (fn [[k v]] [(node->id k) v])) (into {})))
(defn kv->trgx [trgx]
  (mapfn [m] (clojure.walk/postwalk
              (fn [x] (match x [(node :guard #(contains? (map->id-map m) (node->id %))) node-attrs]
                             [node (merge-with merge node-attrs {:DATA ((map->id-map m) (node->id node))})] :else x) )
              trgx)) )

(defn kv->tuple [ks] (mapfn [m] (mapv m ks)))
(defn tkv-select [ks] (mapfn [m] (mapv m ks)))

(defmapfn kv->lkp [m & ks] (mapv m ks))
(defmapcatfn split-rows [x] x)

(defaggregatefn str-max
  ([] nil) ([x] [x])
  ([acc str] (if (neg? (compare acc (convert-null str))) (convert-null str) acc)))

(defaggregatefn str-min
  ([] nil) ([x] [x])
  ([acc str] (if (or (nil? acc) (and (convert-null str) (neg? (compare (convert-null str) acc))))  (convert-null str) acc)))


(defn tr-dimension-metrics [dimension-header metrics-header]
  (aggregatefn ([] {}) ([x] [x])
               ([acc & coll] (let [[dimension metrics] (split-at (count dimension-header) coll)
                                   dimension-pair (mapv vector dimension-header dimension)
                                   metrics-pair (mapv vector metrics-header metrics)]
                               (reduce #(assoc-in %1 (conj dimension-pair (first %2)) (second %2)) acc metrics-pair) ))))
(defmapfn pair-edn->json [pair-tree]
  (-> (clojure.walk/prewalk #(match [%]  [([(k :guard keyword?)  v] :guard (complement (partial instance? java.util.Map$Entry))) ]  (str (name k) "=" v) :else %) pair-tree) generate-string))

(def pg-spec {:dbtype "postgresql"
              :dbname "dw"
              :host "192.168.1.3"
              :user "ms"
              :password "spiderdt"
              :useSSL "true"
              :ssl "true"
              :sslmode "require"
              :characterEncoding "utf-8"
              :stringtype "unspecified"
              :sslkey "/data/ssl/client/client.key.pk8"
              :sslcert "/data/ssl/client/client.cert.pem"
              :sslrootcert "/data/ssl/client/root.cert.pem"
              :sslfactory "org.postgresql.ssl.jdbc4.LibPQFactory"} )

(defn pg-tap [db tabname header]
  (new JDBCTap (str "jdbc:postgresql://192.168.1.3:5432/" db
                    "?useSSL=true&ssl=true&characterEncoding=utf-8&stringtype=unspecified&sslmode=require&sslkey=/data/ssl/client/client.key.pk8&sslcert=/data/ssl/client/client.cert.pem&sslrootcert=/data/ssl/client/root.cert.pem&sslfactory=org.postgresql.ssl.jdbc4.LibPQFactory")
       "ms"
       "spiderdt"
       "org.postgresql.Driver"
       tabname
       (new JDBCScheme
            nil, nil
            (new Fields (into-array (map (partial str "?") header))
                 (into-array (repeat (count header) String)))
            (into-array (map #(clojure.string/replace % #"-" "_") header))
            nil, nil, -1, nil, nil, false)))

(defn report->next-dt [report-tap-out report]
  [(or (some-> (??<- [?dw-dt-max]
                     ((select-fields report-tap-out ["?dw-dt" "?report"] ) :> ?dw-dt report)
                     (str-max ?dw-dt :> ?dw-dt-max))
               ffirst tomorrow-dt)
       "1970-01-01")
   (future-dt) ] )

(defn detele-report! [report-tap-out report-name [start-dt end-dt]]
  (.executeUpdate report-tap-out (format  "DELETE FROM report WHERE project = 'cocacola' AND category = 'score' AND report = '%s' AND dw_dt BETWEEN '%s' AND '%s'" report-name start-dt end-dt) ) )

(defn create-table-if
  ([tap specs] (create-table-if tap (.getTableName tap) specs))
  ([tap tab specs] (let [spec-to-string (fn [spec] (clojure.string/join " " (cons (-> spec first name) (map name (rest spec)))))]
                 (.executeUpdate tap
                                 (format "CREATE TABLE IF NOT EXISTS %s (%s)"
                                         tab
                                         (clojure.string/join ", " (map spec-to-string specs)) ) ))) )

(defn ts->dt [ts] (subs ts 0 10))
(defn save-and-load-rng-dt! [stg-tap-in date-fields f]
  (let [stg-tab (.getTableName stg-tap-in)
        rng-tab (str stg-tab "_rng")
        _ (create-table-if stg-tap-in rng-tab
                           [[:dw_start_dt "CHAR(10)"]
                            [:dw_end_dt "CHAR(10)"]
                            [:dw_start_ts "CHAR(24)"]
                            [:dw_end_ts "CHAR(24)"]
                            [:dw_ld_cnt :INT]
                            [:dw_ld_ts "CHAR(24)"]])
        rng-tuple (as->
                      (cond (vector? date-fields)
                            (<- [?dw-start-date ?dw-end-date] ((select-fields stg-tap-in date-fields) :> ?dw-start-date ?dw-end-date))
                            (= date-fields :latest)
                            (<- [?dw-start-date ?dw-end-date]
                                  (stg-tap-in :>> (v/gen-nullable-vars (num-out-fields stg-tap-in)))
                                  (identity (repeat 2 (latest-ts)) :> ?dw-start-date  ?dw-end-date) )
                            (string? date-fields)
                            (<- [?dw-start-dt ?dw-end-dt]
                            ((select-fields stg-tap-in date-fields) :> ?dw-date)
                            ((c/juxt identity identity) ?dw-date :> ?dw-start-dt ?dw-end-dt) ) )
                      it
                    (??<- [?dw-start-dt-min ?dw-end-dt-max ?dw-start-ts-min ?dw-end-ts-max ?dw-ld-cnt ?dw-ld-ts]
                          (it :> ?src-dw-start-date ?src-dw-end-date)
                          ((c/each f) ?src-dw-start-date ?src-dw-end-date :> ?dw-start-ts ?dw-end-ts)
                          (str-min ?dw-start-ts :> ?dw-start-ts-min)
                          (str-max ?dw-end-ts :> ?dw-end-ts-max)
                          ((c/each ts->dt) ?dw-start-ts-min  ?dw-end-ts-max :> ?dw-start-dt-min ?dw-end-dt-max)
                          (c/count :> ?dw-ld-cnt)
                          (identity (latest-ts) :> ?dw-ld-ts))
                    (first it))]
    (j/insert! pg-spec rng-tab
               [:dw_start_dt :dw_end_dt :dw_start_ts :dw_end_ts :dw_ld_cnt :dw_ld_ts]
               rng-tuple)
    (take 2 rng-tuple)  ))

(defn load-max-dw-id [ods-tap-out]
  (or (ffirst (??<- [?max-dw-id]
                   ((select-fields ods-tap-out "?dw-id") :> ?dw-id)
                   (c/max ?dw-id :> ?max-dw-id)))
      0))

(defn cal-prt-dw-id-kv [prt-cnt-kv dw-id-max]
  (->> prt-cnt-kv
       (sort-by first)
       (reductions #(vector (first %2) (+ (second %1) (second %2))) [nil dw-id-max])
       (partition 2 1)
       (map #(vector (-> % second first) (-> % first second)))
       (into {})) )

(defn row-num [prt-cnt-kv dw-id-max]
  (aggregatefn
   ([] {:prt-kv (cal-prt-dw-id-kv prt-cnt-kv dw-id-max) :dw-id-kv []} )
   ([acc prt-no dw-id & tuple] (as-> acc $
                                    (if dw-id $ (update-in $ [:prt-kv prt-no] inc))
                                    (update $ :dw-id-kv conj {:dw-id (or dw-id (get-in $ [:prt-kv prt-no])) :tuple (vec tuple) }) ))
   ([x] [[(:dw-id-kv x)]])))

(defn replace-into-ods [ods-tap-out ods-tmp-tap-out]
  (let [ods-tmp-tab (-> ods-tmp-tap-out .getTableName)
        ods-tab (-> ods-tap-out .getTableName (clojure.string/split #"\.") second)]
    (.executeUpdate ods-tap-out (format "SET search_path=ods; DROP TABLE IF EXISTS %s CASCADE" ods-tab))
    (.executeUpdate ods-tap-out (format "SET search_path=public; ALTER TABLE %s SET SCHEMA ods" ods-tmp-tab))
    (.executeUpdate ods-tap-out (format "SET search_path=ods; ALTER TABLE %s RENAME TO %s" ods-tmp-tab ods-tab))) )

(defn delete-table-data [tap-out [start-dt end-dt]]
  )

(defn latest-trgx-root []
  (-> (??<- [?data] ((pg-tap "dw" "conf.trgx_cocacola" ["key" "data" "dw_in_use" "dw_ld_ts"]) :> "KPI" ?data "1" _))
      ffirst read-string))

(defn latest-trgx-merge []
  (-> (latest-trgx-root)
      (assoc-in 
       ["全体-Total / 所有渠道" :CHILDREN]
       (-> (latest-trgx-root) (get-in ["全体-Total / 所有渠道" :BRANCH]) vals (->> (apply merge))) )
      (update-in ["全体-Total / 所有渠道"] dissoc :BRANCH)) )

(defn latest-trgx-kpi []
  (-> (latest-trgx-root) (get-in ["全体-Total / 所有渠道" :BRANCH :CODE]))  )

(defn trgx->leaf [trgx]
  (->> trgx (tree-seq map? #(interleave (keys %) (vals %))) rest (partition 2) (filter #(= (:CHILDREN (second %)) {})) (map vec) (into {})))

(defn trgx-get-in [trgx ks]
  (get-in trgx (-> (interleave ks (repeat :CHILDREN)) butlast) ) )
(defn trgx-assoc-in [trgx ks v]
  (assoc-in trgx (-> (interleave ks (repeat :CHILDREN)) butlast) v))

(defn trgx->path [trgx]
  (->>  trgx extract-paths (filter #(= (last %) :DATA))  (map (comp (juxt last identity) (partial take-nth 2))) (into {})) )
(defn path-trgx->path [trgx]
  (->> trgx extract-paths (filter #(= (last %) :DATA)) (map (comp (juxt last identity) butlast)) (into {})) )


(defn trgx-path-seq [trgx]
  (->> trgx extract-paths (filter #(= (last %) :DATA)) (map (partial take-nth 2))) )

(defn trgx-take [n trgx]
  (reduce
   #(trgx-assoc-in %1 %2 (assoc (trgx-get-in trgx %2) :CHILDREN {}))
   {}  (->> trgx trgx-path-seq (filter #(<= (count %) 3))))  )

(defn trgx-last [trgx]
  (let [path-seq (->> trgx trgx-path-seq)
        path-seq-butlast (->> path-seq (map butlast) set)]
    (remove #(contains? path-seq-butlast %) path-seq)) )

(defn trgx-take-last [n trgx]
  (let [new-trgx (trgx-take n trgx)
        trgx-leaf (trgx-last trgx)
        take-last (fn [n coll] (let [[header tail] (split-at n coll)] (if (last tail) (conj (vec header) (last tail)) header)))]
    (reduce #(trgx-assoc-in %1 (take-last n %2) (trgx-get-in trgx %2)) 
            new-trgx trgx-leaf)) )

(defn trgx-split-at [n trgx]
  (->> trgx trgx-path-seq (filter #(<= (count %) n)) (mapv #(hash-map :PATH % :SUBTREE {(last %) (trgx-get-in trgx %) })) ) )

(defn pad [n v coll] (take n (concat coll (repeat v))))

(comment
  (def trgx-merge (latest-trgx-merge))
  (map prn
       (->> (trgx-take-last 3 trgx-merge)  (trgx-split-at 3))
       )
  (def data [{:path [1 2 3] :val 123}
             {:path [11 22 33 44] :val 456}])
  
  (??<- [?a ?b ?c ?d]
        (data :> ?m)
        ((kv->tuple [:path]) ?m :> ?path)
        (pad 4 -1 ?path :> ?a ?b ?c ?d))
  )

(defn trgx-leaf-trunc [level trgx]
  (let [[trgx-leaf trgx-path] ((juxt trgx->leaf trgx->path) trgx)]
    (reduce (fn [acc [leaf-key leaf-value]]
              (assoc-in acc (as-> leaf-key $ (trgx-path $) (take level $) (vec $) (conj $ leaf-key))  leaf-value))
            {} trgx-leaf) ))

(defn trgx->kv [trgx]
  (->> trgx trgx->path vals (mapv #(vector % (get-in trgx (-> (interleave % (repeat :CHILDREN)) butlast)))) (into {})) )
(defn path-trgx->pair [trgx] (->> trgx path-trgx->path vals (mapv #(vector % (get-in trgx %)))))
(defn path-trgx->kv [trgx] (->> trgx path-trgx->pair (into {}))  )
(defn path-trgx->tuple-kv [header trgx] (->> trgx path-trgx->pair (map (fn [[path value]] (zipmap header (conj (vec path)  value)) ))) )


(comment
  (trgx->kv trgx-kpi)
  (path-trgx->tuple-kv [:channel :kpi :metrics :value] (trgx-leaf-trunc 2 trgx-kpi))
  (map count (path-trgx->tuple (trgx-leaf-trunc 2 trgx-kpi)))
  (def stg-tap-in (pg-tap "dw" "stg.d_bolome_show" ["show-id" "show-name" "begin-time" "end-time"]))
  (def ods-tap-out (pg-tap "dw" "ods.d_bolome_show" ["dw-dt" "dw-ts" "dw-id" "show-id" "show-name" "begin-time" "end-time"]))
  
  
  (defaggregatefn str-min
    ([] nil) ([x] [x])
    ([acc str] (prn {:acc acc :str str} ) (if (or (nil? acc) (and (convert-null str) (neg? (compare (convert-null str) acc))))  (convert-null str) acc)))

  (??<- [?a]
        (stg-tap-in :>> (v/gen-nullable-vars (num-out-fields stg-tap-in)))
        (identity "a" :> ?a)
        )
  (into [] #{"a" "b"})
  )
