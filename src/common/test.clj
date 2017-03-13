(ns commmon.test
  (:require
   [clj-time.core :as t :refer [last-day-of-the-month-]]
   [clj-time.format :as tf]
   [clj-time.local :as tl]
   [clj-time.periodic :refer [periodic-seq]]
   ))

(defn parse-dt [dt] (tf/parse (tf/formatter "yyyy-MM-dd") dt))
(defn unparse-dt [dt-obj] (tf/unparse (tf/formatter "yyyy-MM-dd") dt-obj))
(defn offset-dt [dt n] (-> dt parse-dt  (t/plus (t/days n)) unparse-dt))
(defn dt-ge-get [dt-1 dt-2] (if (<= (compare dt-2 dt-1) 0) dt-1) )

(defn str-min [& coll] (-> coll sort first))

(defn dt-rebase [[new-start-dt new-end-dt] [start-dt end-dt]]
  (let [dt-interval (-> (t/interval (parse-dt start-dt) (parse-dt end-dt)) t/in-days)]
    [(or new-start-dt (offset-dt new-end-dt (- dt-interval)))
     (or new-end-dt (offset-dt new-start-dt dt-interval))] ))

(defn take-train-dts [n back-step [dm-start-dt dm-end-dt] [dw-min-dt dw-max-dt] intervals]
  (let [his-max-dt (str-min dw-max-dt (offset-dt dm-start-dt -1))
        train-base-seq (->> (periodic-seq (parse-dt his-max-dt) (t/days (- back-step)) )
                         (map unparse-dt) (take-while #(dt-ge-get % dw-min-dt)))
        cal-dts   (fn [dt] (let [[t-start-dt t-end-dt] (dt-rebase [nil dt] [dm-start-dt dm-end-dt])
                                [h-end-dt _] (dt-rebase [nil t-end-dt] [his-max-dt dm-end-dt])
                                 h-start-dts (keep #(when-let [each-h-end-dt (dt-ge-get (offset-dt h-end-dt (- %)) dw-min-dt)]
                                                     [(->> % inc (format "h-start-%sdays") keyword) each-h-end-dt])
                                                  intervals)]
                            (if (not-empty h-start-dts)
                              (apply merge {:t-start t-start-dt :t-end t-end-dt :h-end h-end-dt} h-start-dts) ) ))
        train-seq (->> train-base-seq (map cal-dts) (take-while identity) (map-indexed #(assoc %2 :Iter (inc %1))))
        all-keys (concat [:t-start :t-end :h-end] (map #(->> % inc (format "h-start-%sdays") keyword) intervals))
        ]
    (->> 
     (if (< n (count train-seq))
       (take n train-seq)
       (take-while #(= (keys %) all-keys) train-seq))
     (cons (merge (cal-dts dm-end-dt) {:Iter 0})) )  ))

(map prn
     (take-train-dts 30 3 ["2016-12-12" "2017-01-10"] ["2014-05-01" "2016-06-30"] #{3 6 13 20 27 59 29})
     )

(->> (periodic-seq (parse-dt "2016-06-30")  (t/days (- 3))) (map unparse-dt) (take 10))
