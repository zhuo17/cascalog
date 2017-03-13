(ns hadoop.bolome.d_bolome_order
  (:require [cascalog.api :refer [?- ??- <- ?<- ??<- stdout defmapfn mapfn defmapcatfn mapcatfn defaggregatefn aggregatefn cross-join select-fields]]
            [cascalog.logic.ops :as c]
            [cascalog.cascading.tap :refer [hfs-seqfile]]
            [cascalog.more-taps :refer [hfs-delimited hfs-wrtseqfile hfs-wholefile]]))

#_(def stg-d_bolome_order-tap (pg-tap "dw" "stg.d_bolome_show" ["show-id" "show-name" "begin-time" "end-time"]))
(as-> (<- [?first-field]
          ())
    $)

