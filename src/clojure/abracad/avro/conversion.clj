(ns abracad.avro.conversion
  "Logical Type converter implementations"
  (:import (java.time LocalDate LocalTime Instant)
           (org.apache.avro Conversions$UUIDConversion Conversions$DecimalConversion Conversion)
           (java.time.temporal ChronoField ChronoUnit)))

;; TODO conversions in a map lt -> conversion so you can merge.
;; TODO should be global binding dynamic thing?? Or just pass in logical types?
(defn conversion? [x]
  (instance? Conversion x))

;; TODO specing the clojure conversions
;; TODO spice up the destructuring, feels like there should be a nicer way
;; TODO each of the proxy-super invocations causes a compiler error? Perhaps they need type hints?
;; TODO all of the remaining function implementations
(defn coerce [conversion]
  (if (conversion? conversion) conversion                   ;; TODO error if the mapped conversion does not match the java .getLogicalTypeName value
                               (let [type (:class conversion)
                                     logicalType (:logical-type conversion)
                                     int-fns (:int conversion)
                                     long-fns (:long conversion)
                                     string-fns (:string conversion)]
                                 (proxy [Conversion] []
                                   (getConvertedType [] type)
                                   (getLogicalTypeName []  logicalType)
                                   (fromInt [int schema lt] (let [from (:from int-fns)]
                                                              (if (nil? from)
                                                                (proxy-super fromInt int schema lt)
                                                                (from int schema lt))))
                                   (toInt [data schema lt] (let [to (:to int-fns)]
                                                             (if (nil? to)
                                                               (proxy-super toInt data schema lt)
                                                               (to data schema lt))))
                                   (fromLong [long schema lt] (let [from (:from long-fns)]
                                                                (if (nil? from)
                                                                  (proxy-super fromLong long schema lt)
                                                                  (from long schema lt))))
                                   (toLong [data schema lt] (let [to (:to long-fns)]
                                                              (if (nil? to)
                                                                (proxy-super toLong data schema lt)
                                                                (to data schema lt))))
                                   (fromCharSequence [string schema lt] (let [from (:from string-fns)]
                                                                          (if (nil? from)
                                                                            (proxy-super fromCharSequence string schema lt)
                                                                            (from string schema lt))))
                                   (toCharSequence [data schema lt] (let [to (:to string-fns)]
                                                                      (if (nil? to)
                                                                        (proxy-super toCharSequence data schema lt)
                                                                        (to data schema lt))))))))

(def date-conversion
  {:logical-type "date"
   :class        LocalDate
   :int          {:from (fn [day ^Integer _ _] (LocalDate/ofEpochDay day))
                  :to   (fn [day ^LocalDate _ _] (Math/toIntExact (.toEpochDay day)))}})

(def time-conversion
  {:logical-type "time-millis"
   :class        LocalTime
   :int          {:from (fn [time ^Integer _ _] (.plus LocalTime/MIDNIGHT (long time) ChronoUnit/MILLIS))
                  :to   (fn [time ^LocalTime _ _] (.get time ChronoField/MILLI_OF_DAY))}})

(def timestamp-conversion
  {:logical-type "timestamp-millis"
   :class        Instant
   :long         {:from (fn [millis ^Long _ _] (Instant/ofEpochMilli millis))
                  :to   (fn [instant ^Instant _ _] (.toEpochMilli instant))}})

(def uuid-conversion (Conversions$UUIDConversion.))

(def decimal-conversion (Conversions$DecimalConversion.))

(def default-conversions
  [date-conversion, time-conversion, timestamp-conversion, uuid-conversion, decimal-conversion])

;; TODO conversion for decimal that rounds and sets scale before conversion
;; TODO keyword logical type and conversion
;; TODO ISO Date String logical type?
