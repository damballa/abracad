(ns abracad.avro.converter
  "Logical Type converter implementations"
  (:import (java.time LocalDate Instant)
           (org.apache.avro Conversions$UUIDConversion Conversions$DecimalConversion)
           (org.apache.avro.data TimeConversions$DateConversion
                                 TimeConversions$TimeConversion
                                 TimeConversions$LossyTimeMicrosConversion
                                 TimeConversions$TimestampConversion
                                 TimeConversions$LossyTimestampMicrosConversion)))

;; Java 8+

(def date-conversion
  {:logical-type :date
   :int          {:from (fn [day ^Integer _ _] (LocalDate/ofEpochDay day))
                  :to   (fn [day ^LocalDate _ _] (Math/toIntExact (.toEpochDay day)))}})
;; TODO localtime time millis & micros conversion

(def timestamp-conversion
  {:logical-type :timestamp-millis
   :long         {:from (fn [millis ^Long _ _] (Instant/ofEpochMilli millis))
                  :to   (fn [instant ^Instant _ _] (.toEpochMilli instant))}})
;; TODO timstamp micros conversion

(def uuid-conversion (Conversions$UUIDConversion.))

(def decimal-conversion (Conversions$DecimalConversion.))

(def default-conversions
  [date-conversion, timestamp-conversion, uuid-conversion, decimal-conversion])

;; TODO conversion for decimal that rounds and sets scale before conversion

;; Java 7
;; TODO tests of joda time converters
(def joda-date-conversion (TimeConversions$DateConversion.))
(def joda-time-conversion (TimeConversions$TimeConversion.))
(def joda-time-micros-conversion (TimeConversions$LossyTimeMicrosConversion.)) ;; Lossy works in both (de)serialisation, standard only works with deserialisation
(def joda-timestamp-conversion (TimeConversions$TimestampConversion.))
(def joda-timestamp-micros-conversion (TimeConversions$LossyTimestampMicrosConversion.)) ;; Lossy works in both (de)serialisation, standard only works with deserialisation

(def default-java7-convertsions
  [joda-date-conversion, joda-time-conversion, joda-time-micros-conversion, joda-timestamp-conversion, joda-timestamp-micros-conversion, uuid-conversion, decimal-conversion])
