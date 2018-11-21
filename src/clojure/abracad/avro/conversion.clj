(ns abracad.avro.conversion
  "Logical Type converter implementations"
  (:import (org.apache.avro Conversions$UUIDConversion Conversions$DecimalConversion)
           (java.math MathContext)
           (abracad.avro ClojureData Java8LogicalTypes$RoundingDecimalConversion Java8LogicalTypes$DateConversion Java8LogicalTypes$TimeMillisConversion Java8LogicalTypes$TimeMicrosConversion Java8LogicalTypes$TimestampMillisConversion Java8LogicalTypes$TimestampMicrosConversion)))

(def ^:dynamic *use-logical-types*
  "When true, record field values will be (de)serialised from/to their logical types
  e.g. {:type :int :logicalType :date} -> java.time.LocalDate. Default value is `true`."
  true)

(def uuid-conversion (Conversions$UUIDConversion.))
(def date-conversion (Java8LogicalTypes$DateConversion.))
(def time-conversion (Java8LogicalTypes$TimeMillisConversion.))
(def time-micros-conversion (Java8LogicalTypes$TimeMicrosConversion.))
(def timestamp-conversion (Java8LogicalTypes$TimestampMillisConversion.))
(def timestamp-micros-conversion (Java8LogicalTypes$TimestampMicrosConversion.))

(defn- default-conversions []
  (let [^MathContext context *math-context*
        decimal-conversion   (if context
                               (Java8LogicalTypes$RoundingDecimalConversion. (.getRoundingMode context))
                               (Conversions$DecimalConversion.))]
    [uuid-conversion date-conversion time-conversion time-micros-conversion
     timestamp-conversion timestamp-micros-conversion decimal-conversion]))

(defn create-clojure-data []
  (if *use-logical-types*
    (ClojureData. (default-conversions))
    (ClojureData/withNoConversions)))
