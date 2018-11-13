(ns abracad.avro.conversion
  "Logical Type converter implementations"
  (:import (java.time LocalDate LocalTime Instant)
           (org.apache.avro Conversions$UUIDConversion Conversions$DecimalConversion Conversion LogicalTypes LogicalType Schema LogicalTypes$Decimal)
           (java.time.temporal ChronoField ChronoUnit)
           (clojure.lang Keyword APersistentMap)
           (abracad.avro KeywordLogicalTypeFactory)
           (java.lang.reflect Field)
           (java.math RoundingMode)
           (java.nio ByteBuffer)
           (org.apache.avro.generic GenericFixed)))

(defn conversion? [x]
  (instance? Conversion x))

(defn java-conversion [logical-type ^Conversion conversion]
  (let [expected-type (.getLogicalTypeName conversion)]
    (assert
      (= logical-type expected-type)
      (str "Java Logical Type Conversions must be keyed agains the logical type they can handle. Key " logical-type
           " was used instead of " expected-type " for " conversion)))
  conversion)

;; TODO all of the remaining function implementations
;; TODO spice up the destructuring, feels like there should be a nicer way
;; TODO each of the proxy-super invocations causes a compiler error? Perhaps they need type hints?
(defn clojure-conversion [logical-type conversion]
  (let [type        (:class conversion)
        int-fns     (:int conversion)
        long-fns    (:long conversion)
        string-fns  (:string conversion)
        bytes-fns   (:bytes conversion)
        fixed-fns   (:fixed conversion)]
    (proxy [Conversion] []
      (getConvertedType [] type)
      (getLogicalTypeName []  logical-type)
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
                                           (to data schema lt))))
      (fromBytes [bytes schema lt] (let [from (:from bytes-fns)]
                                     (if (nil? from)
                                       (proxy-super fromBytes bytes schema lt)
                                       (from bytes schema lt))))
      (toBytes [data schema lt] (let [to (:to bytes-fns)]
                                         (if (nil? to)
                                           (proxy-super toBytes data schema lt)
                                           (to data schema lt))))
      (fromFixed [fixed schema lt] (let [from (:from fixed-fns)]
                                     (if (nil? from)
                                       (proxy-super fromFixed fixed schema lt)
                                       (from fixed schema lt))))
      (toFixed [data schema lt] (let [to (:to fixed-fns)]
                                  (if (nil? to)
                                    (proxy-super toFixed data schema lt)
                                    (to data schema lt)))))))

(defn coerce [[logical-type conversion]]
  (let [logical-type-name (name logical-type)]
    (if (conversion? conversion)
      (java-conversion logical-type-name conversion)
      (clojure-conversion logical-type-name conversion))))

(def date-conversion
  {:class        LocalDate
   :int          {:from (fn [^Integer day _ _] (LocalDate/ofEpochDay day))
                  :to   (fn [^LocalDate day _ _] (Math/toIntExact (.toEpochDay day)))}})

(def time-conversion
  {:class        LocalTime
   :int          {:from (fn [^Integer time _ _] (.plus LocalTime/MIDNIGHT (long time) ChronoUnit/MILLIS))
                  :to   (fn [^LocalTime time _ _] (.get time ChronoField/MILLI_OF_DAY))}})

(def timestamp-conversion
  {:class        Instant
   :long         {:from (fn [^Long millis _ _] (Instant/ofEpochMilli millis))
                  :to   (fn [^Instant instant _ _] (.toEpochMilli instant))}})

(def uuid-conversion (Conversions$UUIDConversion.))

(def ^Conversions$DecimalConversion decimal-conversion (Conversions$DecimalConversion.))

(defn valid-rounding-modes []
  ;; Get all Rounding Modes from the ENUM and put them into a map {:mode-name RoundingMode}
  (->> (seq (.getFields RoundingMode))
       (map (fn [^Field f]
              (when (.isAssignableFrom RoundingMode (.getType f))
                [(-> f
                     (.getName)
                     (.replace \_ \-)
                     (.toLowerCase))
                 (.get f nil)])))
       (keep identity)
       (into {})))

(defn decimal-conversion-rounded [rounding-mode]
  (let [^APersistentMap valid-modes         (valid-rounding-modes)
        ^RoundingMode mode                  (.get valid-modes (name rounding-mode))
        _                                   (assert (not (nil? mode)) (str "Invalid rounding mode (" rounding-mode ") passed. Must be one of: " (keys valid-modes)))]
    {:class BigDecimal
     :bytes {:from (fn [^ByteBuffer bytes ^Schema schema ^LogicalType logicalType] (.fromBytes decimal-conversion bytes schema logicalType))
             :to   (fn [^BigDecimal decimal ^Schema schema ^LogicalTypes$Decimal logicalType]
                     (let [scaled (.setScale decimal (.getScale logicalType) mode)]
                       (.toBytes decimal-conversion scaled schema logicalType)))}
     :fixed {:from (fn [^GenericFixed bytes ^Schema schema ^LogicalType logicalType] (.fromFixed decimal-conversion bytes schema logicalType))
             :to   (fn [^BigDecimal decimal ^Schema schema ^LogicalTypes$Decimal logicalType]
                     (let [scaled (.setScale decimal (.getScale logicalType) mode)]
                       (.toFixed decimal-conversion scaled schema logicalType)))}}))

(def keyword-conversion
  {:class        Keyword
   :string       {:from (fn [^String name _ _] (keyword name))
                  :to   (fn [^Keyword keyword _ _] (name keyword))}})

(def default-conversions
  {:date              date-conversion
   :time-millis       time-conversion
   :timestamp-millis  timestamp-conversion
   :uuid              uuid-conversion
   :decimal           decimal-conversion
   :keyword           keyword-conversion})

;; TODO allow custom logical types to be registered using a global dynamic?
;; Register the custom "keyword" logical type
(LogicalTypes/register KeywordLogicalTypeFactory/TYPE (KeywordLogicalTypeFactory.))
