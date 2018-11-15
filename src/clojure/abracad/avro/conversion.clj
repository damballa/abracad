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
      (str "Java Logical Type Conversions must be keyed against the logical type they can handle. Key " logical-type
           " was used instead of " expected-type " for " conversion)))
  conversion)

;; Like proxy-super but allows using a type hint to help reflection to avoid compiler warnings of "call to method toInt can't be resolved (target class is unknown)."
(defmacro proxy-super-with-class [class meth & args]
  (let [thissym (with-meta (gensym) {:tag class})]
    `(let [~thissym ~'this]
       (proxy-call-with-super (fn [] (. ~thissym ~meth ~@args)) ~thissym ~(name meth)))))

;; from/to not implemented yet for arrays, maps or records. Only add these if requested. Possible array use case: rational/complex numbers?
(defn clojure-conversion [logical-type conversion]
  (let [{conversion-class :class
         int-fns          :int
         long-fns         :long
         string-fns       :string
         bytes-fns        :bytes
         fixed-fns        :fixed
         boolean-fns      :boolean
         float-fns        :float
         double-fns       :double
         enum-fns         :enum} conversion]
    (proxy [Conversion] []
      (getConvertedType [] conversion-class)
      (getLogicalTypeName []  logical-type)
      (fromInt [int schema lt] (let [from (:from int-fns)]
                                 (if (nil? from)
                                   (proxy-super-with-class Conversion fromInt int schema lt)
                                   (from int schema lt))))
      (toInt [data schema lt] (let [to (:to int-fns)]
                                (if (nil? to)
                                  (proxy-super-with-class Conversion toInt data schema lt)
                                  (to data schema lt))))
      (fromLong [long schema lt] (let [from (:from long-fns)]
                                   (if (nil? from)
                                     (proxy-super-with-class Conversion fromLong long schema lt)
                                     (from long schema lt))))
      (toLong [data schema lt] (let [to (:to long-fns)]
                                 (if (nil? to)
                                   (proxy-super-with-class Conversion toLong data schema lt)
                                   (to data schema lt))))
      (fromCharSequence [string schema lt] (let [from (:from string-fns)]
                                             (if (nil? from)
                                               (proxy-super-with-class Conversion fromCharSequence string schema lt)
                                               (from string schema lt))))
      (toCharSequence [data schema lt] (let [to (:to string-fns)]
                                         (if (nil? to)
                                           (proxy-super-with-class Conversion toCharSequence data schema lt)
                                           (to data schema lt))))
      (fromBytes [bytes schema lt] (let [from (:from bytes-fns)]
                                     (if (nil? from)
                                       (proxy-super-with-class Conversion fromBytes bytes schema lt)
                                       (from bytes schema lt))))
      (toBytes [data schema lt] (let [to (:to bytes-fns)]
                                         (if (nil? to)
                                           (proxy-super-with-class Conversion toBytes data schema lt)
                                           (to data schema lt))))
      (fromFixed [fixed schema lt] (let [from (:from fixed-fns)]
                                     (if (nil? from)
                                       (proxy-super-with-class Conversion fromFixed fixed schema lt)
                                       (from fixed schema lt))))
      (toFixed [data schema lt] (let [to (:to fixed-fns)]
                                  (if (nil? to)
                                    (proxy-super-with-class Conversion toFixed data schema lt)
                                    (to data schema lt))))
      (fromBoolean [boolean schema lt] (let [from (:from boolean-fns)]
                                         (if (nil? from)
                                           (proxy-super-with-class Conversion fromBoolean boolean schema lt)
                                           (from boolean schema lt))))
      (toBoolean [data schema lt] (let [to (:to boolean-fns)]
                                    (if (nil? to)
                                      (proxy-super-with-class Conversion toBoolean data schema lt)
                                      (to data schema lt))))
      (fromFloat [float schema lt] (let [from (:from float-fns)]
                                     (if (nil? from)
                                       (proxy-super-with-class Conversion fromFloat float schema lt)
                                       (from float schema lt))))
      (toFloat [data schema lt] (let [to (:to float-fns)]
                                  (if (nil? to)
                                    (proxy-super-with-class Conversion toFloat data schema lt)
                                    (to data schema lt))))
      (fromDouble [double schema lt] (let [from (:from double-fns)]
                                       (if (nil? from)
                                         (proxy-super-with-class Conversion fromDouble double schema lt)
                                         (from double schema lt))))
      (toDouble [data schema lt] (let [to (:to double-fns)]
                                   (if (nil? to)
                                     (proxy-super-with-class Conversion toDouble data schema lt)
                                     (to data schema lt))))
      (fromEnumSymbol [enum-symbol schema lt] (let [from (:from enum-fns)]
                                                (if (nil? from)
                                                  (proxy-super-with-class Conversion fromEnumSymbol enum-symbol schema lt)
                                                  (from enum-symbol schema lt))))
      (toEnumSymbol [data schema lt] (let [to (:to enum-fns)]
                                       (if (nil? to)
                                         (proxy-super-with-class Conversion toEnumSymbol data schema lt)
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
        _                                   (assert (not (nil? mode)) (str "Invalid rounding mode (" (name rounding-mode) ") passed. Must be one of: " (keys valid-modes)))]
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

;; Register the custom "keyword" logical type
(LogicalTypes/register KeywordLogicalTypeFactory/TYPE (KeywordLogicalTypeFactory.))
