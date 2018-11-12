(ns abracad.avro.conversion
  "Logical Type converter implementations"
  (:import (java.time LocalDate LocalTime Instant)
           (org.apache.avro Conversions$UUIDConversion Conversions$DecimalConversion Conversion LogicalTypes)
           (java.time.temporal ChronoField ChronoUnit)
           (clojure.lang Keyword)
           (abracad.avro KeywordLogicalTypeFactory)))

(defn conversion? [x]
  (instance? Conversion x))

(defn java-conversion [logical-type conversion]
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
        string-fns  (:string conversion)]
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
                                           (to data schema lt)))))))

(defn coerce [[logical-type conversion]]
  (let [logical-type-name (name logical-type)]
    (if (conversion? conversion)
      (java-conversion logical-type-name conversion)
      (clojure-conversion logical-type-name conversion))))

(def date-conversion
  {:class        LocalDate
   :int          {:from (fn [day ^Integer _ _] (LocalDate/ofEpochDay day))
                  :to   (fn [day ^LocalDate _ _] (Math/toIntExact (.toEpochDay day)))}})

(def time-conversion
  {:class        LocalTime
   :int          {:from (fn [time ^Integer _ _] (.plus LocalTime/MIDNIGHT (long time) ChronoUnit/MILLIS))
                  :to   (fn [time ^LocalTime _ _] (.get time ChronoField/MILLI_OF_DAY))}})

(def timestamp-conversion
  {:class        Instant
   :long         {:from (fn [millis ^Long _ _] (Instant/ofEpochMilli millis))
                  :to   (fn [instant ^Instant _ _] (.toEpochMilli instant))}})

(def uuid-conversion (Conversions$UUIDConversion.))

(def decimal-conversion (Conversions$DecimalConversion.))
;; TODO conversion for decimal that rounds and sets scale before conversion

(def keyword-conversion
  {:class        Keyword
   :string          {:from (fn [name ^String _ _] (keyword name))
                     :to   (fn [keyword ^Keyword _ _] (name keyword))}})

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
