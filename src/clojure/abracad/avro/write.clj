(ns abracad.avro.write
  "Generic data writing implementation."
  {:private true}
  (:require [abracad.avro.util :refer [case-enum mangle unmangle field-keyword]]
            [abracad.avro :as avro])
  (:import [java.util Collection Map List]
           [java.nio ByteBuffer]
           [clojure.lang Named Sequential IRecord]
           [org.apache.avro Schema Schema$Field Schema$Type]
           [org.apache.avro.io Encoder]
           [abracad.avro ClojureDatumWriter]))

(defn namespaced?
  [x] (instance? Named x))

(defn named?
  [x] (or (symbol? x) (keyword? x) (string? x)))

(def ^:const bytes-class
  (Class/forName "[B"))

(defn write-record
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (doseq [f (.getFields schema)
          :let [key (field-keyword f),
                val (avro/field-get datum key)]]
    (.write writer (.schema f) val out)))

(defn write-enum
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (.writeEnum out (.getEnumOrdinal schema (-> datum name mangle))))

(defn schema-name-type
  [datum]
  (let [t (type datum)]
    (cond (string? t) t
          (instance? Named t)
          , (let [ns (some-> t namespace mangle)
                  n (-> t name mangle)]
              (if ns (str ns "." n) n))
          (class? t) (.getName ^Class t))))

(extend-protocol avro/AvroSerializable
  nil (schema-name [_] "null")
  CharSequence (schema-name [_] "string")
  ByteBuffer (schema-name [_] "bytes")
  Integer (schema-name [_] "int")
  Long (schema-name [_] "long")
  Float (schema-name [_] "float")
  Double (schema-name [_] "double")
  Boolean (schema-name [_] "boolean")
  Collection (schema-name [_] "array")
  Sequential (schema-name [_] "array")

  Map
  (schema-name [this]
    (if (-> this keys first string?)
      "map"
      (schema-name-type this)))
  (field-get [this field] (get this field))
  (field-list [this] (keys this))

  IRecord
  (schema-name [this] (schema-name-type this))
  (field-get [this field] (get this field))
  (field-list [this] (keys this))

  Object
  (schema-name [this] (schema-name-type this))
  (field-get [this field] (get this field))
  (field-list [this] (keys this)))

(defn avro-record?
  [^Schema schema datum]
  (and (map? datum)
       (every? (->> (.getFields schema) (map field-keyword) set)
               (keys datum))))

(defn avro-enum?
  [^Schema schema datum]
  (and (named? datum) (.hasEnumSymbol schema (-> datum name mangle))))

(defn avro-fixed?
  [^Schema schema datum]
  (and (instance? bytes-class datum)
       (= (.getFixedSize schema) (count datum))))

(defn schema-match?
  [^Schema schema datum]
  (case-enum (.getType schema)
    Schema$Type/RECORD  (avro-record? schema datum)
    Schema$Type/ENUM    (avro-enum? schema datum)
    Schema$Type/FIXED   (avro-fixed? schema datum)
    #_ else             false))

(defn resolve-union
  [^ClojureDatumWriter writer ^Schema schema ^Object datum]
  (let [n (avro/schema-name datum)]
    (if-let [index (and n (.getIndexNamed schema n))]
      index
      (->> (vec (.getTypes schema))
           (reduce-kv (fn [_ i schema]
                        (when (schema-match? schema datum)
                          (reduced i)))
                      nil)))))

(defn write-fixed
  [^ClojureDatumWriter writer ^Schema schema ^bytes datum ^Encoder out]
  (.writeFixed out datum 0 (.getFixedSize schema)))
