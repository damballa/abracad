(ns abracad.avro.write
  "Generic data writing implementation."
  {:private true}
  (:require [abracad.avro :as avro]
            [abracad.avro.edn :as edn]
            [abracad.avro.util :refer [case-expr case-enum mangle unmangle
                                       field-keyword]])
  (:import [java.util Collection Map List]
           [java.nio ByteBuffer]
           [clojure.lang Named Sequential IRecord]
           [org.apache.avro Schema Schema$Field Schema$Type AvroTypeException]
           [org.apache.avro.io Encoder]
           [abracad.avro ClojureDatumWriter]))

(def ^:const edn-element
  "abracad.avro.edn.Element")

(def ^:const edn-meta
  "abracad.avro.edn.Meta")

(defn element-schema?
  [^Schema schema]
  (= edn-element (.getFullName schema)))

(defn meta-schema?
  [^Schema schema]
  (= edn-meta (.getFullName schema)))

(defn element-union?
  [^Schema schema]
  (let [^Schema schema (-> schema .getTypes first)]
    (= edn-meta (.getFullName schema))))

(defn edn-schema?
  [^Schema schema]
  (= "abracad.avro.edn" (.getNamespace schema)))

(defn namespaced?
  [x] (instance? Named x))

(defn named?
  [x] (or (symbol? x) (keyword? x) (string? x)))

(def ^:const bytes-class
  (Class/forName "[B"))

(defprotocol HandleBytes
  (count-bytes [this])
  (emit-bytes [this encoder])
  (emit-fixed [this encoder]))

(extend-type (Class/forName "[B")
  HandleBytes
  (count-bytes [bytes] (alength ^bytes bytes))
  (emit-bytes [bytes encoder]
    (.writeBytes ^Encoder encoder ^bytes bytes))
  (emit-fixed [bytes encoder]
    (.writeFixed ^Encoder encoder ^bytes bytes)))

(extend-type ByteBuffer
  HandleBytes
  (count-bytes [^ByteBuffer bytes] (.remaining bytes))
  (emit-bytes [^ByteBuffer bytes ^Encoder encoder]
    (.writeBytes encoder bytes))
  (emit-fixed [^ByteBuffer bytes ^Encoder encoder]
    (.writeFixed encoder bytes)))

(defn elide
  [^Schema schema]
  (.schema ^Schema$Field (first (.getFields schema))))

(defn write-record
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (case-expr (.getFullName schema)
    edn-element (.write writer (elide schema) datum out)
    edn-meta (let [schema (elide schema)]
               (.write writer schema (with-meta datum nil) out)
               (.write writer schema (meta datum) out))
    #_else (doseq [:let [field-get (if (edn-schema? schema)
                                     edn/field-get
                                     avro/field-get)]
                   ^Schema$Field f (.getFields schema)
                   :let [key (field-keyword f),
                         val (field-get datum key)]]
             (.write writer (.schema f) val out))))

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
    (if (->> this keys (every? string?))
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
               (avro/field-list datum))))

(defn avro-enum?
  [^Schema schema datum]
  (and (named? datum) (.hasEnumSymbol schema (-> datum name mangle))))

(defn avro-bytes?
  [^Schema schema datum]
  (or (instance? bytes-class datum)
      (instance? ByteBuffer datum)))

(defn avro-fixed?
  [^Schema schema datum]
  (and (avro-bytes? schema datum)
       (= (.getFixedSize schema) (count-bytes datum))))

(defn schema-match?
  [^Schema schema datum]
  (case-enum (.getType schema)
    Schema$Type/RECORD  (avro-record? schema datum)
    Schema$Type/ENUM    (avro-enum? schema datum)
    Schema$Type/FIXED   (avro-fixed? schema datum)
    Schema$Type/BYTES   (avro-bytes? schema datum)
    Schema$Type/LONG    (integer? datum)
    Schema$Type/INT     (integer? datum)
    Schema$Type/DOUBLE  (float? datum)
    Schema$Type/FLOAT   (float? datum)
    #_ else             false))

(defn resolve-union*
  [^Schema schema ^Object datum]
  (let [n (if (element-union? schema)
            (edn/schema-name datum)
            (avro/schema-name datum))]
    (if-let [index (and n (.getIndexNamed schema n))]
      index
      (->> (vec (.getTypes schema))
           (reduce-kv (fn [_ i schema]
                        (when (schema-match? schema datum)
                          (reduced i)))
                      nil)))))

(defn resolve-union
  [^ClojureDatumWriter writer ^Schema schema ^Object datum]
  (resolve-union* schema datum))

(defn write-bytes
  [^ClojureDatumWriter writer datum ^Encoder out]
  (emit-bytes datum out))

(defn write-fixed
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (when (not= (.getFixedSize schema) (count-bytes datum))
    (throw (AvroTypeException. (str "Not a" schema ": " datum))))
  (emit-fixed datum out))
