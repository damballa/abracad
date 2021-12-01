(ns abracad.avro.write
  "Generic data writing implementation."
  {:private true}
  (:require
    [abracad.avro :as avro]
    [abracad.avro.edn :as edn]
    [abracad.avro.util :refer [case-expr case-enum mangle field-keyword]])
  (:import
    (abracad.avro
      ArrayAccessor
      ClojureDatumWriter)
    (clojure.lang
      IRecord
      Indexed
      Named
      Sequential)
    (java.nio
      ByteBuffer)
    (java.util
      Collection
      Map)
    (org.apache.avro
      AvroTypeException
      Schema
      Schema$Field
      Schema$Type)
    (org.apache.avro.generic
      GenericRecord)
    (org.apache.avro.io
      Encoder)))


(def ^:const edn-element
  "abracad.avro.edn.Element")


(def ^:const edn-meta
  "abracad.avro.edn.Meta")


(def ^:dynamic *unchecked*
  "When `true`, do not perform schema field validation checks during
record serialization."
  false)


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
  [x]
  (instance? Named x))


(defn named?
  [x]
  (or (symbol? x) (keyword? x) (string? x)))


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


(defn schema-error!
  [^Schema schema datum]
  (throw (ex-info "Cannot write datum as schema"
                  {:datum datum, :schema (.getFullName schema)})))


(defn wr-named
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (let [field-get (if (edn-schema? schema) edn/field-get avro/field-get)]
    (doseq [^Schema$Field f (.getFields schema)
            :let [key (field-keyword f), val (field-get datum key)]]
      (.write writer (.schema f) val out))))


(defn wr-named-checked
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (let [fields (into #{} (map field-keyword (.getFields schema)))]
    (when (not-every? fields (avro/field-list datum))
      (schema-error! schema datum))
    (doseq [^Schema$Field f (.getFields schema)
            :let [key (field-keyword f), val (avro/field-get datum key)]]
      (.write writer (.schema f) val out))))


(defn wr-positional
  [^ClojureDatumWriter writer ^Schema schema datum ^Encoder out]
  (let [fields (.getFields schema), nfields (count fields)]
    (when (not= nfields (count datum))
      (schema-error! schema datum))
    (dorun
      (map (fn [^Schema$Field f val] (.write writer (.schema f) val out))
           fields datum))))


(defn elide
  [^Schema schema]
  (.schema ^Schema$Field (first (.getFields schema))))


(defn schema-equal?
  [^Schema schema datum]
  (let [sname (.getFullName schema)]
    (or (and (edn-schema? schema) (= sname (edn/schema-name datum)))
        (= sname (avro/schema-name datum)))))


(defn write-record*
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (case-expr (.getFullName schema)
             edn-element (.write writer (elide schema) datum out)
             edn-meta (let [schema (elide schema)]
                        (.write writer schema (with-meta datum nil) out)
                        (.write writer schema (meta datum) out))
             #_else (let [wrf (cond (schema-equal? schema datum) wr-named
                                    (instance? Indexed datum) wr-positional
                                    *unchecked* wr-named
                                    :else wr-named-checked)]
                      (wrf writer schema datum out))))


(defn write-record
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (let [unchecked (-> datum meta (:avro/unchecked *unchecked*))]
    (if (= unchecked *unchecked*)
      (write-record* writer schema datum out)
      (binding [*unchecked* unchecked]
        (write-record* writer schema datum out)))))


(defn write-enum
  [_ ^Schema schema ^Object datum ^Encoder out]
  (.writeEnum out (.getEnumOrdinal schema (-> datum name mangle))))


(defn array-prim?
  [datum]
  (when-let [cls (class datum)]
    (and (-> cls .isArray)
         (-> cls .getComponentType .isPrimitive))))


(defn write-array-seq
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (.setItemCount out (count datum))
  (doseq [datum datum]
    (.startItem out)
    (.write writer schema datum out)))


(defn write-array
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (let [schema (.getElementType schema)]
    (.writeArrayStart out)
    (if (array-prim? datum)
      (ArrayAccessor/writeArray datum out)
      (write-array-seq writer schema datum out))
    (.writeArrayEnd out)))


(defn schema-name-type
  [datum]
  (let [t (type datum)]
    (cond (string? t) t
          (instance? Named t)
          , (let [ns (namespace t)
                  ns (if ns (mangle ns))
                  n (-> t name mangle)]
              (if ns (str ns "." n) n))
          (class? t) (.getName ^Class t))))


(defn field-name
  [^Schema$Field field]
  (keyword (.name field)))


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

  GenericRecord
  (schema-name [this] (-> this .getSchema .getFullName))
  (field-get [this field] (.get this (name field)))
  (field-list [this] (->> this .getSchema .getFields (map field-name) set))

  Object
  (schema-name [this] (schema-name-type this))
  (field-get [this field] (get this field))
  (field-list [this] (keys this)))


(defn avro-record?
  [^Schema schema datum]
  (or (and (vector? datum)
           (= (count datum) (-> schema .getFields count)))
      (and (map? datum)
           (every? (->> schema .getFields (map field-keyword) set)
                   (avro/field-list datum)))))


(defn avro-enum?
  [^Schema schema datum]
  (and (named? datum) (.hasEnumSymbol schema (-> datum name mangle))))


(defn avro-bytes?
  [_ datum]
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
             Schema$Type/MAP     (map? datum)
             #_ else             false))


(defn resolve-union*
  [^Schema schema ^Object datum]
  (let [n (if (element-union? schema)
            (edn/schema-name datum)
            (avro/schema-name datum))]
    (if-let [index (and n (.getIndexNamed schema n))]
      index
      (loop [schemas (.getTypes schema), i (long 0)]
        (if-let [[schema & schemas] (seq schemas)]
          (if (schema-match? schema datum)
            i
            (recur schemas (inc i))))))))


(defn resolve-union
  [_ ^Schema schema ^Object datum]
  (resolve-union* schema datum))


(defn write-bytes
  [_ datum ^Encoder out]
  (emit-bytes datum out))


(defn write-fixed
  [_ ^Schema schema datum ^Encoder out]
  (when (not= (.getFixedSize schema) (count-bytes datum))
    (throw (AvroTypeException. (str "Not a" schema ": " datum))))
  (emit-fixed datum out))
