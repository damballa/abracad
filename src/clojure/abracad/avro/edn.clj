(ns abracad.avro.edn
  (:require [abracad.avro :as avro])
  (:import [clojure.lang BigInt Cons IMeta IPersistentList IPersistentMap
             IPersistentSet IPersistentVector ISeq Keyword PersistentArrayMap
             PersistentQueue Ratio Sorted Symbol]
           [org.apache.avro Schema]))

(defprotocol EDNAvroSerializable
  "Protocol for customizing EDN-in-Avro serialization."
  (-schema-name [this]
    "Full package-/namespace-qualified name for EDN-in-Avro purposes.")
  (field-get [this field]
    "Value of keyword `field` for EDN-in-Avro serialization of object.")
  (field-list [this]
    "List of keyword EDN-in-Avro fields this object provides."))

(defn schema-name
  "EDN-in-Avro schema name of `obj`."
  [obj]
  (if (meta obj)
    "abracad.avro.edn.Meta"
    (-schema-name obj)))

(extend-protocol EDNAvroSerializable
  nil (-schema-name [_] "null")

  Character
  (-schema-name [_] "abracad.avro.edn.Character")
  (field-get [this _] (int this))
  (field-list [_] #{:value})

  Symbol
  (-schema-name [_] "abracad.avro.edn.Symbol")
  (field-get [this field]
    (case field
      :namespace (namespace this)
      :name (name this)))
  (field-list [_] #{:namespace :name})

  Keyword
  (-schema-name [_] "abracad.avro.edn.Keyword")
  (field-get [this field]
    (case field
      :namespace (namespace this)
      :name (name this)))
  (field-list [_] #{:namespace :name})

  Byte
  (-schema-name [_] "abracad.avro.edn.Byte")
  (field-get [this _] (int this))
  (field-list [_] #{:value})

  Short
  (-schema-name [_] "abracad.avro.edn.Short")
  (field-get [this _] (int this))
  (field-list [_] #{:value})

  BigInt
  (-schema-name [_] "abracad.avro.edn.BigInt")
  (field-get [this _] (.toByteArray (.toBigInteger this)))
  (field-list [_] #{:value})

  BigInteger
  (-schema-name [_] "abracad.avro.edn.BigInteger")
  (field-get [this _] (.toByteArray this))
  (field-list [_] #{:value})

  BigDecimal
  (-schema-name [_] "abracad.avro.edn.BigDecimal")
  (field-get [this field]
    (case field
      :value (.unscaledValue this)
      :scale (.scale this)))
  (field-list [this] #{:value :scale})

  IPersistentList
  (-schema-name [_] "abracad.avro.edn.List")
  (field-get [this _] this)
  (field-list [this] #{:values})

  ISeq
  (-schema-name [_] "abracad.avro.edn.List")
  (field-get [this _] this)
  (field-list [this] #{:values})

  IPersistentVector
  (-schema-name [_] "abracad.avro.edn.Vector")
  (field-get [this _] this)
  (field-list [this] #{:values})

  IPersistentMap
  (-schema-name [this]
    (if (instance? Sorted this)
      "abracad.avro.edn.SortedMap"
      "abracad.avro.edn.Map"))
  (field-get [this _] (apply concat this))
  (field-list [_] #{:keyvals})

  IPersistentSet
  (-schema-name [this]
    (if (instance? Sorted this)
      "abracad.avro.edn.SortedSet"
      "abracad.avro.edn.Set"))
  (field-get [this _] this)
  (field-list [_] #{:values})

  PersistentQueue
  (-schema-name [_] "abracad.avro.edn.Queue")
  (field-get [this _] this)
  (field-list [this] #{:values})

  Ratio
  (-schema-name [_] "abracad.avro.edn.Ratio")
  (field-get [this field]
    (case field
      :numerator (.-numerator this)
      :denominator (.-denominator this)))
  (field-list [this] #{:numerator :denominator})

  Object
  (-schema-name [this] (avro/schema-name this))
  (field-get [this field] (avro/field-get this field))
  (field-list [this] (avro/field-list this)))

(defn ^:private edn-list*
  "List-equivalent of `args`."
  [args] (if (empty? args) () (seq args)))

(defn ^:private hash-map*
  "Unsorted map from interleaved key-values in `args`."
  [args] (apply hash-map args))

(defn ^:private sorted-map*
  "Sorted map from interleaved key-values in `args`."
  [args] (apply sorted-map args))

(defn ^:private hash-set*
  "Unsorted set from values in `args`."
  [args] (apply hash-set args))

(defn ^:private sorted-set*
  "Sorted set from values in `args`."
  [args] (apply sorted-set args))

(defn ^:private bigdecimal
  "BigDecimal constructor function matching EDN-in-Avro serialization."
  [value scale] (BigDecimal. value scale))

(defn ^:private queue*
  "Queue from values in `args`."
  [args] (into PersistentQueue/EMPTY args))

(defn ^:private ratio
  "Ratio constructor function matching EDN-in-Avro serialization."
  [n d] (Ratio. n d))

(def ^:private base-elements
  "Schemas for base EDN element types."
  [;; Core EDN element types, preserving extra run-time type
   ;; information where potentially desirable
   "null"
   "boolean"
   "string"
   {:type "record", :name "Character"
    :fields [{:name "value", :type "int"}]}
   {:type "record", :name "Symbol"
    :fields [{:name "namespace", :type ["null", "string"]}
             {:name "name", :type "string"}]}
   {:type "record", :name "Keyword"
    :fields [{:name "namespace", :type ["null", "string"]}
             {:name "name", :type "string"}]}
   {:type "record", :name "Byte"
    :fields [{:name "value", :type "int"}]}
   "int"
   {:type "record", :name "Short"
    :fields [{:name "value", :type "int"}]}
   "long"
   {:type "record", :name "BigInt"
    :fields [{:name "value", :type "bytes"}]}
   {:type "record", :name "BigInteger"
    :fields [{:name "value", :type "bytes"}]}
   "float"
   "double"
   {:type "record", :name "BigDecimal"
    :fields [{:name "value", :type "BigInteger"}
             {:name "scale", :type "int"}]}
   {:type "record", :name "List"
    :fields [{:name "values",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "Vector"
    :fields [{:name "values",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "Map"
    :fields [{:name "keyvals",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "SortedMap"
    :fields [{:name "keyvals",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "Set"
    :fields [{:name "values",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "SortedSet"
    :fields [{:name "values",
              :type {:type "array",
                     :items "Element"}}]}
   ;;
   ;; Built-in tagged literals
   ;; [TODO]
   ;;
   ;; Extensions
   "bytes"
   {:type "record", :name "Queue"
    :fields [{:name "values",
              :type {:type "array",
                     :items "Element"}}]}
   {:type "record", :name "Ratio"
    :fields [{:name "numerator", :type "BigInteger"}
             {:name "denominator", :type "BigInteger"}]}
   ])

(defn new-schema
  "Return new EDN-in-Avro schema.  If provided, incorporate
`schemas` (which should be compatible with `avro/parse-schema`) as
additional allowed element types."
  ([] (new-schema nil))
  ([schemas]
     (let [schemas (map avro/parse-schema schemas)
           names (map #(.getFullName ^Schema %) schemas)]
       (apply avro/parse-schema
              `[~@schemas
                {:type "record", :name "abracad.avro.edn.Element",
                 :fields [{:name "value"
                           :type [{:type "record", :name "Meta"
                                   :fields [{:name "value", :type "Element"}
                                            {:name "meta", :type "Element"}]}
                                  ~@names
                                  ~@base-elements]}]}]))))
