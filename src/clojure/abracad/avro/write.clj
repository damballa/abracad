(ns abracad.avro.write
  (:require [abracad.avro.util :refer [case-enum mangle unmangle field-keyword]]
            [abracad.avro.mapping :as avrom])
  (:import [java.util Collection Map List]
           [java.nio ByteBuffer]
           [clojure.lang Named]
           [org.apache.avro Schema Schema$Field Schema$Type]
           [org.apache.avro.io Encoder]
           [abracad.avro ClojureDatumWriter]))

(defn namespaced?
  [x] (instance? Named x))

(defn named?
  [x] (or (symbol? x) (keyword? x) (string? x)))

(defn full-name
  [x]
  (let [ns (and (namespaced? x) (-> x namespace mangle))
        n (-> x name mangle)]
    (if ns (str ns "." n) n)))

(def ^:const bytes-class
  (Class/forName "[B"))

(defn write-record
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (doseq [f (.getFields schema)
          :let [key (field-keyword f),
                val (avrom/field-get datum key)]]
    (.write writer (.schema f) val out)))

(defn write-enum
  [^ClojureDatumWriter writer ^Schema schema ^Object datum ^Encoder out]
  (.writeEnum out (.getEnumOrdinal schema (-> datum name mangle))))

(defn enum-symbol?
  [^Schema schema datum]
  (and (named? datum) (.hasEnumSymbol schema (-> datum name mangle))))

(defn fixed-bytes?
  [^Schema schema datum]
  (and (instance? bytes-class datum)
       (= (.getFixedSize schema) (count datum))))

(defn schema-match?
  [^Schema schema datum]
  (case-enum (.getType schema)
    Schema$Type/RECORD  (and (map? datum) (-> datum keys first keyword?))
    Schema$Type/ENUM    (enum-symbol? schema datum)
    Schema$Type/ARRAY   (or (seq? datum) (instance? Collection datum))
    Schema$Type/MAP     (and (map? datum) (-> datum keys first string?))
    Schema$Type/UNION   false ;; Recursive resolution?
    Schema$Type/FIXED   (fixed-bytes? schema datum)
    Schema$Type/STRING  (instance? CharSequence datum)
    Schema$Type/BYTES   (instance? ByteBuffer datum)
    Schema$Type/INT     (instance? Integer datum)
    Schema$Type/LONG    (instance? Long datum)
    Schema$Type/FLOAT   (instance? Float datum)
    Schema$Type/DOUBLE  (instance? Double datum)
    Schema$Type/BOOLEAN (instance? Boolean datum)
    Schema$Type/NULL    (nil? datum)))

(defn resolve-union
  [^ClojureDatumWriter writer ^Schema schema ^Object datum]
  (let [t (type datum)
        n (cond (nil? t) "null"
                (named? t) (full-name t)
                (instance? Class t) (.getName t))]
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
