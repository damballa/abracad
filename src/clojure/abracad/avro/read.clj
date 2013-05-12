(ns abracad.avro.read
  "Generic data reading implementation."
  {:private true}
  (:require [abracad.avro.util :refer [mangle unmangle field-keyword]]
            [abracad.avro :as avro])
  (:import [org.apache.avro Schema Schema$Field]
           [org.apache.avro.io Decoder ResolvingDecoder]
           [abracad.avro ClojureDatumReader]))

(defn schema-symbol
  [^Schema schema]
  (let [ns (.getNamespace schema), n (-> schema .getName unmangle)]
    (if ns (symbol (unmangle ns) n) (symbol n))))

(defn read-record
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [rname (schema-symbol expected)
        rmap (with-meta
               (persistent!
                (reduce (fn [m ^Schema$Field f]
                          (assoc! m
                                  (field-keyword f)
                                  (.read reader nil (.schema f) in)))
                        (transient {})
                        (.readFieldOrder in)))
               {:type rname})]
    (if-let [avro-reader (get avro/*avro-readers* rname)]
      (avro-reader rmap)
      rmap)))

(defn read-enum
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (-> expected .getEnumSymbols (.get (.readEnum in)) unmangle keyword))

(defn read-array
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [vtype (.getElementType expected), n (.readArrayStart in)]
    (if-not (pos? n)
      []
      (persistent!
       (loop [m (transient []), n (long n)]
         (if-not (pos? n)
           (let [n (.arrayNext in)] (if-not (pos? n) m (recur m n)))
           (let [v (.read reader nil vtype in)]
             (recur (conj! m v) (dec n)))))))))

(defn read-map
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [vtype (.getValueType expected), n (.readMapStart in)]
    (if-not (pos? n)
      {}
      (persistent!
       (loop [m (transient {}), n (long n)]
         (if-not (pos? n)
           (let [n (.mapNext in)] (if-not (pos? n) m (recur m n)))
           (let [k (.readString in), v (.read reader nil vtype in)]
             (recur (assoc! m k v) (dec n)))))))))

(defn read-fixed
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (let [size (.getFixedSize expected), bytes (byte-array size)]
    (.readFixed in bytes 0 size)
    bytes))

(defn read-bytes
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (.readBytes in))
