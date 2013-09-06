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

(defn reader-fn
  [^Schema schema rname]
  (or (get avro/*avro-readers* rname)
      (if-let [reader (.getProp schema "abracad.reader")]
        (case reader
          nil      nil
          "vector" vector
          #_else   (-> reader symbol resolve)))))

(defn read-record
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [rname (schema-symbol expected)
        readerf (reader-fn expected rname)
        [reducef record] (if readerf
                           [(fn [v ^Schema$Field f]
                              (conj! v (.read reader nil (.schema f) in)))
                            (transient [])]
                           [(fn [m ^Schema$Field f]
                              (assoc! m
                                (field-keyword f)
                                (.read reader nil (.schema f) in)))
                            (transient {})])
        record (->> in .readFieldOrder (reduce reducef record) persistent!)]
    (if readerf
      (apply readerf record)
      (with-meta record {:type rname}))))

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
         (let [m (conj! m (.read reader nil vtype in)), n (dec n)]
           (if-not (pos? n)
             (let [n (.arrayNext in)] (if-not (pos? n) m (recur m n)))
             (recur m n))))))))

(defn read-map
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [vtype (.getValueType expected), n (.readMapStart in)]
    (if-not (pos? n)
      {}
      (persistent!
       (loop [m (transient {}), n (long n)]
         (let [k (.readString in), v (.read reader nil vtype in)
               m (assoc! m k v), n (dec n)]
           (if-not (pos? n)
             (let [n (.mapNext in)] (if-not (pos? n) m (recur m n)))
             (recur m n))))))))

(defn read-fixed
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (let [size (.getFixedSize expected), bytes (byte-array size)]
    (.readFixed in bytes 0 size)
    bytes))

(defn read-bytes
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (.array (.readBytes in nil)))
