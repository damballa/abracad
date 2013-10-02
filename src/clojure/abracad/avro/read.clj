(ns abracad.avro.read
  "Generic data reading implementation."
  {:private true}
  (:require [abracad.avro :as avro]
            [abracad.avro.util
             :refer [mangle unmangle field-keyword if-not-let]])
  (:import [org.apache.avro Schema Schema$Field]
           [org.apache.avro.io Decoder ResolvingDecoder]
           [abracad.avro ClojureDatumReader]))

(defn schema-symbol
  [^Schema schema]
  (let [ns (.getNamespace schema), n (-> schema .getName unmangle)]
    (if ns (symbol (unmangle ns) n) (symbol n))))

(defn record-plain
  "Record as plain decoded data structures, with :type metadata
indicating schema name."
  [rname record]
  (with-meta record {:type rname}))

(defn record-reader
  "Generate wrapper for provided Avro reader function."
  [f] (fn [_ record] (apply f record)))

(defn reader-fn
  "Return tuple of `(named?, readerf)` for provided Avro `schema` and
schema name symbol `rname`."
  [^Schema schema rname]
  (if-let [f (get avro/*avro-readers* rname)]
    [false (record-reader f)]
    (if-not-let [reader (.getProp schema "abracad.reader")]
      [true record-plain]
      (case reader
        "vector" [false record-plain]
        #_else   (throw (ex-info "unknown `abracad.reader`"
                                 {:reader reader}))))))

(defn read-record
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [rname (schema-symbol expected)
        [named? readerf] (reader-fn expected rname)
        [reducef record] (if named?
                           [(fn [m ^Schema$Field f]
                              (assoc! m
                                (field-keyword f)
                                (.read reader nil (.schema f) in)))
                            (transient {})]
                           [(fn [v ^Schema$Field f]
                              (conj! v (.read reader nil (.schema f) in)))
                            (transient [])])
        record (->> in .readFieldOrder (reduce reducef record) persistent!)]
    (readerf rname record)))

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
