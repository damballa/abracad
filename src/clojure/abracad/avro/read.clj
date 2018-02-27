(ns abracad.avro.read
  "Generic data reading implementation."
  {:private true}
  (:require [abracad.avro :as avro]
            [abracad.avro.util :as util])
  (:import [abracad.avro ArrayAccessor ClojureDatumReader]
           [clojure.lang Var]
           [org.apache.avro Schema Schema$Field]
           [org.apache.avro.io Decoder ResolvingDecoder]))

(defn schema-symbol
  [^Schema schema]
  (let [ns (.getNamespace schema), n (-> schema .getName util/unmangle)]
    (if ns (symbol (util/unmangle ns) n) (symbol n))))

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
    (if-let [reader (.getProp schema "abracad.reader")]
      (case reader
        "vector" [false record-plain]
        #_else   (throw (ex-info "unknown `abracad.reader`"
                                 {:reader reader})))
      [true record-plain])))

(defn read-record
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [rname (schema-symbol expected)
        [named? readerf] (reader-fn expected rname)
        [reducef record] (if named?
                           [(fn [m ^Schema$Field f]
                              (assoc! m
                                (util/field-keyword f)
                                (.read reader nil (.schema f) in)))
                            (transient {})]
                           [(fn [v ^Schema$Field f]
                              (conj! v (.read reader nil (.schema f) in)))
                            (transient [])])
        record (->> in .readFieldOrder (reduce reducef record) persistent!)]
    (readerf rname record)))

(defn read-enum
  [^ClojureDatumReader reader ^Schema expected ^Decoder in]
  (-> expected .getEnumSymbols (.get (.readEnum in)) util/unmangle keyword))

(defn read-array-vector
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in ^long n]
  (if-not (pos? n)
    []
    (persistent!
     (loop [m (transient []), n (long n)]
       (let [m (conj! m (.read reader nil expected in)), n (dec n)]
         (if-not (pos? n)
           (let [n (.arrayNext in)] (if-not (pos? n) m (recur m n)))
           (recur m n)))))))

(defn read-array
  [^ClojureDatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [etype (.getElementType expected)
        atype (.getProp expected "abracad.array")
        n (.readArrayStart in)]
    (if (or (nil? atype) (= atype "vector"))
      (read-array-vector reader etype in n)
      (case atype
        "booleans" (ArrayAccessor/readArray ^booleans (boolean-array n) n in)
        "shorts" (ArrayAccessor/readArray ^shorts (short-array n) n in)
        "chars" (ArrayAccessor/readArray ^chars (char-array n) n in)
        "ints" (ArrayAccessor/readArray ^ints (int-array n) n in)
        "longs" (ArrayAccessor/readArray ^longs (long-array n) n in)
        "floats" (ArrayAccessor/readArray ^floats (float-array n) n in)
        "doubles" (ArrayAccessor/readArray ^doubles (double-array n) n in)))))

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

;; Load namespaces in order to ensure Avro reader vars are available
(doseq [ns (->> (vals avro/*avro-readers*)
                (map #(-> ^Var % .-ns .-name))
                (distinct)
                (sort))]
  (require ns))
