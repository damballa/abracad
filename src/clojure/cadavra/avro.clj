(ns cadavra.avro
  (:import [org.apache.avro Schema]
           [org.apache.avro.io DatumReader Decoder ResolvingDecoder]))

(defn read-record
  [^DatumReader this ^Schema expected ^ResolvingDecoder in]
  (let [rname (keyword (.getNamespace expected) (.getName expected))]
    (with-meta
      (persistent!
       (reduce (fn [m f]
                 (assoc! m
                   (keyword (.name f))
                   (.read this nil (.schema f) in)))
               (transient {})
               (.readFieldOrder in)))
      {:type rname})))

(defn read-enum
  [^DatumReader this ^Schema expected ^Decoder in]
  (keyword (-> expected .getEnumSymbols (.get (.readEnum in)))))

(defn read-array
  [^DatumReader this ^Schema expected ^ResolvingDecoder in]
  (let [vtype (.getElementType expected), n (.readArrayStart in)]
    (if-not (pos? n)
      []
      (persistent!
       (loop [m (transient []), n (long n)]
         (if-not (pos? n)
           (let [n (.arrayNext in)] (if-not (pos? n) m (recur m n)))
           (let [v (.read this nil vtype in)]
             (recur (conj! m v) (dec n)))))))))

(defn read-map
  [^DatumReader this ^Schema expected ^ResolvingDecoder in]
  (let [vtype (.getValueType expected), n (.readMapStart in)]
    (if-not (pos? n)
      {}
      (persistent!
       (loop [m (transient {}), n (long n)]
         (if-not (pos? n)
           (let [n (.mapNext in)] (if-not (pos? n) m (recur m n)))
           (let [k (.readString in), v (.read this nil vtype in)]
             (recur (assoc! m k v) (dec n)))))))))

(defn read-fixed
  [^DatumReader this ^Schema expected ^Decoder in]
  )

(defn read-bytes
  [^DatumReader this ^Schema expected ^Decoder in]
  )
