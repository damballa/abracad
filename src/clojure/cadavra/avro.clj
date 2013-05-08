(ns cadavra.avro
  (:require [cadavra.util :refer [returning]])
  (:import [org.apache.avro Schema]
           [org.apache.avro.io DatumReader Decoder ResolvingDecoder]))

(defn read-record
  [^DatumReader reader ^Schema expected ^ResolvingDecoder in]
  (let [rname (keyword (.getNamespace expected) (.getName expected))]
    (with-meta
      (persistent!
       (reduce (fn [m f]
                 (assoc! m
                   (keyword (.name f))
                   (.read reader nil (.schema f) in)))
               (transient {})
               (.readFieldOrder in)))
      {:type rname})))

(defn read-enum
  [^DatumReader reader ^Schema expected ^Decoder in]
  (keyword (-> expected .getEnumSymbols (.get (.readEnum in)))))

(defn read-array
  [^DatumReader reader ^Schema expected ^ResolvingDecoder in]
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
  [^DatumReader reader ^Schema expected ^ResolvingDecoder in]
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
  [^DatumReader reader ^Schema expected ^Decoder in]
  (let [size (.getFixedSize expected), bytes (byte-array size)]
    (returning bytes
      (.readFixed in bytes 0 size))))

(defn read-bytes
  [^DatumReader reader ^Schema expected ^Decoder in]
  (.readBytes in))
