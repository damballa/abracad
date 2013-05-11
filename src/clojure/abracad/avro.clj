(ns abracad.avro
  (:require [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [cheshire.core :as json]
            [abracad.avro.util :refer [mangle unmangle]])
  (:import [java.io InputStream EOFException]
           [org.apache.avro Schema Schema$Parser]
           [org.apache.avro.file CodecFactory DataFileWriter DataFileReader]
           [org.apache.avro.io
             DatumReader DatumWriter Decoder DecoderFactory
             Encoder EncoderFactory]
           [abracad.avro ClojureDatumReader ClojureDatumWriter]))

(defn ^:private mangle-value
  [m k] (if-not (contains? m k) m (assoc m k (-> m k name mangle))))

(defn ^:private schema-mangle
  [form]
  (if-not (map? form)
    form
    (-> (mangle-value form :name)
        (mangle-value ,,,, :namespace))))

(defn ^:private clj->json
  [schema] (json/generate-string (postwalk schema-mangle schema)))

(defn ^:private codec-for
  [codec] (if-not (string? codec) codec (CodecFactory/fromString codec)))

(defn ^:private sink-for
  [sink] (if-not (string? sink) sink (io/file sink)))

(defn schema-parse
  ""
  [& sources]
  (let [parser (Schema$Parser.)]
    (reduce (fn [_ source]
              (->> (if (or (string? source) (instance? InputStream source))
                     source
                     (clj->json source))
                   (.parse parser)))
            nil
            sources)))

(defn datum-reader
  ""
  {:tag 'abracad.avro.ClojureDatumReader}
  ([] (ClojureDatumReader.))
  ([schema] (ClojureDatumReader. schema))
  ([expected actual] (ClojureDatumReader. expected actual)))

(defn data-file-reader
  ""
  {:tag 'org.apache.avro.file.DataFileReader}
  ([source] (data-file-reader nil source))
  ([expected source]
     (DataFileReader. (sink-for source) (datum-reader expected))))

(defmacro ^:private decoder-factory
  [method & args] `(. (DecoderFactory/get) ~method ~@args))

(defn binary-decoder
  {:tag 'org.apache.avro.io.Decoder}
  [source]
  (if-not (vector? source)
    (decoder-factory binaryDecoder source nil)
    (let [[source off len] source]
      (decoder-factory binaryDecoder source off len nil))))

(defn direct-binary-decoder
  {:tag 'org.apache.avro.io.Decoder}
  [source] (decoder-factory directBinaryDecoder source nil))

(defn json-decoder
  {:tag 'org.apache.avro.io.Decoder}
  [schema source] (decoder-factory jsonDecoder schema source))

(defn decode
  ""
  [schema source]
  (let [reader (if (instance? DatumReader schema) schema (datum-reader schema))
        decoder (if (instance? Decoder source) source (binary-decoder source))]
    (.read ^DatumReader reader nil ^Decoder decoder)))

(defn decode-seq
  ""
  [schema source]
  (let [reader (if (instance? DatumReader schema) schema (datum-reader schema))
        decoder (if (instance? Decoder source) source (binary-decoder source))]
    ((fn step []
       (lazy-seq
        (try
          (let [record (.read ^DatumReader reader nil ^Decoder decoder)]
            (cons record (step)))
          (catch EOFException _ nil)))))))

(defn datum-writer
  ""
  {:tag 'abracad.avro.ClojureDatumWriter}
  ([] (ClojureDatumWriter.))
  ([schema] (ClojureDatumWriter. schema)))

(defn data-file-writer
  ""
  {:tag 'org.apache.avro.file.DataFileWriter}
  ([] (DataFileWriter. (datum-writer)))
  ([sink] (doto (data-file-writer) (.appendTo (sink-for sink))))
  ([schema sink] (data-file-writer nil schema sink))
  ([codec schema sink]
     (let [writer (data-file-writer)]
       (when codec (.setCodec writer (codec-for codec)))
       (.create writer schema (sink-for sink))
       writer)))

(defmacro ^:private encoder-factory
  [method & args] `(. (EncoderFactory/get) ~method ~@args))

(defn binary-encoder
  {:tag 'org.apache.avro.io.Encoder}
  [source] (encoder-factory binaryEncoder source nil))

(defn direct-binary-encoder
  {:tag 'org.apache.avro.io.Encoder}
  [source] (encoder-factory directBinaryEncoder source nil))

(defn json-encoder
  {:tag 'org.apache.avro.io.Encoder}
  [schema source] (encoder-factory jsonEncoder schema source))

(defn encode
  [schema sink & records]
  (let [writer (if (instance? DatumWriter schema) schema (datum-writer schema))
        encoder (if (instance? Encoder sink) sink (binary-encoder sink))]
    (doseq [record records]
      (.write ^DatumWriter writer record ^Encoder encoder))
    (.flush encoder)))

(defprotocol FieldLookup
  "Protocol for accessing fields for Avro serialization."
  (field-get [this field]
    "Value of keyword `field` for Avro serialization of object.")
  (field-list [this]
    "List of keyword fields this object provides."))

(extend-protocol FieldLookup
  Object
  (field-get [this field] (get this field))
  (field-list [this] (keys this)))

(def ^:dynamic *avro-readers*
  {})

(defn ^:private avro-reader-urls
  [] (enumeration-seq
      (-> (Thread/currentThread) .getContextClassLoader
          (.getResources "avro_readers.clj"))))

(defn ^:private avro-reader-var
  [sym] (intern (create-ns (symbol (namespace sym))) (symbol (name sym))))

(defn ^:private load-avro-reader-file
  [mappings ^java.net.URL url]
  (with-open [rdr (clojure.lang.LineNumberingPushbackReader.
                   (java.io.InputStreamReader.
                    (.openStream url) "UTF-8"))]
    (binding [*file* (.getFile url)]
      (let [new-mappings (read rdr false nil)]
        (when (not (map? new-mappings))
          (throw (ex-info (str "Not a valid avro-reader map")
                          {:url url})))
        (reduce
         (fn [m [k v]]
           (when (not (symbol? k))
             (throw (ex-info (str "Invalid form in avro-reader file")
                             {:url url
                              :form k})))
           (let [v-var (avro-reader-var v)]
             (when (and (contains? mappings k)
                        (not= (mappings k) v-var))
               (throw (ex-info "Conflicting avro-reader mapping"
                               {:url url
                                :conflict k
                                :mappings m})))
             (assoc m k v-var)))
         mappings
         new-mappings)))))

(defn ^:private load-avro-readers
  [] (alter-var-root #'*avro-readers*
                     (fn [mappings]
                       (reduce load-avro-reader-file
                               mappings (avro-reader-urls)))))

(try
  (load-avro-readers)
  (catch Throwable t
    (.printStackTrace t)
    (throw t)))
