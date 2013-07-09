(ns abracad.avro
  "Functions for de/serializing data with Avro."
  (:require [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [cheshire.core :as json]
            [abracad.avro.util :refer [returning mangle unmangle]])
  (:import [java.io InputStream EOFException]
           [clojure.lang Named]
           [org.apache.avro Schema Schema$Parser]
           [org.apache.avro.file CodecFactory DataFileWriter DataFileReader]
           [org.apache.avro.io
             DatumReader DatumWriter Decoder DecoderFactory
             Encoder EncoderFactory]
           [abracad.avro ClojureDatumReader ClojureDatumWriter]))

(defn ^:private named?
  "True iff `x` is something which may be provided as an argument to `name`."
  [x] (or (string? x) (instance? Named x)))

(defn ^:private mangle-value
  "Mange the value of key `k` in map `m`."
  [m k] (let [x (m k)] (if-not (named? x) m (assoc m k (-> x name mangle)))))

(defn ^:private schema-mangle
  "If `form` is a map, mangle the values of the `:name`, `:namespace`,
and `:type` keys."
  [form]
  (if-not (map? form)
    form
    (-> (mangle-value form :name)
        (mangle-value ,,,, :namespace)
        (mangle-value ,,,, :type))))

(defn ^:private clj->json
  "Parse Clojure data into a JSON schema."
  [schema] (json/generate-string (postwalk schema-mangle schema)))

(defn ^:private codec-for
  "Return Avro codec factory for `codec`."
  {:tag 'org.apache.avro.file.CodecFactory}
  [codec] (if-not (string? codec) codec (CodecFactory/fromString codec)))

(defn ^:private sink-for
  "Return sinkable endpoint for `sink`."
  [sink] (if-not (string? sink) sink (io/file sink)))

(defn ^:private raw-schema?
  "True if schema `source` should be parsed as-is."
  [source] (or (string? source) (instance? InputStream source)))

(defn parse-schema
  "Parse Avro schemas in `sources`.  Each schema source may be a JSON
string, an input stream containing a JSON schema, a Clojure data
structure which may be converted to a JSON schema, or an
already-parsed Avro schema object.  The schema for each subsequent
source may represent the types defined in the previous schemas.  The
parsed schema from the final source is returned."
  [& sources]
  (let [parser (Schema$Parser.)]
    (reduce (fn [_ source]
              (if (instance? Schema source)
                (returning source
                  (.addTypes parser {(.getName ^Schema source) source}))
                (->> (if (raw-schema? source) source (clj->json source))
                     (.parse parser))))
            nil
            sources)))

(defn datum-reader
  "Return an Avro DatumReader which produces Clojure data structures."
  {:tag 'abracad.avro.ClojureDatumReader}
  ([] (ClojureDatumReader.))
  ([schema] (ClojureDatumReader. schema))
  ([expected actual] (ClojureDatumReader. expected actual)))

(defn data-file-reader
  "Return an Avro DataFileReader which produces Clojure data structures."
  {:tag 'org.apache.avro.file.DataFileReader}
  ([source] (data-file-reader nil source))
  ([expected source]
     (DataFileReader. (sink-for source) (datum-reader expected))))

(defmacro ^:private decoder-factory
  "Invoke static methods of default Avro Decoder factory."
  [method & args] `(. (DecoderFactory/get) ~method ~@args))

(defn binary-decoder
  "Return a binary-encoding decoder for `source`.  The `source` may be
an input stream, a byte array, or a vector of `[bytes off len]`."
  {:tag 'org.apache.avro.io.Decoder}
  [source]
  (if-not (vector? source)
    (decoder-factory binaryDecoder source nil)
    (let [[source off len] source]
      (decoder-factory binaryDecoder source off len nil))))

(defn direct-binary-decoder
  "Return a non-buffered binary-encoding decoder for `source`."
  {:tag 'org.apache.avro.io.Decoder}
  [source] (decoder-factory directBinaryDecoder source nil))

(defn json-decoder
  "Return a JSON-encoding decoder for `source` using `schema`."
  {:tag 'org.apache.avro.io.Decoder}
  [schema source] (decoder-factory jsonDecoder schema source))

(defn decode
  "Decode and return one object from `source` using `schema`.  The
`source` may be an existing Decoder object or anything on which
a (binary-encoding) Decoder may be opened."
  [schema source]
  (let [reader (if (instance? DatumReader schema) schema (datum-reader schema))
        decoder (if (instance? Decoder source) source (binary-decoder source))]
    (.read ^DatumReader reader nil ^Decoder decoder)))

(defn decode-seq
  "As per `decode`, but decode and return a sequence of all objects
decoded serially from `source`."
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
  "Return an Avro DatumWriter which consumes Clojure data structures."
  {:tag 'abracad.avro.ClojureDatumWriter}
  ([] (ClojureDatumWriter.))
  ([schema] (ClojureDatumWriter. schema)))

(defn data-file-writer
  "Return an Avro DataFileWriter which consumes Clojure data structures."
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
  "Invoke static methods of default Avro Encoder factory."
  [method & args] `(. (EncoderFactory/get) ~method ~@args))

(defn binary-encoder
  "Return a binary-encoding encoder for `sink`."
  {:tag 'org.apache.avro.io.Encoder}
  [sink] (encoder-factory binaryEncoder sink nil))

(defn direct-binary-encoder
  "Return an unbuffered binary-encoding encoder for `sink`."
  {:tag 'org.apache.avro.io.Encoder}
  [sink] (encoder-factory directBinaryEncoder sink nil))

(defn json-encoder
  "Return a JSON-encoding encoder for `sink` using `schema`."
  {:tag 'org.apache.avro.io.Encoder}
  [schema sink] (encoder-factory jsonEncoder schema sink))

(defn encode
  "Serially encode each record in `records` to `sink` using `schema`.
The `sink` may be an existing Encoder object, or anything on which
a (binary-encoding) Encoder may be opened."
  [schema sink & records]
  (let [writer (if (instance? DatumWriter schema) schema (datum-writer schema))
        encoder (if (instance? Encoder sink) sink (binary-encoder sink))]
    (doseq [record records]
      (.write ^DatumWriter writer record ^Encoder encoder))
    (.flush encoder)))

(defprotocol AvroSerializable
  "Protocol for customizing Avro serialization."
  (schema-name [this]
    "Full package-/namespace-qualified name for Avro purposes.")
  (field-get [this field]
    "Value of keyword `field` for Avro serialization of object.")
  (field-list [this]
    "List of keyword fields this object provides."))

;; The following implementation is pretty much just copy-pasted
;; directly from the Clojure *data-readers* implementation.

(def ^:dynamic *avro-readers*
  "Like `clojure.core/*data-readers*`, but for reading Avro records.
Initializes with merged contents of `avro_readers.clj` resources.
Whenever an Avro record is deserialized, the Clojure datum reader will
check this map for a key matching the Avro record name represented as
a namespace-qualified symbol.  When found, the datum reader will
invoke the associated value as a function on the deserialized record's
fields as positional arguments.  The datum reader will use the return
value as the deserialization result."
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
