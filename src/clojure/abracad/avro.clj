(ns abracad.avro
  "Functions for de/serializing data with Avro."
  (:refer-clojure :exclude [compare])
  (:require [clojure.java.io :as io]
            [clojure.walk :refer [postwalk]]
            [cheshire.core :as json]
            [abracad.avro.util :refer [returning mangle unmangle coerce]])
  (:import [java.io
             ByteArrayInputStream ByteArrayOutputStream EOFException
             File FileInputStream InputStream OutputStream]
           [clojure.lang Named]
           [org.apache.avro Schema Schema$Parser Schema$Type]
           [org.apache.avro.file
             CodecFactory DataFileWriter DataFileReader SeekableInput
             SeekableFileInput SeekableByteArrayInput]
           [org.apache.avro.io
             DatumReader DatumWriter Decoder DecoderFactory
             Encoder EncoderFactory]
           [abracad.avro ClojureDatumReader ClojureDatumWriter ClojureData]))

(defn schema?
  "True iff `schema` is an Avro `Schema` instance."
  [schema] (instance? Schema schema))

(defn ^:private named?
  "True iff `x` is something which may be provided as an argument to `name`."
  [x] (or (string? x) (instance? Named x)))

(defn ^:private schema-mangle
  "Mangle `named` forms and existing schemas."
  [form]
  (cond (named? form) (-> form name mangle)
        (schema? form) (json/parse-string (str form))
        :else form))

(defn ^:private clj->json
  "Parse Clojure data into a JSON schema."
  [schema] (json/generate-string (postwalk schema-mangle schema)))

(defn ^:private codec-for
  "Return Avro codec factory for `codec`."
  {:tag `CodecFactory}
  [codec] (if-not (string? codec) codec (CodecFactory/fromString codec)))

(defprotocol PSeekableInput
  "Protocol for coercing to an Avro `SeekableInput`."
  (-seekable-input [x opts]
    "Attempt to coerce `x` to an Avro `SeekableInput`."))

(defn seekable-input
  "Attempt to coerce `x` to an Avro `SeekableInput`."
  {:tag `SeekableInput}
  ([x] (-seekable-input x nil))
  ([opts x] (-seekable-input x opts)))

(extend-protocol PSeekableInput
  (Class/forName "[B") (-seekable-input [x opts] (SeekableByteArrayInput. x))
  SeekableInput (-seekable-input [x opts] x)
  File (-seekable-input [x opts] (SeekableFileInput. x))
  String (-seekable-input [x opts] (seekable-input opts (io/file x))))

(defn ^:private raw-schema?
  "True if schema `source` should be parsed as-is."
  [source]
  (or (instance? InputStream source)
      (and (string? source)
           (.lookingAt (re-matcher #"[\[\{\"]" source)))))

(defn ^:private parse-schema-raw
  [^Schema$Parser parser source]
  (if (instance? String source)
    (.parse parser ^String source)
    (.parse parser ^InputStream source)))

(defn ^:private parse-schema*
  {:tag `Schema}
  [& sources]
  (let [parser (Schema$Parser.)]
    (reduce (fn [_ source]
              (->> (cond (schema? source) (str source)
                         (raw-schema? source) source
                         :else (clj->json source))
                   (parse-schema-raw parser)))
            nil
            sources)))

(defn parse-schema
  "Parse Avro schemas in `source` and `sources`.  Each schema source may be a
JSON string, an input stream containing a JSON schema, a Clojure data structure
which may be converted to a JSON schema, or an already-parsed Avro schema
object.  The schema for each subsequent source may refer to the types defined in
the previous schemas.  The parsed schema from the final source is returned."
  {:tag `Schema}
  ([source] (if (schema? source) source (parse-schema* source)))
  ([source & sources] (apply parse-schema* source sources)))

(defn unparse-schema
  "Return Avro-normalized Clojure data version of `schema`.  If `schema` is not
already a parsed schema, will first normalize and parse it."
  [schema] (-> schema parse-schema str (json/parse-string true)))

(defn tuple-schema
  "Return Clojure-data Avro schema for record consisting of fields of the
provided `types`, and optionally named `name`."
  ([types] (-> "abracad.avro.tuple" gensym name (tuple-schema types)))
  ([name types]
     {:name name, :type "record",
      :abracad.reader "vector",
      :fields (vec (map-indexed (fn [i type]
                                  (merge {:name (str "field" i),
                                          :type type}
                                         (meta type)))
                                types))}))

(defn ^:private order-ignore
  "Update all but the first `n` record-field specifiers `fields` to have an
`:order` of \"ignore\"."
  [fields n]
  (vec (map-indexed (fn [i field]
                      (if (< i n)
                        field
                        (assoc field :order "ignore")))
                    fields)))

(defn grouping-schema
  "Produce a grouping schema version of record schema `schema` which ignores all
but the first `n` fields when sorting."
  [n schema] (-> schema unparse-schema (update-in [:fields] order-ignore n)))

(defn datum-reader
  "Return an Avro DatumReader which produces Clojure data structures."
  {:tag `ClojureDatumReader}
  ([] (ClojureDatumReader.))
  ([schema] (ClojureDatumReader. schema))
  ([expected actual] (ClojureDatumReader. expected actual)))

(defn data-file-reader
  "Return an Avro DataFileReader which produces Clojure data structures."
  {:tag `DataFileReader}
  ([source] (data-file-reader nil source))
  ([expected source]
     (DataFileReader/openReader
      (seekable-input source) (datum-reader expected))))

(defmacro ^:private decoder-factory
  "Invoke static methods of default Avro Decoder factory."
  [method & args] `(. (DecoderFactory/get) ~method ~@args))

(defn binary-decoder
  "Return a binary-encoding decoder for `source`.  The `source` may be
an input stream, a byte array, or a vector of `[bytes off len]`."
  {:tag `Decoder}
  [source]
  (if (vector? source)
    (let [[source off len] source]
      (decoder-factory binaryDecoder source off len nil))
    (if (instance? InputStream source)
      (decoder-factory binaryDecoder ^InputStream source nil)
      (decoder-factory binaryDecoder ^bytes source nil))))

(defn direct-binary-decoder
  "Return a non-buffered binary-encoding decoder for `source`."
  {:tag `Decoder}
  [source] (decoder-factory directBinaryDecoder source nil))

(defn json-decoder
  "Return a JSON-encoding decoder for `source` using `schema`."
  {:tag `Decoder}
  [schema source]
  (if (instance? InputStream source)
    (decoder-factory jsonDecoder ^Schema schema ^InputStream source)
    (decoder-factory jsonDecoder ^Schema schema ^String source)))

(defn decode
  "Decode and return one object from `source` using `schema`.  The
`source` may be an existing Decoder object or anything on which
a (binary-encoding) Decoder may be opened."
  [schema source]
  (let [reader (coerce DatumReader datum-reader schema)
        decoder (coerce Decoder binary-decoder source)]
    (.read ^DatumReader reader nil ^Decoder decoder)))

(defn decode-seq
  "As per `decode`, but decode and return a sequence of all objects
decoded serially from `source`."
  [schema source]
  (let [reader (coerce DatumReader datum-reader schema)
        decoder (coerce Decoder binary-decoder source)]
    ((fn step []
       (lazy-seq
        (try
          (let [record (.read ^DatumReader reader nil ^Decoder decoder)]
            (cons record (step)))
          (catch EOFException _ nil)))))))

(defn datum-writer
  "Return an Avro DatumWriter which consumes Clojure data structures."
  {:tag `ClojureDatumWriter}
  ([] (ClojureDatumWriter.))
  ([schema] (ClojureDatumWriter. schema)))

(defn data-file-writer
  "Return an Avro DataFileWriter which consumes Clojure data structures."
  {:tag `DataFileWriter}
  ([] (DataFileWriter. (datum-writer)))
  ([sink]
     (let [^DataFileWriter dfw (data-file-writer)]
       (doto dfw (.appendTo (io/file sink)))))
  ([schema sink]
     (data-file-writer nil schema sink))
  ([codec schema sink]
     (let [^DataFileWriter writer (data-file-writer)]
       (when codec (.setCodec writer (codec-for codec)))
       (if (instance? OutputStream sink)
         (.create writer ^Schema schema ^OutputStream sink)
         (.create writer ^Schema schema (io/file sink)))
       writer)))

(defmacro ^:private encoder-factory
  "Invoke static methods of default Avro Encoder factory."
  [method & args] `(. (EncoderFactory/get) ~method ~@args))

(defn binary-encoder
  "Return a binary-encoding encoder for `sink`."
  {:tag `Encoder}
  [sink] (encoder-factory binaryEncoder sink nil))

(defn direct-binary-encoder
  "Return an unbuffered binary-encoding encoder for `sink`."
  {:tag `Encoder}
  [sink] (encoder-factory directBinaryEncoder sink nil))

(defn json-encoder
  "Return a JSON-encoding encoder for `sink` using `schema`."
  {:tag `Encoder}
  [schema sink]
  (encoder-factory jsonEncoder ^Schema schema ^OutputStream sink))

(defn encode
  "Serially encode each record in `records` to `sink` using `schema`.
The `sink` may be an existing Encoder object, or anything on which
a (binary-encoding) Encoder may be opened."
  [schema sink & records]
  (let [^DatumWriter writer (coerce DatumWriter datum-writer schema)
        ^Encoder encoder (coerce Encoder binary-encoder sink)]
    (doseq [record records]
      (.write writer record encoder))
    (.flush encoder)))

(defn binary-encoded
  "Return bytes produced by binary-encoding `records` with `schema`
via `encode`."
  [schema & records]
  (with-open [out (ByteArrayOutputStream.)]
    (apply encode schema out records)
    (.toByteArray out)))

(defn json-encoded
  "Return string produced by JSON-encoding `records` with `schema`
via `encode`."
  [schema & records]
  (with-open [out (ByteArrayOutputStream.)]
    (apply encode schema (json-encoder schema out) records)
    (String. (.toByteArray out))))

(defn compare
  "Compare `x` and `y` according to `schema`."
  [schema x y] (.compare (ClojureData/get) x y ^Schema schema))

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
