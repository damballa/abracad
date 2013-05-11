(ns abracad.avro.mapping)

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
