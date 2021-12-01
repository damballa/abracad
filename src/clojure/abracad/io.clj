(ns abracad.io
  (:require
    [cheshire.core :as json]
    [clojure.java.io :as io]))


(defn read-json
  [io-or-path]
  (try
    (json/parse-string (slurp io-or-path) true)
    (catch Exception e
      (throw (ex-info
               "Failed to parse schema file"
               {:io io-or-path}
               e)))))


(defn read-json-resource
  [path]
  (if-let [json-schema (io/resource path)]
    (read-json json-schema)
    (throw (ex-info "missing avro schema file" {:path path}))))
