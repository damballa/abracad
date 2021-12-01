(ns abracad.avro.codec
  "Basic functions for serializing/deserializing Avro
  data and making it portable via Base64 encoding"
  (:refer-clojure :exclude [load])
  (:require
    [abracad.avro :as avro])
  (:import
    (java.util
      Base64
      Base64$Decoder
      Base64$Encoder)))


(def base64-encoder (Base64/getEncoder))

(def base64-decoder (Base64/getDecoder))


(defn base64-encode
  [^bytes bytes]
  (.encode ^Base64$Encoder base64-encoder ^bytes bytes))


(defn base64-decode
  [payload]
  (let [payload-string (if (string? payload)
                         payload
                         (String. ^bytes payload))]
    (.decode ^Base64$Decoder base64-decoder ^String payload-string)))


(defn dump
  "Encode Avro message into a bag of bytes"
  [schema message]
  (avro/binary-encoded schema message))


(defn load
  "Deserialize Avro structure into a Clojure structure, must conform to `schema`"
  [schema payload]
  (avro/decode schema payload))


(defn parse-schema*
  "Reads schema definition and parses it"
  [& schema-definition]
  (apply avro/parse-schema schema-definition))


;; Memoized version of parse schema.
;; Schemas do not change at runtime so it's
;; ok to cache parsing results
(def ^{:doc "Memoized version of parse-schema*"} parse-schema
  (memoize parse-schema*))


(defn ->avro
  "Encodes given arg with Avro schema"
  [schema-struct payload]
  (dump (parse-schema schema-struct) payload))


(defn avro->
  "Decodes given bag bytes with Avro schema"
  [schema-struct payload]
  (load (parse-schema schema-struct) payload))


(defn ->avro-base64
  "Encode the payload with given Avro schema
  and encode it as Base64"
  [schema-struct payload]
  (base64-encode (->avro schema-struct payload)))


(defn avro-base64->
  "Decode Base64 data and load data with given Avro schema"
  [schema-struct payload]
  (avro-> schema-struct (base64-decode payload)))
