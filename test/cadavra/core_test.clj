(ns cadavra.core-test
  (:require [clojure.test :refer :all]
            [cadavra.avro :as avro]
            [cheshire.core :as json]))


(import '[org.apache.avro.generic
           GenericDatumWriter GenericDatumReader GenericData$Record])
(import '[java.io ByteArrayInputStream ByteArrayOutputStream])
(import '[org.apache.avro Schema Schema$Parser])
(def schema (->> {:type :record,
                  :namespace 'cadavra.core-test
                  :name 'Example
                  :fields [{:name "foo" :type :string}
                           {:name "bar"
                            :type [:null
                                   {:type :record
                                    :namespace 'cadavra.core-test
                                    :name 'SubExample
                                    :fields [{:name "baz", :type :long}]}]}]}
                 (json/generate-string)
                 (.parse (Schema$Parser.))))
(def sub-schema (->> {:type :record
                      :namespace 'cadavra.core-test
                      :name 'SubExample
                      :fields [{:name "baz", :type :long}]}
                     (json/generate-string)
                     (.parse (Schema$Parser.))))
(import '[org.apache.avro.io EncoderFactory DecoderFactory])
(def example-bytes
  (let [writer (GenericDatumWriter. schema),
        out (ByteArrayOutputStream.),
        encoder (-> (EncoderFactory/get) (.binaryEncoder out nil))
        record (doto (GenericData$Record. schema)
                 (.put "foo" "bar")
                 (.put "bar" (doto (GenericData$Record. sub-schema)
                               (.put "baz" 0))))]
    (.write writer record encoder)
    (.flush encoder)
    (.close out)
    (.toByteArray out)))
(import 'cadavra.avro.ClojureDatumReader)

(comment

 (let [reader (ClojureDatumReader. schema),
       decoder (-> (DecoderFactory/get) (.binaryDecoder example-bytes nil))
       record (.read reader nil decoder)]
   [record (meta record)])

 )
