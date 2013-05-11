(ns abracad.core-test
  (:require [clojure.test :refer :all]
            [cheshire.core :as json]
            [abracad.avro :as avro])
  (:import [java.io ByteArrayOutputStream]
           [java.net InetAddress]))

(def schema
  (avro/schema-parse
   {:type :record,
    :namespace 'abracad.core-test
    :name 'Example
    :fields [{:name "foo-foo" :type :string}
             {:name "bar"
              :type [:null
                     {:type :record
                      :name 'SubExample
                      :fields [{:name "baz", :type :long}]}]}]}))

(defrecord Example [foo-foo bar])

(defrecord SubExample [^long baz])

(def example-bytes
  (with-open [out (ByteArrayOutputStream.)]
    (avro/encode schema out (->Example "bar" (->SubExample 0)))
    (.toByteArray out)))

(extend-type InetAddress
  avro/FieldLookup
  (field-get [this field]
    (case field
      :address (.getAddress this)))
  (field-list [this] #{:address}))

(defn map->InetAddress
  [{:keys [address]}] (InetAddress/getByAddress address))

(comment

  (binding [avro/*avro-readers*
            {'abracad.core-test/Example #'map->Example
             'abracad.core-test/SubExample #'map->SubExample}]
    (let [record (avro/decode schema example-bytes)]
      [record (meta record)]))

  (let [schema (avro/schema-parse
                {:type :record
                 :name 'java.net.InetAddress
                 :fields [{:name :address
                           :type [{:type :fixed, :name "IPv4", :size 4}
                                  {:type :fixed, :name "IPv6", :size 16}]}]})
        bytes (with-open [out (ByteArrayOutputStream.)]
                (avro/encode schema out
                   (InetAddress/getByName "8.8.8.8")
                   (InetAddress/getByName "8::8"))
                (.toByteArray out))]
    (binding [avro/*avro-readers* {'java.net/InetAddress #'map->InetAddress}]
      (doall (avro/decode-seq schema bytes))))

  )
