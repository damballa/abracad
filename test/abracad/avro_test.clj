(ns abracad.avro-test
  (:require [abracad.avro :as avro]
            [abracad.custom-types-test :as custom-types]
            [clojure.java.io :as io]
            [clojure.test :refer :all]
            [matcher-combinators.midje :refer [match]]
            [midje.sweet :refer :all])
  (:import [clojure.lang ExceptionInfo]
           [java.io FileInputStream]
           [java.net InetAddress]
           [org.apache.avro.file DataFileStream]
           [org.apache.avro SchemaParseException]))

(defn roundtrip-binary [schema & records]
  (->> (apply avro/binary-encoded schema records)
       (avro/decode-seq schema)))

(defn roundtrip-json [schema & records]
  (->> (apply avro/json-encoded schema records)
       (avro/json-decoder schema)
       (avro/decode-seq schema)))

(defn roundtrips?
  ([schema input] (roundtrips? schema input input))
  ([schema expected input]
     (and (= expected (apply roundtrip-binary schema input))
          (= expected (apply roundtrip-json schema input)))))

(defn roundtrips [schema input]
  [(apply roundtrip-binary schema input)
   (apply roundtrip-json schema input)])

(defn match-duplicate-data [data]
  (->> data
       (repeat 2)
       (into [])
       match))

(defrecord Example [foo-foo bar])

(defrecord SubExample [^long baz])

(facts "we can encode/decode records"
  (let [schema  {:type      :record,
                 :namespace 'abracad.core-test
                 :name      'Example
                 :fields    [{:name "foo-foo" :type :string}
                             {:name "bar"
                              :type [:null
                                     {:type   :record
                                      :name   'SubExample
                                      :fields [{:name "baz",
                                                :type :long}]}]}]}
        records [(->Example "bar" (->SubExample 0))]]

    (fact "by default it returns a map'"
      (roundtrips schema records) => (match-duplicate-data [{:foo-foo "bar" :bar {:baz 0}}]))

    (fact "if we epand *avro-reader* it returns the records we created"
      (binding [avro/*avro-readers*
                , {'abracad.core-test/Example    #'->Example
                   'abracad.core-test/SubExample #'->SubExample}]
        (roundtrips schema records) => (match-duplicate-data [{:foo-foo "bar" :bar {:baz 0}}])))))

(fact "we can encode/decode java objects when we implement the AvroSerializable protocol"
  (let [schema  (avro/parse-schema
                  {:type   :record
                   :name   'ip.address
                   :fields [{:name :address
                             :type [{:type :fixed, :name "IPv4", :size 4}
                                    {:type :fixed, :name "IPv6", :size 16}]}]})
        records [(InetAddress/getByName "8.8.8.8")
                 (InetAddress/getByName "8::8")]]
    (binding [avro/*avro-readers* {'ip/address #'custom-types/->InetAddress}]
      (roundtrips schema records) => (match-duplicate-data records))))

(fact "we can encode/decode data with a int schema"
  (let [schema (avro/parse-schema :int)]
    (roundtrips schema [(int 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(long 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(float 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(double 12345)]) => (match-duplicate-data [12345])))

(fact "we can encode/decode data with a long schema"
  (let [schema (avro/parse-schema :long)]
    (roundtrips schema [(int 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(long 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(float 12345)]) => (match-duplicate-data [12345])
    (roundtrips schema [(double 12345)]) => (match-duplicate-data [12345])))

(fact "we can encode/decode data with a float schema"
  (let [schema (avro/parse-schema :float)]
    (roundtrips schema [(int 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(long 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(float 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(double 12345)]) => (match-duplicate-data [12345.0])))

(fact "we can encode/decode data with a double schema"
  (let [schema (avro/parse-schema :double)]
    (roundtrips schema [(int 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(long 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(float 12345)]) => (match-duplicate-data [12345.0])
    (roundtrips schema [(double 12345)]) => (match-duplicate-data [12345.0])))

(fact "we can encode/decode data with a boolean schema"
  (let [schema (avro/parse-schema :boolean)]
    (roundtrips schema [:anything]) => (match-duplicate-data [true])
    (roundtrips schema [true]) => (match-duplicate-data [true])
    (roundtrips schema [false]) => (match-duplicate-data [false])
    (roundtrips schema [nil]) => (match-duplicate-data [false])))

(fact "we can encode/decode data with a union schema"
  (let [vertical   {:type :enum, :name "vertical", :symbols [:up :down]}
        horizontal (avro/parse-schema
                     {:type :enum, :name "horizontal", :symbols [:left :right]})
        schema     (avro/parse-schema
                     vertical horizontal
                     [:null :long :string "vertical" "horizontal"])
        records    ["down" :up :down :left 0 :right "left"]]
    (roundtrips schema records) => (match-duplicate-data records)))

(fact "we can encode/decode data of either record schema of the union"
  (let [example1 {:type   :record, :name 'schema1,
                  :fields [{:name 'long, :type 'long}]}
        example2 {:type   :record, :name 'schema2,
                  :fields [{:name 'string, :type 'string}]}
        schema   (avro/parse-schema [example1 example2])
        records  [{:long 0} {:string "string"}]
        records' (roundtrips schema records)]
    records' => (match-duplicate-data records)
    (map #(map (comp :type meta) %) records') => (match-duplicate-data '[schema1 schema2])))

(fact "we can encode/decode bytes using both fixed and bytes schema"
  (let [schema  (avro/parse-schema
                  [{:type :fixed, :name "foo", :size 1}
                   :bytes])
        records [(byte-array (map byte [1]))
                 (byte-array (map byte [1 2]))]
        bytes   (apply avro/binary-encoded schema records)
        thawed  (avro/decode-seq schema bytes)]
    ;; Only testing binary, as JSON encoding does *not* round-trip :-(
    (alength ^bytes bytes) => 6
    thawed => #(every? (partial instance? (Class/forName "[B")) %)
    (map seq thawed) => (map seq records)))

(fact "we can encode/decode arrays"
  (let [schema  (avro/parse-schema {:type :array, :items :long})
        records [[] [0 1] (range 1024)]]
    (roundtrips schema records) => [records records]))

(fact "when decoding arrays we can use primitives to add additional conversions"
  (let [schema   (avro/parse-schema
                   {:type 'array, :items 'int, :abracad.array 'ints})
        records  [(int-array []) (int-array [0 1]) (int-array (range 1024))]
        thawed-b (apply roundtrip-binary schema records)
        thawed-j (apply roundtrip-json schema records)]
    (map class thawed-b) => (map class records)
    (map seq thawed-j) => (map seq records)))

(fact "we can encode/decode avro maps (keys are string and values is as defined in the schema)"
  (let [schema  (avro/parse-schema {:type :map, :values :long})
        records [{}
                 {"foo" 0, "bar" 1}
                 (->> (range 1024)
                      (map #(-> [(str %) %]))
                      (into {}))]]
    (roundtrips schema records) => (match-duplicate-data records)))

(facts "when adding extra elements to the be encoded"
  (let [schema (avro/parse-schema
                 {:name   "Example", :type "record",
                  :fields [{:name "foo", :type "long"}]})]
    (fact "throws exception when having a element not defined in the schema"
      (avro/binary-encoded schema {:foo 0, :bar 1}) => (throws clojure.lang.ExceptionInfo)
      (avro/json-encoded schema {:foo 0, :bar 1}) => (throws clojure.lang.ExceptionInfo))
    (fact "we can type hint the datum to allow extra elements in the encoding - but the extra elements will be dropped"
      (roundtrips schema [^{:type 'Example} {:foo 0, :bar 1}]) => (match-duplicate-data [{:foo 0}]))
    (fact "we can turn of the check of schemas - but the extra elements will be dropped"
      (roundtrips schema [^:avro/unchecked {:foo 0, :bar 1}]) => (match-duplicate-data [{:foo 0}]))))

(fact "test-positional"
  (let [schema  (avro/parse-schema {:name           "Example"
                                    :type           "record"
                                    :abracad.reader "vector"
                                    :fields         [{:name "left", :type "long"}
                                                     {:name "right", :type "string"}]}
                                   [:Example :string])
        records [[0 "foo"] [1 "bar"] [2 "baz"] "quux"]
        record  (first records)
        thawed  (->> records
                     (apply avro/binary-encoded schema)
                     (avro/decode-seq schema))
        records' (roundtrips schema records)]
    records' => (match-duplicate-data records)
    (map #(map type %) records') => [['Example 'Example 'Example String]
                                     ['Example 'Example 'Example String]]))

(fact "on schema mangling"
  (fact "when a name is mangled in the encoding it gets back to the original on the decoding"
    (let [schema   (avro/parse-schema
                     {:name           "mangle-me", :type "record",
                      :abracad.reader "vector"
                      :fields         [{:name "field0", :type "long"}]}
                     ["mangle-me" "long"])
          records  [0 [1] [2] 3 [4]]
          records' (roundtrips schema records)]
     records' => (match-duplicate-data records)))

  (fact "also works for subschemas"
    (let [schema-def {:name           "mangle-me", :type "record",
                      :abracad.reader "vector"
                      :fields         [{:name "field0", :type "long"}]}
          schema1    (avro/parse-schema schema-def)
          schema     (avro/parse-schema [schema1 "long"])
          records    [0 [1] [2] 3 4 [5]]]
    (roundtrips schema records) => (match-duplicate-data records)
    (-> schema1 avro/unparse-schema :name) => "mangle_me"))

  (fact "test-mangling"
    (let [schema     (avro/parse-schema
                       {:name   "mangling", :type "record",
                        :fields [{:name "a-dash", :type "long"}]})
          dash-data  [{:a-dash 1}]
          underscore-data [{:a_dash 1}]]

      (facts "by default names are mangled"
        (fact "if you use your clojure data only with dash it works fine"
          (roundtrips schema dash-data) => (match-duplicate-data dash-data))
        (fact "if you use your clojure data with underscore it'll not work"
          (roundtrips? schema underscore-data) => (throws ExceptionInfo)))

      (facts "we can disallow name mangling"
        (fact "if there are dashes we get an error"
          (binding [abracad.avro.util/*mangle-names* false]
            (roundtrips? schema dash-data))) => (throws ExceptionInfo)

        (fact "if you use only underscores it works fine"
          (binding [abracad.avro.util/*mangle-names* false]
            (roundtrips schema underscore-data) => (match-duplicate-data underscore-data)))))))

(fact "we can define schemas based on previously defined schemas"
  (let [schema1 (avro/parse-schema
                  {:name           "Example0", :type "record",
                   :abracad.reader "vector"
                   :fields         [{:name "field0", :type "long"}]}
                  {:name           "Example1", :type "record",
                   :abracad.reader "vector"
                   :fields         [{:name "field0", :type "Example0"}]})
        schema  (avro/parse-schema schema1 "Example0")
        records [[0] [1] [2] [3] [4] [5]]]
    (roundtrips schema records) => (match-duplicate-data records)))

(fact "we have a fn to easily define tuple schema"
  (let [schema1 (avro/tuple-schema ["string" "long" "long"])
        schema2 (avro/tuple-schema ["long" schema1])
        schema  (avro/parse-schema schema2)
        records [[0 ["foo" 1 2]] [3 ["bar" 4 5]]]]
    (roundtrips schema records) => (match-duplicate-data records)))

(fact "test-grouping-schema"
  (let [schema1 (avro/unparse-schema (avro/tuple-schema ["string" "long"]))
        schema2 (avro/unparse-schema (avro/grouping-schema 2 schema1))
        schema3 (avro/unparse-schema (avro/grouping-schema 1 schema1))]
    schema1 => schema2
    schema1 =not=> schema3
    (get-in schema3 [:fields 0 :order] "ascending") => "ascending"
    (get-in schema3 [:fields 1 :order] "ascending") => "ignore"))

(facts "we can write (spit) and then read (slurp) a file - the data is not changed"
  (fact "we can write a single entry per spit call"
    (let [path    "tmp/spit-slurp.avro"
          schema  {:type :array, :items 'long}
          records [0 1 2 3 4 5]]
      (io/make-parents path)
      (avro/spit schema path records)
      (avro/slurp path) => records))

  (fact "we can write all data in a single call"
    (let [path    "tmp/spit-slurp.avro"
          schema  'long
          records [0 1 2 3 4 5]]
      (io/make-parents path)
      (avro/mspit schema path records)
      (avro/mslurp path) => records)))

(facts "we can read a file as a stream of records"
  (let [path    "tmp/data-file-stream.avro"
        schema  {:type :long}
        records [0 1 2 3 4 5]]
    (io/make-parents path)
    (avro/mspit schema path records)

    (fact "we can get the data as a sequence"
      (with-open [dfs ^DataFileStream (avro/data-file-stream (FileInputStream. path))]
        (seq dfs) => records))))
