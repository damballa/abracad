(ns abracad.avro-test
  (:require [clojure.test :refer :all]
            [abracad.avro :as avro]
            [clojure.java.io :as io])
  (:import [java.io ByteArrayOutputStream FileInputStream]
           [java.net InetAddress]
           [org.apache.avro SchemaParseException]
           [clojure.lang ExceptionInfo]))

(defn roundtrip-binary
  [schema & records]
  (->> (apply avro/binary-encoded schema records)
       (avro/decode-seq schema)
       (apply avro/json-encoded schema)
       (avro/json-decoder schema)
       (avro/decode-seq schema)))

(defn roundtrip-json
  [schema & records]
  (->> (apply avro/json-encoded schema records)
       (avro/json-decoder schema)
       (avro/decode-seq schema)))

(defn roundtrips?
  ([schema input] (roundtrips? schema input input))
  ([schema expected input]
     (and (= expected (apply roundtrip-binary schema input))
          (= expected (apply roundtrip-json schema input)))))

(defrecord Example [foo-foo bar])

(defrecord SubExample [^long baz])

(extend-type InetAddress
  avro/AvroSerializable
  (schema-name [_] "ip.address")
  (field-get [this field]
    (case field
      :address (.getAddress this)))
  (field-list [this] #{:address}))

(defn ->InetAddress
  [address] (InetAddress/getByAddress address))

(deftest test-example
  (let [schema {:type :record,
                :namespace 'abracad.core-test
                :name 'Example
                :fields [{:name "foo-foo" :type :string}
                         {:name "bar"
                          :type [:null
                                 {:type :record
                                  :name 'SubExample
                                  :fields [{:name "baz",
                                            :type :long}]}]}]}
        records [(->Example "bar" (->SubExample 0))]]
    (is (roundtrips? schema [{:foo-foo "bar" :bar {:baz 0}}] records))
    (binding [avro/*avro-readers*
              , {'abracad.core-test/Example #'->Example
                 'abracad.core-test/SubExample #'->SubExample}]
      (is (roundtrips? schema records)))))

(deftest test-customized
  (let [schema (avro/parse-schema
                {:type :record
                 :name 'ip.address
                 :fields [{:name :address
                           :type [{:type :fixed, :name "IPv4", :size 4}
                                  {:type :fixed, :name "IPv6", :size 16}]}]})
        records [(InetAddress/getByName "8.8.8.8")
                 (InetAddress/getByName "8::8")]]
    (binding [avro/*avro-readers* {'ip/address #'->InetAddress}]
      (is (roundtrips? schema records)))))

(deftest test-int
  (let [schema (avro/parse-schema 'int)]
    (is (roundtrips? schema [12345] [(int 12345)]))
    (is (roundtrips? schema [12345] [(long 12345)]))
    (is (roundtrips? schema [12345] [(float 12345)]))
    (is (roundtrips? schema [12345] [(double 12345)]))))

(deftest test-long
  (let [schema (avro/parse-schema 'long)]
    (is (roundtrips? schema [12345] [(int 12345)]))
    (is (roundtrips? schema [12345] [(long 12345)]))
    (is (roundtrips? schema [12345] [(float 12345)]))
    (is (roundtrips? schema [12345] [(double 12345)]))))

(deftest test-float
  (let [schema (avro/parse-schema 'float)]
    (is (roundtrips? schema [12345.0] [(int 12345)]))
    (is (roundtrips? schema [12345.0] [(long 12345)]))
    (is (roundtrips? schema [12345.0] [(float 12345)]))
    (is (roundtrips? schema [12345.0] [(double 12345)]))))

(deftest test-double
  (let [schema (avro/parse-schema 'double)]
    (is (roundtrips? schema [12345.0] [(int 12345)]))
    (is (roundtrips? schema [12345.0] [(long 12345)]))
    (is (roundtrips? schema [12345.0] [(float 12345)]))
    (is (roundtrips? schema [12345.0] [(double 12345)]))))

(deftest test-boolean
  (let [schema (avro/parse-schema 'boolean)]
    (is (roundtrips? schema [true] [:anything]))
    (is (roundtrips? schema [true] [true]))
    (is (roundtrips? schema [false] [false]))
    (is (roundtrips? schema [false] [nil]))))

(deftest test-union
  (let [vertical {:type :enum, :name "vertical", :symbols [:up :down]}
        horizontal (avro/parse-schema
                    {:type :enum, :name "horizontal", :symbols [:left :right]})
        schema (avro/parse-schema
                vertical horizontal
                [:null :long :string "vertical" "horizontal"])
        records ["down" :up :down :left 0 :right "left"]]
    (is (roundtrips? schema records))))

(deftest test-union-records
  (let [example1 {:type :record, :name 'example1,
                  :fields [{:name 'long, :type 'long}]}
        example2 {:type :record, :name 'example2,
                  :fields [{:name 'string, :type 'string}]}
        schema (avro/parse-schema [example1 example2])
        records [{:long 0} {:string "string"}]
        records' (apply roundtrip-binary schema records)]
    (is (= records records'))
    (is (= '[example1 example2]
           (map (comp :type meta) records')))))

(deftest test-bytes
  (let [schema (avro/parse-schema
                [{:type :fixed, :name "foo", :size 1}, :bytes])
        records [(byte-array (map byte [1]))
                 (byte-array (map byte [1 2]))]
        bytes (apply avro/binary-encoded schema records)
        thawed (avro/decode-seq schema bytes)]
    ;; Only testing binary, as JSON encoding does *not* round-trip :-(
    (is (= 6 (alength ^bytes bytes)))
    (is (every? (partial instance? (Class/forName "[B")) thawed))
    (is (= (map seq records) (map seq thawed)))))

(deftest test-arrays
  (let [schema (avro/parse-schema {:type :array, :items :long})
        records [[] [0 1] (range 1024)]]
    (is (roundtrips? schema records))))

(deftest test-arrays-primitive
  (let [schema (avro/parse-schema
                {:type 'array, :items 'int, :abracad.array 'ints})
        records [(int-array []) (int-array [0 1]) (int-array (range 1024))]
        thawed-b (apply roundtrip-binary schema records)
        thawed-j (apply roundtrip-json schema records)]
    (is (= (map class records) (map class thawed-b)))
    (is (= (map seq records) (map seq thawed-j)))))

(deftest test-maps
  (let [schema (avro/parse-schema {:type :map, :values :long})
        records [{}
                 {"foo" 0, "bar" 1}
                 (->> (range 1024)
                      (map #(-> [(str %) %]))
                      (into {}))]]
    (is (roundtrips? schema records))))

(deftest test-extra
  (let [schema (avro/parse-schema
                {:name "Example", :type "record",
                 :fields [{:name "foo", :type "long"}]})]
    (is (thrown? clojure.lang.ExceptionInfo
          (roundtrips? schema [{:foo 0}] [{:foo 0, :bar 1}])))
    (is (roundtrips? schema [{:foo 0}] [^{:type 'Example} {:foo 0, :bar 1}]))
    (is (roundtrips? schema [{:foo 0}] [^:avro/unchecked {:foo 0, :bar 1}]))))

(deftest test-positional
  (let [schema (avro/parse-schema
                {:name "Example", :type "record",
                 :abracad.reader "vector",
                 :fields [{:name "left", :type "long"}
                          {:name "right", :type "string"}]}
                ["Example" "string"])
        records [[0 "foo"] [1 "bar"] [2 "baz"] "quux"]
        [record] records
        bytes (apply avro/binary-encoded schema records)
        thawed (avro/decode-seq schema bytes)]
    (is (roundtrips? schema records))
    (is (= ['Example] (map type (roundtrip-binary schema record))))
    (is (= ['Example] (map type (roundtrip-json schema record))))))

(deftest test-mangle-union
  (let [schema (avro/parse-schema
                {:name "mangle-me", :type "record",
                 :abracad.reader "vector"
                 :fields [{:name "field0", :type "long"}]}
                ["mangle-me" "long"])
        records [0 [1] [2] 3 4 [5]]]
    (is (roundtrips? schema records))))

(deftest test-mangle-sub-schema
  (let [schema-def {:name "mangle-me", :type "record",
                    :abracad.reader "vector"
                    :fields [{:name "field0", :type "long"}]}
        schema1 (avro/parse-schema schema-def)
        schema (avro/parse-schema [schema1 "long"])
        records [0 [1] [2] 3 4 [5]]]
    (is (roundtrips? schema records))
    (is (= "mangle_me"
          (-> schema1 avro/unparse-schema :name)))
    (is (thrown? SchemaParseException
          (binding [abracad.avro.util/*mangle-names* false]
            (avro/parse-schema schema-def))))))

(deftest test-mangling
  (let [schema (avro/parse-schema
                 {:name "mangling", :type "record",
                  :fields [{:name "a-dash", :type "long"}]})
        dash-data [{:a-dash 1}]
        under-data [{:a_dash 1}]]
    (is (roundtrips? schema dash-data))
    (is (thrown-with-msg? ExceptionInfo #"Cannot write datum as schema"
          (binding [abracad.avro.util/*mangle-names* false]
            (roundtrips? schema dash-data))))
    (is (thrown-with-msg? ExceptionInfo #"Cannot write datum as schema"
          (roundtrips? schema under-data)))
    (is (binding [abracad.avro.util/*mangle-names* false]
          (roundtrips? schema under-data)))))

(deftest test-sub-types
  (let [schema1 (avro/parse-schema
                 {:name "Example0", :type "record",
                  :abracad.reader "vector"
                  :fields [{:name "field0", :type "long"}]}
                 {:name "Example1", :type "record",
                  :abracad.reader "vector"
                  :fields [{:name "field0", :type "Example0"}]})
        schema (avro/parse-schema schema1 "Example0")
        records [[0] [1] [2] [3] [4] [5]]]
    (is (roundtrips? schema records))))

(deftest test-tuple-schema
  (let [schema1 (avro/tuple-schema ["string" "long" "long"])
        schema2 (avro/tuple-schema ["long" schema1])
        schema (avro/parse-schema schema2)
        records [[0 ["foo" 1 2]] [3 ["bar" 4 5]]]]
    (is (roundtrips? schema records))))

(deftest test-grouping-schema
  (let [schema1 (avro/unparse-schema (avro/tuple-schema ["string" "long"]))
        schema2 (avro/unparse-schema (avro/grouping-schema 2 schema1))
        schema3 (avro/unparse-schema (avro/grouping-schema 1 schema1))]
    (is (= schema1 schema2))
    (is (not= schema1 schema3))
    (is (= "ascending" (get-in schema3 [:fields 0 :order] "ascending")))
    (is (= "ignore" (get-in schema3 [:fields 1 :order] "ascending")))))

(deftest test-spit-slurp
  (let [path "tmp/spit-slurp.avro"
        schema {:type :array, :items 'long}
        records [0 1 2 3 4 5]]
    (io/make-parents path)
    (avro/spit schema path records)
    (is (= records (avro/slurp path)))))

(deftest test-mspit-mslurp
  (let [path "tmp/spit-slurp.avro"
        schema 'long
        records [0 1 2 3 4 5]]
    (io/make-parents path)
    (avro/mspit schema path records)
    (is (= records (avro/mslurp path)))))

(deftest test-data-file-stream
  (let [path "tmp/data-file-stream.avro"
        schema {:type :long}
        records [0 1 2 3 4 5]]
    (io/make-parents path)
    (avro/mspit schema path records)
    (with-open [dfs (avro/data-file-stream (FileInputStream. path))]
      (is (= records (seq dfs))))))
