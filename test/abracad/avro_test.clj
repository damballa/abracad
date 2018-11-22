(ns abracad.avro-test
  (:require [clojure.test :refer :all]
            [abracad.avro :as avro]
            [clojure.java.io :as io]
            [abracad.avro.conversion :as c])
  (:import [java.io FileInputStream]
           [java.net InetAddress]
           [java.time LocalDate LocalTime Instant]
           [org.apache.avro SchemaParseException]
           [clojure.lang ExceptionInfo]
           (java.util UUID)
           (java.time.temporal ChronoUnit)))

(defn roundtrip-binary
  [schema & records]
  (->> (apply avro/binary-encoded schema records)
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
  (field-list [_] #{:address}))

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

(deftest test-date
  (let [schema         (avro/parse-schema {:type 'int :logicalType :date})
        epoch          (LocalDate/of 1970 1 1)
        today          (LocalDate/now)
        before-epoch   (LocalDate/of 1969 12 31)
        max-date       (LocalDate/of 5881580 7 11)          ;; Date corresponding to MAX_VALUE days since epoch
        after-max      (LocalDate/of 5881580 7 12)
        min-date       (LocalDate/of -5877641 6 23)         ;; Date corresponding to MIN_VALUE days before epoch
        before-min     (LocalDate/of -5877641 6 22)]
    (is (roundtrips? schema [epoch]))
    (is (roundtrips? schema [today]))
    (is (roundtrips? schema [before-epoch]))
    (is (roundtrips? schema [max-date]))
    (is (roundtrips? schema [min-date]))
    (is (thrown? ArithmeticException (roundtrips? schema [after-max])))
    (is (thrown? ArithmeticException (roundtrips? schema [before-min])))
    (testing "An array of dates roundtrips with logical types"
      (let [array-schema (avro/parse-schema {:type :array :items {:type 'int :logicalType :date}})]
        (is (roundtrips? array-schema [[epoch today max-date]]))))))

(deftest test-date-with-logical-types-off
  (binding [abracad.avro.conversion/*use-logical-types* false]
    (let [schema (avro/parse-schema {:type 'int :logicalType :date})
          today (LocalDate/now)]
      (testing "Underlying primitive still roundtrips"
        (is (roundtrips? schema [10])))
      (testing "Logical type fails"
        (is (thrown? ClassCastException (roundtrips? schema [today])))))))

(deftest test-time
  (let [schema                    (avro/parse-schema {:type 'int :logicalType :time-millis})
        midnight                  LocalTime/MIDNIGHT
        now                       (LocalTime/now)
        one-milli-before-midnight (.minus midnight 1 ChronoUnit/MILLIS)]
    (is (roundtrips? schema [midnight]))
    (is (roundtrips? schema [now]))
    (is (roundtrips? schema [one-milli-before-midnight]))))

(deftest test-timestamp-millis
  (let [schema        (avro/parse-schema {:type 'long :logicalType :timestamp-millis})
        epoch         Instant/EPOCH
        now           (Instant/now)
        before-epoch  (Instant/ofEpochMilli -1)
        max-time      (Instant/ofEpochMilli Long/MAX_VALUE)
        after-max     (.plusMillis max-time 1)
        min-time      (Instant/ofEpochMilli Long/MIN_VALUE)
        before-min    (.minusMillis min-time 1)]
    (is (roundtrips? schema [epoch]))
    (is (roundtrips? schema [now]))
    (is (roundtrips? schema [before-epoch]))
    (is (roundtrips? schema [max-time]))
    (is (roundtrips? schema [min-time]))
    (is (thrown? ArithmeticException (roundtrips? schema [after-max])))
    (is (thrown? ArithmeticException (roundtrips? schema [before-min])))))

(deftest test-decimal
  (let [schema        (avro/parse-schema {:type :bytes :logicalType :decimal :scale 6 :precision 12})
        fixed-schema  (avro/parse-schema {:type :fixed :name :foo :size 10 :logicalType :decimal :scale 6 :precision 12})]
    (is (roundtrips? schema [(.setScale 5M 6)]))
    (is (roundtrips? fixed-schema [(.setScale 5M 6)]))
    (is (roundtrips? schema [5.12345M]))                       ;; Scale too small
    (is (roundtrips? fixed-schema [5.12345M]))
    (is (thrown? ArithmeticException (roundtrips? schema [5.123456789M])))                   ;; Scale too big
    (is (thrown? ArithmeticException (roundtrips? fixed-schema [5.123456789M])))
    (is (roundtrips? schema [(bigdec 1234567890123.123456)]))
    (is (roundtrips? fixed-schema [(bigdec 1234567890123.123456)]))))

(deftest test-decimal-with-rounding
  (with-precision 8 :rounding HALF_UP
                    (let [schema (avro/parse-schema {:type :bytes :logicalType :decimal :scale 6 :precision 8})
                          fixed-schema (avro/parse-schema {:type :fixed :name :foo :size 10 :logicalType :decimal :scale 6 :precision 8})]
                      (is (roundtrips? schema [5M]))
                      (is (roundtrips? fixed-schema [5M]))
                      (is (roundtrips? schema [5.12345M]))
                      (is (roundtrips? fixed-schema [5.12345M]))
                      (is (roundtrips? schema [5.123457M 5.123456M] [5.1234565M 5.12345649M])) ;; Rounded [half up, down]
                      (is (roundtrips? fixed-schema [5.123457M 5.123456M] [5.1234565M 5.12345649M]))))) ;; Rounded [half up, down]

(deftest test-uuid
  (let [schema      (avro/parse-schema {:type 'string :logicalType :uuid})
        uuid        (UUID/randomUUID)
        stringUUID  "a7b168ce-d4ff-49a2-a7a5-e65ac06dbe67"]
    (is (roundtrips? schema [uuid]))
    (is (roundtrips? schema [(UUID/fromString stringUUID)] [stringUUID]))))

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

(deftest test-keyword-string
  (let [schema (avro/parse-schema {:type 'string :clojureType :keyword})]
    (is (roundtrips? schema [:foo]))
    (is (roundtrips? schema [:bar]))))

(deftest test-with-logical-types
  (let [schema (avro/parse-schema
                 {:type      :record
                  :name      :Person
                  :namespace "com.test"
                  :fields    [{:name :message-timestamp :type {:type 'long :logicalType :timestamp-millis}}
                              {:name :firstName :type 'string}
                              {:name :lastName :type 'string}
                              {:name :dateOfBirth :type {:type 'int :logicalType :date}}
                              {:name :height :type {:type :bytes :logicalType :decimal :scale 2 :precision 12}}
                              {:name :candles :type [{:type 'int}
                                                     {:type 'string :clojureType :keyword}]}]})
        records [{:message-timestamp (Instant/now)
                  :firstName   "Ronnie",
                  :lastName    "Corbet",
                  :dateOfBirth (LocalDate/of 1930 12 4)
                  :height      1.55M
                  :candles     4}
                 {:message-timestamp (Instant/ofEpochMilli 1234567890)
                  :firstName         "Ronnie",
                  :lastName          "Barker",
                  :dateOfBirth       (LocalDate/of 1929 9 25)
                  :height            1.72M
                  :candles :fork}]]
    (is (roundtrips? schema records))))
