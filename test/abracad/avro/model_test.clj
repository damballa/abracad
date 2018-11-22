(ns abracad.avro.model-test
  (:require [clojure.test :refer :all]
            [abracad.avro :as avro])
  (:import [org.apache.avro Schema]))

(defn ac-compare
  [x y ^Schema schema]
  (avro/compare schema x y ))

(deftest test-compare-bytes
  (let [schema (avro/parse-schema :bytes)
        b1 (byte-array (map byte [1 2 3]))
        b2 (byte-array (map byte [1 2 4]))
        b3 (byte-array (map byte [1 2 4]))]
    (is (neg? (ac-compare b1 b2 schema)))
    (is (pos? (ac-compare b2 b1 schema)))
    (is (zero? (ac-compare b2 b3 schema)))))

(deftest test-compare-records
  (let [schema (avro/parse-schema
                {:type :record, :name "example",
                 :fields [{:name "foo", :type :string}]})
        r1 {:foo "bar"}, r2 {:foo "baz"}, r3 {:foo "baz"}]
    (is (neg? (ac-compare r1 r2 schema)))
    (is (pos? (ac-compare r2 r1 schema)))
    (is (zero? (ac-compare r2 r3 schema)))))

(deftest test-compare-unions
  (let [schema (avro/parse-schema [:int :string])]
    (is (neg? (ac-compare 2 "1" schema)))
    (is (pos? (ac-compare "1" 2 schema)))
    (is (neg? (ac-compare 1 2 schema)))
    (is (neg? (ac-compare "1" "2" schema)))))

(deftest test-compare-enums
  (let [schema (avro/parse-schema
                {:type :enum, :name "example",
                 :symbols [:foo :bar :baz]})]
    (is (neg? (ac-compare :foo :bar schema)))
    (is (neg? (ac-compare :foo :baz schema)))
    (is (zero? (ac-compare :foo :foo schema)))
    (is (pos? (ac-compare :baz :bar schema)))))
