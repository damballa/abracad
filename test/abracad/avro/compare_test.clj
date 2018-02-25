(ns abracad.avro.compare-test
  (:require [abracad.avro :as avro]
            [matcher-combinators.midje :refer [match]]
            [midje.sweet :refer :all])
  (:import [abracad.avro ClojureData]
           [org.apache.avro Schema]))

(defn compare-elements
  [elem1 elem2 ^Schema schema]
  (.compare (ClojureData/get) elem1 elem2 schema))

(facts "bytes comparison"
  (let [schema          (avro/parse-schema :bytes)
        smaller-bytes   (byte-array (map byte [1 2 3]))
        larger-bytes-v0 (byte-array (map byte [1 2 4]))
        larger-bytes-v1 (byte-array (map byte [1 2 4]))]

    (fact "when the first element that is different in the two bytes is smaller in the first byte-array"
      (compare-elements smaller-bytes larger-bytes-v0 schema) => (match neg?))
    (fact "when the first element that is different in the two bytes is larger in the first byte-array"
      (compare-elements larger-bytes-v0 smaller-bytes schema) => (match pos?))
    (fact "when the byte arrays are equal"
      (compare-elements larger-bytes-v0 larger-bytes-v1 schema) => (match zero?))))

(fact "records comparison"
  (let [schema           (avro/parse-schema {:type   :record, :name "example",
                                             :fields [{:name "foo", :type :string}]})
        smaller-record   {:foo "bar"}
        larger-record-v0 {:foo "baz"}
        larger-record-v1 {:foo "baz"}]

    (fact "when the first element that is different in the two record is smaller in the first record"
      (compare-elements smaller-record larger-record-v0 schema) => (match neg?))
    (fact "when the first element that is different in the two record is larger in the first record"
      (compare-elements larger-record-v0 smaller-record schema) => (match pos?))
    (fact "when the records are equal"
      (compare-elements larger-record-v0 larger-record-v1 schema) => (match zero?))))

(fact "unions comparison"
  (let [schema (avro/parse-schema [:int :string])]
    (fact "if the elements have the same type the comparison is the regular comparison of the type"
      (compare-elements 1 2 schema) => (match neg?)
      (compare-elements "1" "2" schema) => (match neg?))
    (fact "if the elements have different types the types in the left of the union are smaller than the others"
      (compare-elements 2 "1" schema) => (match neg?)
      (compare-elements "1" 2 schema) => (match pos?))))

(fact "enums comparison - the elements in the left of the symbol list are smaller than the others"
  (let [schema (avro/parse-schema {:type    :enum, :name "example"
                                   :symbols [:foo :bar :baz]})]
    (compare-elements :foo :bar schema) => (match neg?)
    (compare-elements :foo :baz schema) => (match neg?)
    (compare-elements :foo :foo schema) => (match zero?)
    (compare-elements :baz :bar schema) => (match pos?)))
