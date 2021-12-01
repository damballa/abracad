(ns abracad.avro.codec-test
  (:require
    [abracad.avro.codec :as codec]
    [abracad.io :as io]
    [clojure.test :refer [deftest is testing]]))


(def schema-data
  {:type "record"
   :name "Example"
   :fields [{:type "long"
             :name "count"}
            {:type "string"
             :name "codename"}]})


(deftest full-cycle
  (let [sample {:count 1
                :codename "bananas"}
        encoded (codec/->avro-base64 schema-data sample)]
    (is (bytes? encoded))
    (is (= sample
           (codec/avro-base64-> schema-data encoded)))))


(deftest embedded-schemas
  (let [convo-schema-json (io/read-json-resource "data/conversation.avsc")
        author-schema-json (io/read-json-resource "data/author.avsc")
        message-schema-json (io/read-json-resource "data/message.avsc")
        notice-schema-json (io/read-json-resource "data/notice.avsc")
        full-schema (codec/parse-schema*
                      author-schema-json
                      message-schema-json
                      notice-schema-json
                      convo-schema-json)
        author-schema (codec/parse-schema* author-schema-json)
        msg-schema (codec/parse-schema*
                     author-schema-json
                     message-schema-json)]
    (testing "individual sub-schemas"
      (is (= {:username "foo" :email "lol"}
             (codec/avro-> author-schema
                           (codec/->avro author-schema {:username "foo" :email "lol"}))))

      (is (= {:body "oh" :author {:username "ok" :email "not@valid"}}
             (codec/avro-> msg-schema
                           (codec/->avro msg-schema
                                         {:body "oh" :author {:username "ok" :email "not@valid"}})))))
    (testing "empty conversation"
      (let [sample {:title "no story"
                    :lines []}]
        (is (= sample
               (codec/avro-> full-schema (codec/->avro full-schema sample))))))
    (testing "embeded example"
      (let [sample {:title "a story"
                    :lines [{:body "oh"
                             :author {:username "foo"
                                      :email "lol@example.com"}}
                            {:author {:username "bar" :email "test@example.com"}
                             :body "no"}
                            {:text "has been assigned to Lol"}
                            {:author {:username "lol" :email "lol@example.com"}
                             :body "oh hi"}]}]
        (is (= sample
               (codec/avro-> full-schema (codec/->avro full-schema sample))))))))
