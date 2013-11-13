(ns abracad.avro.edn-test
  (:require [clojure.test :refer :all]
            [abracad.avro :as avro]
            [abracad.avro.edn :as aedn])
  (:import [clojure.lang PersistentQueue]
           [java.net InetAddress]))

;; Stress-test data from nippy
;; - https://github.com/ptaoussanis/nippy
(def stress-data
  {:bytes        (byte-array [(byte 1) (byte 2) (byte 3)])
   :nil          nil
   :boolean      true

   :char-utf8    \ಬ
   :string-utf8  "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ"
   :string-long  (apply str (range 1000))
   :keyword      :keyword
   :keyword-ns   ::keyword

   :queue        (-> (PersistentQueue/EMPTY) (conj :a :b :c :d :e :f :g))
   :queue-empty  (PersistentQueue/EMPTY)
   :sorted-set   (sorted-set 1 2 3 4 5)
   :sorted-map   (sorted-map :b 2 :a 1 :d 4 :c 3)

   :list         (list 1 2 3 4 5 (list 6 7 8 (list 9 10)))
   :list-quoted  '(1 2 3 4 5 (6 7 8 (9 10)))
   :list-empty   (list)
   :vector       [1 2 3 4 5 [6 7 8 [9 10]]]
   :vector-empty []
   :map          {:a 1 :b 2 :c 3 :d {:e 4 :f {:g 5 :h 6 :i 7}}}
   :map-empty    {}
   :set          #{1 2 3 4 5 #{6 7 8 #{9 10}}}
   :set-empty    #{}
   :meta         (with-meta {:a :A} {:metakey :metaval})

   :coll         (repeatedly 1000 rand)

   :byte         (byte 16)
   :short        (short 42)
   :integer      (int 3)
   :long         (long 3)
   :bigint       (bigint 31415926535897932384626433832795)

   :float        (float 3.14)
   :double       (double 3.14)
   :bigdec       (bigdec 3.1415926535897932384626433832795)

   :ratio        22/7

   ;; Clojure 1.4+ tagged literals [TODO]
   ;;:tagged-uuid  (java.util.UUID/randomUUID)
   ;;:tagged-date  (java.util.Date.)
   })

(deftest test-stress
  (let [schema (aedn/new-schema)
        rted-data (->> stress-data
                       (avro/binary-encoded schema)
                       (avro/decode schema))]
    (doseq [kw (disj (-> stress-data keys set) :bytes)]
      (is (= (get stress-data kw) (get rted-data kw))))
    (is (= (-> stress-data :bytes seq) (-> rted-data :bytes seq)))
    (is (= (-> stress-data :meta meta) (-> rted-data :meta meta)))))

(def ip-address-schema
  {:type :record
   :name 'ip.address
   :fields [{:name :address
             :type [{:type :fixed, :name "IPv4", :size 4}
                    {:type :fixed, :name "IPv6", :size 16}]}]})

(extend-type InetAddress
  avro/AvroSerializable
  (schema-name [_] "ip.address")
  (field-get [this field]
    (case field
      :address (.getAddress this)))
  (field-list [this] #{:address}))

(defn ->InetAddress
  [address] (InetAddress/getByAddress address))

(defn run-test-edn-custom
  [schema]
  (binding [avro/*avro-readers*
            , (assoc avro/*avro-readers* 'ip/address #'->InetAddress)]
    (let [schema (aedn/new-schema schema)
          records [{:foo (InetAddress/getByName "8.8.8.8")}
                   [:bar #{(InetAddress/getByName "8::8")}]]
          bytes (apply avro/binary-encoded schema records)
          thawed (avro/decode-seq schema bytes)]
      (is (= records thawed)))))

(deftest test-edn-custom
  (run-test-edn-custom ip-address-schema))

(deftest test-edn-custom-parsed
  (run-test-edn-custom (avro/parse-schema ip-address-schema)))
