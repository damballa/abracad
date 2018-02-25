(ns abracad.custom-types-test
  (:require [abracad.avro :as avro]
            [matcher-combinators.core :as matcher.core]
            [matcher-combinators.model :as matcher.model])
  (:import [java.net InetAddress]))

(extend-type InetAddress 
  avro/AvroSerializable
  (schema-name [_] "ip.address")
  (field-get [this field]
    (case field
      :address (.getAddress this)))
  (field-list [this] #{:address})

  matcher.core/Matcher
  (match [this ^InetAddress actual]
    (cond
      (= :matcher-combinators.core/missing actual)        [:mismatch (matcher.model/->Missing this)]
      (= (.getHostAddress this) (.getHostAddress actual)) [:match actual]
      :else                                               [:mismatch (matcher.model/->Mismatch this actual)])))

(defn ->InetAddress
  [address] (InetAddress/getByAddress address))
