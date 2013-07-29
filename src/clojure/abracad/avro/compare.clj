(ns abracad.avro.compare
  "Generic data comparison implementation."
  {:private true}
  (:refer-clojure :exclude [compare])
  (:require [clojure.core :as cc]
            [abracad.avro :as avro]
            [abracad.avro.write :refer [resolve-union*]]
            [abracad.avro.util :refer [case-enum]])
  (:import [org.apache.avro Schema Schema$Field Schema$Field$Order Schema$Type]
           [org.apache.avro.reflect ReflectData]))

(declare compare)

(defn order-ignore?
  [f] (identical? (.order f) Schema$Field$Order/IGNORE))

(defn order-ascending?
  [f] (identical? (.order f) Schema$Field$Order/ASCENDING))

(defn compare-record
  ^long [x y ^Schema schema]
  (reduce (fn [_ f]
            (if (order-ignore? f)
              0
              (let [schema (.schema f), name (keyword (.name f))
                    x (avro/field-get x name), y (avro/field-get y name)
                    result (compare x y schema)]
                (if (zero? result)
                  0
                  (reduced (if (order-ascending? f)
                             result
                             (- result)))))))
          0 (.getFields schema)))

(defn compare-enum
  ^long [x y ^Schema schema]
  (- (.getEnumOrdinal schema (name x))
     (.getEnumOrdinal schema (name y))))

(defn compare-union
  ^long [x y ^Schema schema]
  (let [ix (resolve-union* schema x)
        iy (resolve-union* schema y)]
    (if (= ix iy)
      (compare x y (-> schema .getTypes (.get ix)))
      (- ix iy))))

(defn compare
  ^long [x y ^Schema schema]
  (if (identical? x y)
    0
    (case-enum (.getType schema)
      Schema$Type/RECORD (compare-record x y schema)
      Schema$Type/ENUM (compare-enum x y schema)
      Schema$Type/UNION (compare-union x y schema)
      #_ else (.compare (ReflectData/get) x y schema))))
