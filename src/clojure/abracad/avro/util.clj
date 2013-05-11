(ns abracad.avro.util
  (:import [org.apache.avro Schema$Field]))

(defmacro case-enum
  "Like `case`, but explicitly dispatch an Java enums."
  [e & clauses]
  `(case (.ordinal ~e)
     ~@(concat
        (mapcat (fn [[test result]]
                  [(eval `(let [test# (.ordinal ~test)] test#)) result])
                (partition 2 clauses))
        (when (odd? (count clauses))
          (list (last clauses))))))

(defn mangle
  "Perform reversible Clojure->Avro name-mangling."
  [^String n] (.replace n \- \_))

(defn unmangle
  "Reverse Clojure->Avro name-mangling."
  [^String n] (.replace n \_ \-))

(defn field-keyword
  "Keyword for Avro schema field."
  [^Schema$Field f] (-> f .name unmangle keyword))
