(ns abracad.avro.util
  "Internal utility functions."
  {:private true}
  (:import [org.apache.avro Schema$Field]))

(defmacro returning
  "Evaluates the result of expr, then evaluates the forms in body (presumably
for side-effects), then returns the result of expr."
  [expr & body] `(let [value# ~expr] ~@body value#))

(defmacro case-expr
  "Like case, but only supports individual test expressions, which are
evaluated at macro-expansion time."
  [e & clauses]
  `(case ~e
     ~@(concat
        (mapcat (fn [[test result]]
                  [(eval `(let [test# ~test] test#)) result])
                (partition 2 clauses))
        (when (odd? (count clauses))
          (list (last clauses))))))

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
