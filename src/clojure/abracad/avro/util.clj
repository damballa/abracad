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

(defmacro if-not-let
  "Like `if-let`, but with order of `then` and `else` swapped."
  [bindings then else]
  `(if-let ~bindings ~else ~then))

(defn ^:private coerce*
  "Type-hinted form for inline `coerce`."
  [c f x]
  (let [y (vary-meta (gensym) assoc :tag c)]
    `(let [x# ~x, ~y (if (instance? ~c x#) x# (~f x#))]
       ~y)))

(defn coerce
  "Coerce `x` to be of class `c` by applying `f` to it iff `x` isn't
already an instance of `c`."
  {:inline (identity coerce*), :inline-arities #{3}}
  [c f x] (if (instance? c x) x (f x)))
