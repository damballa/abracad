# abracad

[![Build Status](https://secure.travis-ci.org/damballa/abracad.png)](http://travis-ci.org/damballa/abracad)

Abracad is a Clojure library for de/serializing Clojure data
structures with Avro, leveraging the Java Avro implementation.

Abracad supports: a generic mapping between Avro and Clojure data for
arbitrary schemas; customized protocol-based mappings between Avro
records and any JVM types; and “schema-less” EDN-in-Avro serialization
of arbitrary Clojure data.

## Installation

Abracad is available on Clojars.  Add this `:dependency` to your
Leiningen `project.clj`:

```clj
[com.damballa/abracad "0.4.13"]
```

## Usage

Example usage follows; [detailed API documentation][api] available,
generated via [codox][codox].

[api]: http://damballa.github.io/abracad/
[codox]: https://github.com/weavejester/codox

### Schemas

Avro schemas may be parsed from JSON (from either strings or input
streams), from the Clojure data representation of a JSON schema, or
from existing Avro Schema objects.

```clj
(require '[abracad.avro :as avro])

(def schema
  (avro/parse-schema
   {:type :record
    :name "LongList"
    :aliases ["LinkedLongs"]
    :fields [{:name "value", :type :long}
             {:name "next", :type ["LongList", :null]}]}))
```

The `parse-schema` function may be passed multiple schemas, in which
case later schemas may reference types defined in earlier schemas.
The result is the schema generated from the final argument.

### Basic de/serialization

Abracad provides functions which act as a thin layer over the Java
Avro interface, plus Clojure generic datum reader and writer
implementations which allow Clojure data structures to be directly
de/serialized.

```clj
(with-open [adf (avro/data-file-writer "snappy" schema "example.avro")]
  (.append adf {:value 0, :next nil})
  (.append adf {:value 8, :next {:value 16, :next nil}}))

(with-open [adf (avro/data-file-reader "example.avro")]
  (doall (seq adf)))
;;=> ({:value 0, :next nil} {:value 8, :next {:value 16, :next nil}})
```

The Avro type deserialization mappings are as follows:

  - Numeric primitives deserialize as their Java counterparts
  - Strings currently always deserialize as Strings
  - Enums deserialize as keywords
  - Arrays currently always deserialize as persistent vectors
  - Maps deserialize as persistent maps
  - Fixed values currently always deserialize as primitive byte arrays
  - Bytes values currently always deserialize as primitive byte arrays
  - Records deserialize as maps with keyword field names and `:type` metadata
    indicating the Avro schema name

The Avro specification allows field names to contain the `_` character but
disallows the `-` character.  Clojure keywords frequently contain `-` but rarely
contain `_`.  Abracad attempts to work around this difference by mapping `_` in
Avro field names to `-` in Clojure symbols and vice-versa.  The current
implementation of this conversion does *not* handle keywords containing `_`
instead, which is probably a bug.  This mangling may be disabled by binding
`abracad.avro.util/*mangle-names*` to `false`.

#### Record de/serialization tweaking

In addition to the generic map de/serialization, records may also be
generically de/serialized as vectors.  During serialization, whenever
a record is expected and a vector is encountered, the vector will be
serialized by matching fields by position, so long as the expected and
provided numbers of fields match.

During deserialization, a record schema with the annotation
`:abracad.reader` set to `"vector"` will be deserialized as a vector,
with fields encoded by position.

```clj
(let [schema (avro/parse-schema
              {:name "example", :type "record",
               :fields [{:name "left", :type "string"}
                        {:name "right", :type "long"}]
               :abracad.reader "vector"})]
  (->> ["foo" 31337]
       (avro/binary-encoded schema)
       (avro/decode schema)))
;;=> ["foo" 31337"]
```

Maps serialized as records will be checked to ensure that they do not
have any extra entries not encoded by the schema, raising an exception
if extra entries are present.  This check may be avoided for
individual records by including `:type` metadata matching the schema.
The check may be en/disabled recursively for a record and all
contained records via the `:abracad.avro/unchecked` metadata.

### Custom record de/serialization

During union and record serialization, Abracad uses a protocol to
determine an object's Avro schema name and to access its fields.
During deserialization, Abracad uses a facility directly analogous to
the Clojure Reader `*data-readers*` facility to find custom
deserialization constructor functions.  These may be used to extend
Avro de/serialization to arbitrary existing types.

```clj
(import 'java.net.InetAddress)

(extend-type InetAddress
  avro/AvroSerializable
  (schema-name [_] "ip.address")
  (field-get [this field] (case field :address (.getAddress this)))
  (field-list [this] #{:address}))

(defn ->InetAddress
  [address] (InetAddress/getByAddress address))

(def schema
  (avro/parse-schema
   {:type :record
    :name 'ip.address
    :fields [{:name :address
              :type [{:type :fixed, :name "IPv4", :size 4}
                     {:type :fixed, :name "IPv6", :size 16}]}]}))

(binding [avro/*avro-readers* {'ip/address #'->InetAddress}]
  (with-open [adf (avro/data-file-writer schema "example.avro")]
    (.append adf (InetAddress/getByName "8.8.8.8"))
    (.append adf (InetAddress/getByName "8::8")))

  (with-open [adf (avro/data-file-reader "example.avro")]
    (doall (seq adf))))
;;=> (#<Inet4Address /8.8.8.8> #<Inet6Address /8:0:0:0:0:0:0:8>)
```

### EDN-in-Avro

Abracad supports expressing EDN data structures as Avro records in the
`abracad.avro.edn` Avro namespace.  The `new-schema` function in the
same-named Clojure namespace returns schemas which express a superset
of EDN capturing most commonly-used Clojure constructs.  These allow
using Avro for Clojure data without pre-defining application-specific
schemas.

```clj
(require '[abracad.avro.edn :as aedn])

(def schema (aedn/new-schema))

(->> {:foo ['bar "baz" 1337]}
     (avro/binary-encoded schema)
     (avro/decode schema))
;;=> {:foo [bar "baz" 1337]}
```

### Hadoop MapReduce integration

Avro 1.7.5 and later supports configurable “data models” for datum
reading, writing, and comparison in Hadoop MapReduce jobs.  Abracad
0.4.0 and later provides a `ClojureData` class which can be passed to
the `AvroJob/setDataModelClass` static method in order to map job Avro
input and output directly to and from Clojure data structures.

## TODO

These are the early days.  Still to be done:

  - Kick the tires on the interface.  There may be glaring holes.
  - Write more exhaustive tests, to cover the full range of types.
  - Figure out a cleaner way of handling `_` vs `-`.
  - Dynamically generate schema-specific datum reader/writer
    implementations.  All the speed of generating & compiling
    de/serialization classes from schemas, but with none of the
    ahead-of-time hassle.

## License

Copyright © 2013-2015 Damballa Inc. and contributors.

Distributed under your choice of the Eclipse Public License or the
Apache License, Version 2.0.
