# abracad

Abracad is a Clojure library for de/serializing Clojure data
structures with Avro, leveraging the Java Avro implementation.

## Installation

Abracad is available on Clojars.  Add this `:dependency` to your
Leiningen `project.clj`:

```clj
[com.damballa/abracad "0.1.0"]
```

## Usage

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
  - Arrays currently always deserialize as vectors
  - Maps deserialize as maps
  - Fixed values currently deserialize as primitive byte arrays
  - Bytes values deserialize as NIO ByteBuffers
  - Records deserialize as maps with keyword field names and `:type`
    metadata indicating the Avro schema name

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

(defn map->InetAddress
  [{:keys [address]}] (InetAddress/getByAddress address))

(def schema
  (avro/parse-schema
   {:type :record
    :name 'ip.address
    :fields [{:name :address
              :type [{:type :fixed, :name "IPv4", :size 4}
                     {:type :fixed, :name "IPv6", :size 16}]}]}))

(binding [avro/*avro-readers* {'ip/address #'map->InetAddress}]
  (with-open [adf (avro/data-file-writer schema "example.avro")]
    (.append adf (InetAddress/getByName "8.8.8.8"))
    (.append adf (InetAddress/getByName "8::8")))

  (with-open [adf (avro/data-file-reader "example.avro")]
    (doall (seq adf))))
;;=> (#<Inet4Address /8.8.8.8> #<Inet6Address /8:0:0:0:0:0:0:8>)
```

## TODO

These are the early days.  Still to be done:

  - Kick the tires on the interface.  There may be glaring holes.
  - Write more exhaustive tests, to cover the full range of types.
  - Dynamically generate schema-specific datum reader/writer
    implementations.  All the speed of generating & compiling
    de/serialization classes from schemas, but with none of the
    ahead-of-time hassle.
  - Hadoop serialization integration.  The Avro datum reader/writer
    implementation used by the Avro Hadoop serialization is not
    currently configurable, which prevents direct Clojure interop via
    the custom Datum reader/writers.

## License

Copyright © 2013 Damballa Inc.

Distributed under the Eclipse Public License, the same as Clojure.
