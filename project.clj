(defproject com.damballa/abracad "0.4.14-snapshot"
  :description "de/serialize clojure data structures with avro."
  :url "http://github.com/damballa/abracad"
  :licenses [{:name "eclipse public license"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "apache license, version 2.0"
              :url "http://www.apache.org/licenses/license-2.0.html"}]
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.7" "-source" "1.7"]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.apache.avro/avro "1.9.0"]
                 [org.xerial.snappy/snappy-java "1.1.7.3"]
                 [cheshire/cheshire "5.9.0"]]
  :plugins [[codox/codox "0.6.4"]]
  :codox {:include [abracad.avro abracad.avro.edn]}
  :aliases {"test-all" ["with-profile" ~(str "clojure-1-6:"
                                             "clojure-1-7:"
                                             "clojure-1-8"
                                             "clojure-1-9")
                        "test"]}
  :profiles {:clojure-1-6 {:dependencies
                           [[org.clojure/clojure "1.6.0"]]}
             :clojure-1-7 {:dependencies
                           [[org.clojure/clojure "1.7.0"]]}
             :clojure-1-8 {:dependencies
                           [[org.clojure/clojure "1.8.0"]]}
             :clojure-1-9 {:dependencies
                           [[org.clojure/clojure "1.9.0"]]}})
