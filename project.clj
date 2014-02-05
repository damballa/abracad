(defproject com.damballa/abracad "0.4.10-SNAPSHOT"
  :description "De/serialize Clojure data structures with Avro."
  :url "http://github.com/damballa/abracad"
  :licenses [{:name "Eclipse Public License"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}]
  :global-vars {*warn-on-reflection* true}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.avro/avro "1.7.5"]
                 [cheshire/cheshire "5.2.0"]]
  :plugins [[codox/codox "0.6.4"]]
  :codox {:include [abracad.avro abracad.avro.edn]}
  :aliases {"test-all" ["with-profile" ~(str "clojure-1-4:"
                                             "clojure-1-5:"
                                             "clojure-1-6")
                        ,              "test"]}
  :profiles {:clojure-1-4 {:dependencies
                           [[org.clojure/clojure "1.4.0"]]}
             :clojure-1-5 {:dependencies
                           [[org.clojure/clojure "1.5.1"]]}
             :clojure-1-6 {:dependencies
                           [[org.clojure/clojure "1.6.0-alpha3"]]}})
