(defproject nomnom/abracad "0.4.14"
  :description "De/serialize Clojure data structures with Avro. Public fork, while waiting for upstream"
  :url "http://github.com/damballa/abracad"
  :licenses [{:name "Eclipse Public License"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}]
  :global-vars {*warn-on-reflection* true}
  :deploy-repositories {"clojars" {:sign-releases false}}
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
