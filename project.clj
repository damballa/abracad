(defproject nubank/abracad "0.4.18"
  :description "De/serialize Clojure data structures with Avro."
  :url "http://github.com/nubank/abracad"
  :licenses [{:name "Eclipse Public License"
              :url  "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url  "http://www.apache.org/licenses/LICENSE-2.0.html"}]

  :plugins [[codox/codox "0.10.7"]
            [lein-midje "3.2.1"]
            [lein-cloverage "1.1.2"]
            [lein-vanity "0.2.0"]]

  :repositories [["central" {:url "https://repo1.maven.org/maven2/" :snapshots false}]
                 ["clojars" {:url "https://clojars.org/repo/"}]]

  :global-vars {*warn-on-reflection* true}

  :dependencies [[org.apache.avro/avro "1.10.1"]
                 [cheshire/cheshire "5.9.0"]]

  :codox {:include [abracad.avro abracad.avro.edn]}

  :aliases {"test-all" ["with-profile" ~(str "clojure-1-7:"
                                             "clojure-1-8:"
                                             "clojure-1-9:"
                                             "clojure-1-10")
                        "midje"]
            "coverage" ["cloverage" "-s" "coverage"]
            "loc"      ["vanity"]}

  :profiles {:dev         {:dependencies
                           [[midje "1.9.9" :exclusions [org.clojure/clojure]]
                            [org.xerial.snappy/snappy-java "1.1.7.3"]
                            [nubank/matcher-combinators "1.2.1"]]}
             :clojure-1-7 {:dependencies
                           [[org.clojure/clojure "1.7.0"]
                            [midje "1.9.9" :exclusions [org.clojure/clojure]]
                            [org.xerial.snappy/snappy-java "1.1.7.3"]
                            [nubank/matcher-combinators "0.2.1"]]}
             :clojure-1-8 {:dependencies
                           [[org.clojure/clojure "1.8.0"]
                            [midje "1.9.9" :exclusions [org.clojure/clojure]]
                            [org.xerial.snappy/snappy-java "1.1.7.3"]
                            [nubank/matcher-combinators "0.2.1"]]}
             :clojure-1-9 {:dependencies
                           [[org.clojure/clojure "1.9.0"]
                            [midje "1.9.9" :exclusions [org.clojure/clojure]]
                            [org.xerial.snappy/snappy-java "1.1.7.3"]
                            [nubank/matcher-combinators "1.2.1"]]}
             :clojure-1-10 {:dependencies
                           [[org.clojure/clojure "1.10.1"]
                            [midje "1.9.9" :exclusions [org.clojure/clojure]]
                            [org.xerial.snappy/snappy-java "1.1.7.3"]
                            [nubank/matcher-combinators "1.2.1"]]}}

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :test-paths ["test/"]

  :javac-options ["-target" "1.8" "-source" "1.8"])
