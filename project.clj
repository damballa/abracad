(defproject nubank/abracad "0.4.16"
  :description "De/serialize Clojure data structures with Avro."
  :url "http://github.com/damballa/abracad"
  :licenses [{:name "Eclipse Public License"
              :url  "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url  "http://www.apache.org/licenses/LICENSE-2.0.html"}]

  :plugins [[codox/codox "0.6.4"]
            [lein-midje "3.2.1"]
            [lein-cloverage "1.0.10"]
            [lein-vanity "0.2.0"]]

  :repositories [["central" {:url "https://repo1.maven.org/maven2/" :snapshots false}]
                 ["clojars" {:url "https://clojars.org/repo/"}]]

  :global-vars {*warn-on-reflection* true}

  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.apache.avro/avro "1.8.2"]
                 [cheshire/cheshire "5.6.1"]]

  :codox {:include [abracad.avro abracad.avro.edn]}

  :aliases {"test-all" ["with-profile" ~(str "clojure-1-7:"
                                             "clojure-1-8:"
                                             "clojure-1-9")
                        "midje"]
            "coverage" ["cloverage" "-s" "coverage"]
            "loc"      ["vanity"]}

  :profiles {:dev         {:dependencies
                           [[midje "1.9.1" :exclusions [org.clojure/clojure]]
                            [nubank/matcher-combinators "0.2.1"]]}
             :clojure-1-7 {:dependencies
                           [[org.clojure/clojure "1.7.0"]
                            [midje "1.9.1" :exclusions [org.clojure/clojure]]
                            [nubank/matcher-combinators "0.2.1"]]}
             :clojure-1-8 {:dependencies
                           [[org.clojure/clojure "1.8.0"]
                            [midje "1.9.1" :exclusions [org.clojure/clojure]]
                            [nubank/matcher-combinators "0.2.1"]]}
             :clojure-1-9 {:dependencies
                           [[org.clojure/clojure "1.9.0"]
                            [midje "1.9.1" :exclusions [org.clojure/clojure]]
                            [nubank/matcher-combinators "0.2.1"]]}}

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :test-paths ["test/"]

  :javac-options ["-target" "1.8" "-source" "1.8"])
