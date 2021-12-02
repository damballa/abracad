(defproject nomnom/abracad "0.5.2"
  :description "De/serialize Clojure data structures with Avro"
  :url "http://github.com/nomnom/abracad"
  :licenses [{:name "Eclipse Public License"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}]

  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.apache.avro/avro "1.11.0"]
                 [org.xerial.snappy/snappy-java "1.1.8.4"]
                 [cheshire/cheshire "5.10.1"]]

  :global-vars {*warn-on-reflection* true}

  :deploy-repositories {"clojars" {:sign-releases false}}

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.7" "-source" "1.7"]

  :profiles {:dev {:resource-paths ["dev-resources"]}})
