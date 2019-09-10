(defproject nomnom/abracad "0.4.16"
  :description "De/serialize Clojure data structures with Avro"
  :url "http://github.com/nomnom/abracad"
  :licenses [{:name "Eclipse Public License"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}]
  :global-vars {*warn-on-reflection* true}
  :deploy-repositories {"clojars" {:sign-releases false}
                        "releases" {:url "https://repo.deps.co/nomnom/releases"
                                    :sign-releases false
                                    :username :env/deps_key
                                    :password :env/deps_secret}
                        "snapshots" {:url "https://repo.deps.co/nomnom/snapshots"
                                     :sign-releases false
                                     :username :env/deps_key
                                     :password :env/deps_secret}}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :javac-options ["-target" "1.7" "-source" "1.7"]
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.apache.avro/avro "1.9.0"]
                 [org.xerial.snappy/snappy-java "1.1.7.3"]
                 [cheshire/cheshire "5.9.0"]]
  :plugins [[lein-cloverage "1.0.13" :exclusions [org.clojure/clojure]]])
