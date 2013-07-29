(defproject com.damballa/abracad "0.3.1-SNAPSHOT"
  :description "De/serialize Clojure data structures with Avro."
  :url "http://github.com/damballa/abracad"
  :licenses [{:name "Eclipse Public License"
              :url "http://www.eclipse.org/legal/epl-v10.html"}
             {:name "Apache License, Version 2.0"
              :url "http://www.apache.org/licenses/LICENSE-2.0.html"}]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.avro/avro "1.7.4"]
                 [cheshire/cheshire "5.2.0"]])
