(defproject com.damballa/abracad "0.1.0-SNAPSHOT"
  :description "De/serialize Clojure data structures with Avro."
  :url "http://github.com/damballa/abracad"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.apache.avro/avro "1.7.4"]
                 [cheshire/cheshire "5.1.1"]])
