(defproject atakeuchii/my-storage "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "https://github.com/atakeuchii/my-storage"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.12.5"]]
  :profiles {:dev {:dependencies [[org.clojure/test.check "1.1.1"]]}
             :bench {:source-paths ["bench"]}}
  :repl-options {:init-ns my-storage.core}

  :repositories [["github"
                  {:url "https://maven.pkg.github.com/atakeuchii/my-storage"
                   :username :env/github_actor
                   :password :env/github_token}]]
  :deploy-repositories [["github"
                         {:url "https://maven.pkg.github.com/atakeuchii/my-storage"
                          :username :env/github_actor
                          :password :env/github_token}]])
