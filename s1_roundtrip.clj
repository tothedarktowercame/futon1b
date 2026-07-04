(ns s1-roundtrip
  "S1 scaffold: start an in-process XTDB 2 node, round-trip one document.
  Self-terminating — the node is closed at the end. No durable state."
  (:require [xtdb.api :as xtdb])
  (:import (java.nio.file Files)))

(defn- temp-dir [prefix]
  (-> (Files/createTempDirectory prefix (make-array java.nio.file.attribute.FileAttribute 0))
      (.toFile)
      (.getAbsolutePath)))

(defn -main [& _args]
  (let [dir (temp-dir "futon1b-s1-")]
    (prn :s1/start dir)
    (let [node (xtdb/start-node {})]
      (try
        (let [doc {:xt/id "doc/roundtrip-1"
                   :greeting "hello from futon1b"
                   :hx/type :test/roundtrip}]
          (prn :s1/submit doc)
          @(xtdb/submit-tx node [[:put-docs :docs doc]])
          (xtdb/sync node)
          (let [db  (xtdb/open-db node)
                got (xtdb/entity db (:xt/id doc))]
            (prn :s1/read-back got)
            (assert (= doc got) :s1/roundtrip-mismatch)
            (prn :s1/OK)))
        (finally
          (.close node)
          (prn :s1/node-closed))))))

(System/exit 0)
