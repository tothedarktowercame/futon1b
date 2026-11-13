(ns scripts.export-workplan
  "Export committed records from an OFFLINE copy, never a serving store.
   Copy log AND storage while stable; preserve a before/after hash manifest."
  (:require [clojure.java.io :as io]
            [futon1b-arxana :as arx]
            [futon1b-xt :as fxt]
            [xtdb.api :as xt]
            [xtdb.node :as xtn]))

(defn export! [offline-root output]
  (let [root (.getCanonicalPath (io/file offline-root))
        live (.getCanonicalPath (io/file (System/getProperty "user.home") ".local/share/futon4a/workplan-demo"))]
    (when (or (= root live) (.startsWith root (str live "/")))
      (throw (ex-info "Refusing to open the live workplan store" {:root root})))
    (doseq [part ["log" "storage"]]
      (when-not (.isDirectory (io/file root part))
        (throw (ex-info "Offline copy requires both log and storage" {:part part}))))
    (with-open [node (xtn/start-node {:log [:local {:path (str root "/log")}]
                                    :storage [:local {:path (str root "/storage")}]})]
      (let [deadline (+ (System/currentTimeMillis) 600000)]
        (loop []
          (let [status (xt/status node)]
            (when-not (= (:latest-submitted-msg-ids status) (:latest-processed-msg-ids status))
              (when (> (System/currentTimeMillis) deadline)
                (throw (ex-info "Offline replay timeout" {:status status})))
              (Thread/sleep 250) (recur)))))
      (let [tables [:arxana_clusions :arxana_evidence :arxana_assessments :arxana_attempts]
            rows (vec (mapcat
                       (fn [table]
                         (map #(assoc (dissoc % :xt/valid-from :xt/system-from)
                                      :export/table table
                                      :export/valid-from (str (:xt/valid-from %))
                                      :export/system-from (str (:xt/system-from %)))
                              (fxt/safe-q node [(str "SELECT *, _valid_from, _system_from FROM " (name table))])))
                       tables))
            result {:status (str (xt/status node)) :source-sequence (arx/source-sequence node) :rows rows}]
        (spit output (pr-str result))
        {:rows (count rows) :tables (frequencies (map :export/table rows))
         :source-sequence (:source-sequence result)}))))
