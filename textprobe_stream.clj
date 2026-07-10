;; textprobe_stream.clj — shared streaming EDN reader for the M-text-sidecar
;; probes (loaded via load-file by textprobe_*.clj). Reads a file whose
;; top-level form is a vector of maps, one doc at a time — the evidence
;; export is 150M, so the probes never materialize the whole vector.

(require '[clojure.edn :as edn]
         '[clojure.java.io :as io])

(def edn-readers
  ;; Mirror the export's tagged literals (migration finding F2); the probes
  ;; treat timestamps as opaque.
  {'futon1a/instant identity})

(defn skip-ws
  "Consume EDN whitespace (incl. commas); return the next significant char
  as an int, or -1 at EOF."
  [^java.io.PushbackReader rdr]
  (loop []
    (let [c (.read rdr)]
      (if (and (pos? c)
               (or (Character/isWhitespace (char c)) (= c (int \,))))
        (recur)
        c))))

(defn stream-docs
  "Call (f doc) for each element of the top-level vector in `path` without
  materializing the vector. Returns the doc count."
  [path f]
  (with-open [rdr (java.io.PushbackReader. (io/reader path))]
    (let [c (skip-ws rdr)]
      (when-not (= c (int \[))
        (throw (ex-info "expected top-level [" {:path path :char c}))))
    (loop [n 0]
      (let [c (skip-ws rdr)]
        (if (or (neg? c) (= c (int \])))
          n
          (do (.unread rdr c)
              (f (edn/read {:readers edn-readers :default (fn [_ v] v)} rdr))
              (recur (inc n))))))))
