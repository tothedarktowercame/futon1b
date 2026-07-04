(ns s0-probe
  "Throwaway probe: verify XTQL unnest syntax on a tiny seed.
  Single JVM boot, self-terminating."
  (:require [xtdb.node :as xtn]
            [xtdb.api :as xt]))

(defn -main [& _]
  (with-open [node (xtn/start-node)]
    (xt/execute-tx node [[:put-docs :hx
                          {:xt/id "h1" :hx/type :link/supports :hx/endpoints ["e-1" "e-2"]}
                          {:xt/id "h2" :hx/type :link/opposes   :hx/endpoints ["e-2" "e-3"]}
                          {:xt/id "h3" :hx/type :link/supports :hx/endpoints ["e-1" "e-3" "e-4"]}]])
    ;; --- simple by-type ---
    (prn :by-type-supports
         (xt/q node '(from :hx [{:xt/id hx-id} hx/type]
                        (where (= hx/type :link/supports)))))
    ;; --- endpoint membership via unnest ---
    (prn :endpoint-e1-unnest
         (xt/q node '(from :hx [{:xt/id hx-id} {:hx/endpoints ep}]
                        (unnest ep hx/endpoints)
                        (where (= ep "e-1"))
                        (with hx-id))))
    ;; --- endpoint membership as relation-join attempt ---
    (prn :endpoint-e1-where
         (xt/q node '(from :hx [{:xt/id hx-id} hx/type])))
    (prn :DONE))
  (System/exit 0))
