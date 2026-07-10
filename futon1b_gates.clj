;; futon1b-gates — the A2 slice of the operational switchover
;; (E-futon1b-operational-switchover, futon2/holes): the write-path gate
;; machinery ported from futon1a per API-CONTRACT.md §1.
;;
;; Ported here: the layered error envelope (L4=400 L3=403 L2=500 L1=409
;; L0=503), penholder resolution + allow-list (L3), and the canonical-id
;; gate (L4) with the :mission/doc contract seeded at boot (mirrors
;; futon1a.scripts.seed-futon1-descriptors — owner claude-2, E-futon1a-
;; archivist; the id lives in :entity/name, not :entity/id).
;;
;; NOT ported (v1): proof paths (:path/id), expansion gate, identity L1
;; machinery — no caller on the switchover path uses them.
(ns futon1b-gates
  (:require [clojure.string :as str]))

;; ---------------------------------------------------------------------------
;; Layered error envelope.
;; ---------------------------------------------------------------------------

(def layer->status
  {4 400, 3 403, 2 500, 1 409, 0 503})

(defn layered-error
  "Throwable carrying futon1a's uniform error envelope:
  {:error {:layer N :reason kw :context map}}."
  [layer reason context]
  (ex-info (str (name reason))
           {:error {:layer layer :reason reason :context context}}))

(defn error->response
  "Map a caught Throwable to [status body]. Layered ex-infos keep their
  envelope and status; anything else is futon1a's generic 500."
  [^Throwable e]
  (let [err (:error (ex-data e))]
    (if (and (map? err) (contains? layer->status (:layer err)))
      [(layer->status (:layer err)) {:error err}]
      [500 {:error {:reason :exception :message (.getMessage e)}}])))

;; ---------------------------------------------------------------------------
;; L3 — penholder.
;; ---------------------------------------------------------------------------

(defn- parse-allowed [s]
  (->> (str/split (or s "") #",")
       (map str/trim)
       (remove str/blank?)
       set))

(def allowed-penholders
  "Same default as the futon3c bootstrap's FUTON1A_ALLOWED_PENHOLDERS."
  (let [env (System/getenv "FUTON1B_ALLOWED_PENHOLDERS")]
    (if (str/blank? env) #{"api" "joe"} (parse-allowed env))))

(def compat-penholder-default
  "Fallback penholder for headerless/bodyless writes (futon1a:
  FUTON1A_COMPAT_PENHOLDER; nil when unset — such writes then 403)."
  (some-> (System/getenv "FUTON1B_COMPAT_PENHOLDER") str/trim not-empty))

(defn resolve-penholder
  "futon1a compat-penholder order (app.clj:176-182): body :penholder →
  x-penholder header → server default."
  [body header-val]
  (or (some-> (:penholder body) str str/trim not-empty)
      (some-> header-val str/trim not-empty)
      compat-penholder-default))

(defn authorize!
  "L3: penholder must be a non-blank string in the allow-list
  (futon1a.auth.penholder/authorize!). Returns the penholder."
  [penholder]
  (cond
    (or (nil? penholder) (str/blank? (str penholder)))
    (throw (layered-error 3 :missing-penholder {:allowed allowed-penholders}))

    (not (contains? allowed-penholders (str penholder)))
    (throw (layered-error 3 :forbidden {:penholder (str penholder)
                                        :allowed allowed-penholders}))

    :else (str penholder)))

;; ---------------------------------------------------------------------------
;; L4 — canonical-id gate (model registry + gate queue).
;; ---------------------------------------------------------------------------

(defonce !registry (atom {}))
(defonce !gate-queue (atom []))

(defn register-model! [{:keys [id descriptor]}]
  (swap! !registry assoc id descriptor))

(defn get-model [model-id]
  (get @!registry model-id))

(defn gate-queue-record! [entry]
  (swap! !gate-queue conj (assoc entry :at (str (java.time.Instant/now)))))

(defn- classify-id
  "futon1a.ingest.open-world/classify-id: :id-pattern match → :accept;
  any :queue regex match → :queue; else :reject. No :id-pattern → :accept."
  [entity-id {:keys [id-pattern queue]}]
  (cond
    (nil? id-pattern) :accept
    (and entity-id (re-find (re-pattern id-pattern) entity-id)) :accept
    (and entity-id (some #(re-find (re-pattern %) entity-id) queue)) :queue
    :else :reject))

(defn gate-entity-id!
  "L4 canonical-id gate for one entity doc (futon1a.ingest.open-world/
  gate-entity-id!). Fires only when a descriptor is registered for the
  doc's :entity/type. :accept → nil · :queue → records + {:queued? true}
  (write proceeds) · :reject → records + throws L4 400 :non-canonical-id."
  [entity]
  (let [model-id (:entity/type entity)
        descriptor (get-model model-id)]
    (when descriptor
      (let [id-field (get descriptor :id-field :entity/id)
            id-val (get entity id-field)]
        (case (classify-id id-val descriptor)
          :reject
          (do (gate-queue-record! {:entity-id id-val :entity-type model-id
                                   :disposition :rejected
                                   :reason :non-canonical-id
                                   :expected (:id-pattern descriptor)})
              (throw (layered-error 4 :non-canonical-id
                                    {:entity/id id-val :id-field id-field
                                     :entity/type model-id
                                     :expected (:id-pattern descriptor)})))
          :queue
          (do (gate-queue-record! {:entity-id id-val :entity-type model-id
                                   :disposition :queued
                                   :reason :alias-pending-migration
                                   :expected (:id-pattern descriptor)})
              {:queued? true})
          :accept nil)))))

;; ---------------------------------------------------------------------------
;; The :mission/doc contract (verbatim from futon1a.scripts.seed-futon1-
;; descriptors — keep in lockstep with the source until cutover retires it).
;; ---------------------------------------------------------------------------

(def mission-doc-repo-allow-list
  ["futon0-d" "futon2-d" "futon3-d" "futon3a-d" "futon3b-d" "futon3c-d"
   "futon4-d" "futon4-elisp-d" "futon5-d" "futon5a-d" "futon6-d" "futon6-py-d"
   "futon7-d"])

(def mission-doc-id-pattern
  (str "^(" (str/join "|" mission-doc-repo-allow-list)
       ")/mission/(?!M-)[A-Za-z0-9-]+$"))

(def mission-doc-queue-patterns
  ["^M-[^/]+$" "^mission[|]"])

(def mission-doc-descriptor
  {:required [:entity/name :entity/external-id]
   :id-strategy :name
   :id-field :entity/name
   :id-pattern mission-doc-id-pattern
   :queue mission-doc-queue-patterns
   :repo-allow-list mission-doc-repo-allow-list})

(defn seed-mission-contract!
  "Boot-time governance (futon1a system.clj:135): the L4 gate must survive
  JVM restarts without hand re-registration. Idempotent."
  []
  (register-model! {:id :mission/doc :descriptor mission-doc-descriptor}))
