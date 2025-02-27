(ns datahike.meetup-big-demo
  (:require [datahike.meetup-helpers :as mh]
            [datahike.trace-utils :as tu]
            [datahike.api :as datahike]
            [datahike.query :as dq]))

(def broader-query-expr
  '{:query
    {:find [?from-id ?id],
     :keys [from_id id],

         
     ;; Input argument declaration
     :in [$ ;; <-- The database

          % ;; <-- Rules

          [?from-id ...] ;; All source objects
          ?relation-type ;; Kind of relation
          [?type ...]    ;; Types


          ],
     
     :where
     [
      ;; Det finns ett begrepp ?c med id from-id
      [?c :concept/id ?from-id]

      ;; ... en kant från detta begrepp pekar på ett annat begrepp ?related-c
      (edge ?c ?relation-type ?related-c ?r)

      ;; Detta andra begrepp related-c har id ?id
      ;; och typen ?type
      [?related-c :concept/id ?id]
      [?related-c :concept/type ?type]

      ;; Deprecated ska vara false.
      ;; https://docs.datomic.com/pro/query/query.html#ground
      [(ground false) ?deprecated]

          
      [(get-else $ ?related-c :concept/deprecated false) ?deprecated]
      ]},
    :settings {}
    :args
    [

     ;; Rules
     [[(-forward-edge ?from-concept ?type ?to-concept ?relation)
       [(ground
         ["broader"
          "related"
          "possible-combination"
          "unlikely-combination"
          "substitutability"
          "broad-match"
          "exact-match"
          "close-match"])
        [?type ...]]
       [?relation :relation/concept-1 ?from-concept]
       [?relation :relation/type ?type]
       [?relation :relation/concept-2 ?to-concept]]
      [(-reverse-edge ?from-concept ?type ?to-concept ?relation)
       [(ground
         {"narrower" "broader",
          "related" "related",
          "possible-combination" "possible-combination",
          "unlikely-combination" "unlikely-combination",
          "substituted-by" "substitutability",
          "narrow-match" "broad-match",
          "exact-match" "exact-match",
          "close-match" "close-match"})
        [[?type ?reverse-type]]]
       [?relation :relation/concept-2 ?from-concept]
       [?relation :relation/type ?reverse-type]
       [?relation :relation/concept-1 ?to-concept]]
      [(edge ?from-concept ?type ?to-concept ?relation)
       (or
        (-forward-edge ?from-concept ?type ?to-concept ?relation)
        (-reverse-edge ?from-concept ?type ?to-concept ?relation))]]

     ;; Detta är argumenten som man skickar in
     #{"w6ud_quG_dgh"}
     "broader"
     ["occupation-field"]]})

(def query-0 '{:query
               {:find [?id ?inst],
                :keys [:id :timestamp],
                :in [$],
                :where
                [[?v :taxonomy-version/id ?id]
                 [(< 0 ?id)]
                 [?v :taxonomy-version/tx ?tx]
                 [?tx :db/txInstant ?inst]]},
               :args []})

(def query-1 (dq/normalize-q-input
              '[:find
                ?id
                (pull ?c pull-pattern)
                :in
                $
                [?id ...]
                pull-pattern
                :where
                [?c :concept/id ?id]]
              [#{"4CNy_4r7_Kqk"
                 "49hp_SsT_G6u"
                 "n4aT_1x4_oFw"
                 "XMgK_vMp_xJp"
                 "dvre_Duj_oyv"
                 "H5iv_5v9_t4o"
                 "s6jT_CW8_viT"
                 "CESx_ZDS_4bY"
                 "1nfZ_zkU_RyS"
                 "bLS3_m9V_c41"}
               [:concept/preferred-label]]))

(defn set-db [query db]
  (update query :args (fn [args] (into [db] args))))

(defn run-example [query]
  (tu/with-connection (deref tu/db)
    (fn [conn]
      (mh/disp-traced-q
       (set-db query
               (datahike/as-of
                (deref conn)
                536870932))))))

(comment

  (run-example broader-query-expr)
  (run-example query-0)
  (run-example query-1)

  )
