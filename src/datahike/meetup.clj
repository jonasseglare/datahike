(ns datahike.meetup
  (:require [datahike.api :as datahike]))

(defn demo0 []
  (let [cfg {:store {:backend :mem
                     :id (str (gensym))}
             :attribute-refs? true
             :keep-history? true
             :schema-flexibility :write}

        _ (datahike/create-database cfg)
        conn (datahike/connect cfg)]
    conn))
