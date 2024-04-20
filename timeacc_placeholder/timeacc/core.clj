(ns timeacc.core)

(defn root [] nil)
(defn reset [_])
(defn report [_])

(defn unsafe-acc [_ _])

(defmacro measure [_acc & body]
  `(do ~@body))

(defn measure-xform [_acc xform]
  xform)

(defn accumulate-nano-seconds-since [_ _])

(defn accumulate-nano-seconds [_ _])

(defn counter [_] 0)

(defn total-time-seconds [_] 0.0)

(defn avg-time-seconds [_] 0.0)

(defn acc-map [_] {})

(defn stop-watch [_])

(defn start [_])

(defn stop [_])

(defmacro with-pause [_ & expr]
  `(do ~@expr))

(defmacro with-watch [_ & expr]
  `(do ~@expr))




