(ns my-storage.merge)

(defn merge-sorted
  "sources: 新しい→古い順のリスト。各要素は [k v] を k 昇順に並べたシーケンス
    戻り値: 各キーにつき最も新しい source の [k v] を、k 昇順で並べた遅延シーケンス"
  [sources]
  (lazy-seq
   (let [live (keep-indexed (fn [i s] (when (seq s) [i s])) sources)]
     (when (seq live)
       (let [min-k (reduce (fn [m [_ s]]
                             (let [k (ffirst s)]
                               (if (or (nil? m) (neg? (compare k m))) k m)))
                           nil
                           live)
             winner (->> live
                         (filter (fn [[_ s]] (= (ffirst s) min-k)))
                         (sort-by first)
                         first)
             win-v (second (first (second winner)))
             next-sources (map (fn [[_ s]]
                                 (if (= (ffirst s) min-k)
                                   (rest s)
                                   s))
                               live)]
         (cons [min-k win-v]
               (merge-sorted next-sources)))))))