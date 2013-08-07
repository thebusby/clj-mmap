(ns clj-mmap)

(set! *warn-on-reflection* true)

(def ^:private bytes-per-map 
  "The number of bytes a single MappedByteBuffer will store"
  java.lang.Integer/MAX_VALUE)

(deftype Mmap [fis fc maps]  
  clojure.lang.Indexed 
  (nth [this i] (get maps i))
  (nth [this i not-found] (get maps i not-found))

  clojure.lang.Counted
  (count [this] (.size fc))

  java.io.Closeable
  (close 
    [this]
     (do   
       (.close fc)
       (.close fis))))

(defn get-mmap [^String filename]
  "Provided a filename, mmap the entire file, and return a opaque type to allow further access.
   Remember to use with-open or to call .close to clean up memory and open file descriptors."
  (let [fis  (java.io.FileInputStream. filename)
        fc   (.getChannel fis)
        size (.size fc)]
    (Mmap. fis 
           fc 
           (reduce (fn [agg pos] (conj agg
                                       (.map fc 
                                             java.nio.channels.FileChannel$MapMode/READ_ONLY 
                                             pos 
                                             (min bytes-per-map (- size pos)))))
                   []
                   (range 0 size bytes-per-map)))))

(defn get-bytes [mmap pos n]
  "Retrieve n bytes at position pos"
  (let [get-chunk   #(nth mmap (int (/ % bytes-per-map)))
        end         (+ pos n)
        chunk-term  (-> pos
                        (/ bytes-per-map)
                        int
                        inc
                        (* bytes-per-map))
        read-size   (- (min end chunk-term) pos)
        start-chunk (get-chunk pos)
        end-chunk   (get-chunk end)
        buf         (byte-array n)]
    (locking start-chunk 
      (.position start-chunk (mod pos bytes-per-map))
      (.get start-chunk buf 0 read-size))
    (if (not= start-chunk end-chunk)
      (locking end-chunk 
        (.position end-chunk 0)
        (.get end-chunk buf read-size (- n read-size))))
    buf))
