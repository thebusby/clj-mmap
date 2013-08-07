(ns clj-mmap)

(set! *warn-on-reflection* true)

(def ^:private bytes-per-map 
  "The number of bytes a single MappedByteBuffer will store"
  java.lang.Integer/MAX_VALUE)

(definterface ISize
  (^long size []))

(deftype Mmap [^java.io.FileInputStream fis ^java.nio.channels.FileChannel fc maps]  
  ISize
  (size [this] (.size fc))

  clojure.lang.Indexed 
  (nth [this i] (get maps i))
  (nth [this i not-found] (get maps i not-found))

  clojure.lang.Seqable
  (seq [this] (seq maps))

  java.io.Closeable
  (close 
    [this]
     (do   
       (.close fc)
       (.close fis))))

(def ^:private map-modes
  {:private    java.nio.channels.FileChannel$MapMode/PRIVATE
   :read-only  java.nio.channels.FileChannel$MapMode/READ_ONLY 
   :read-write java.nio.channels.FileChannel$MapMode/READ_WRITE})

(defn get-mmap 
  "Provided a filename, mmap the entire file, and return an opaque type to allow further access.
   Remember to use with-open, or to call .close, to clean up memory and open file descriptors."
  ([^String filename] (get-mmap filename :read-only))
  ([^String filename map-mode]
   (let [fis  (java.io.FileInputStream. filename)
         fc   (.getChannel fis)
         size (.size fc)]
     (Mmap. fis 
            fc 
            (reduce (fn [agg pos] (conj agg
                                        (.map fc 
                                              (map-modes map-mode)
                                              pos 
                                              (min bytes-per-map 
                                                   (- size pos)))))
                    []
                    (range 0 size bytes-per-map))))))

(defn get-bytes ^bytes [mmap pos n]
  "Retrieve n bytes from mmap, at byte position pos."
  (let [get-chunk   #(nth mmap (int (/ % bytes-per-map)))
        end         (+ pos n)
        chunk-term  (-> pos
                        (/ bytes-per-map)
                        int
                        inc
                        (* bytes-per-map))
        read-size   (- (min end chunk-term) 
                       pos)
        start-chunk (get-chunk pos)
        end-chunk   (get-chunk end)
        buf         (byte-array n)]

    (locking start-chunk 
      (.position start-chunk (mod pos bytes-per-map))
      (.get start-chunk buf 0 read-size))

    ;; Handle reads that span MappedByteBuffers
    (if (not= start-chunk end-chunk)
      (locking end-chunk 
        (.position end-chunk 0)
        (.get end-chunk buf read-size (- n read-size))))

    buf))

(defn put-bytes
  "Write n bytes from buf into mmap, at byte position pos.
   If n isn't provided, the size of the buffer provided is used."
  ([mmap ^bytes buf pos] (put-bytes buf pos (.size buf)))
  ([mmap ^bytes buf pos n]
     (let [get-chunk   #(nth mmap (int (/ % bytes-per-map)))
           end         (+ pos n)
           chunk-term  (-> pos
                           (/ bytes-per-map)
                           int
                           inc
                           (* bytes-per-map))
           write-size   (- (min end chunk-term) 
                          pos)
           start-chunk (get-chunk pos)
           end-chunk   (get-chunk end)]

       (locking start-chunk 
         (.position start-chunk (mod pos bytes-per-map))
         (.put start-chunk buf 0 write-size))

       ;; Handle writes that span MappedByteBuffers
       (if (not= start-chunk end-chunk)
         (locking end-chunk 
           (.position end-chunk 0)
           (.put end-chunk buf write-size (- n write-size))))

       nil)))

(defn loaded? [mmap]
  "Returns true if it is likely that the buffer's contents reside in physical memory."
  (every? (fn [^java.nio.MappedByteBuffer buf] 
            (.isLoaded buf))  
          mmap))
