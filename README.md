Available via [clojars](http://clojars.org/search?q=clj-mmap)   
Current stable version: [clj-mmap "1.1.0"]


# clj-mmap

A Clojure library designed to allow you to easily mmap files via Java's NIO, and to handle files larger than 2GB.


## Usage
```clojure
(with-open [mapped-file (clj-mmap/get-mmap "/tmp/big_file.txt")]
  (let [some-bytes (clj-mmap/get-bytes mapped-file 0 30)]
    (println (str "First 30 bytes of file, '" (String. some-bytes "UTF-8") "'"))))
```


## Artifacts

clj-mmap artifacts are [released to Clojars](https://clojars.org/clj-mmap).

If you are using Maven, add the following repository definition to your `pom.xml`:

``` xml
<repository>
  <id>clojars</id>
  <url>http://clojars.org/repo</url>
</repository>
```

### The Most Recent Release

With Leiningen:

    [clj-mmap "1.1.0"]


With Maven:

    <dependency>
      <groupId>clj-mmap</groupId>
      <artifactId>clj-mmap</artifactId>
      <version>1.1.0</version>
    </dependency>


## License

CC0
http://creativecommons.org/publicdomain/zero/1.0/

I'd also like to thank my employer, Gracenote, for allowing me to create this open source port.

Copyright (C) 2012-2013 Alan Busby