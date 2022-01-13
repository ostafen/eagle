# EagleDB

EagleDB is a fast and simple key-value store written in Golang. Design choices have been inspired by [HaloDB](https://github.com/yahoo/HaloDB).

# Usage

From a code perspective, you can use Eagle just like a simple map.

```go
import (
	"log"
	"github.com/ostafen/eagle"
)

func main() {
    db, _ := eagle.Open(eagle.DefaultOptions("/tmp/eagle-db"))
    defer db.Close()
    db.Put([]byte("name"), []byte("Bob"))
    val, _ := db.Get([]byte("name"))
    log.Println(string(val))
}
```

# Features

Eagle has been designed to handle an high volume of read/write operations. It shares serveral concepts and principles from log-structured databases. Records are stored in append-only files, while keys are held in an in-memory structure directly pointing directly to the locations on disk where the data resides. Because of this design choice, the system must have enough memory to hold the entire key space. However, this enables several major advantages. 
First of all read and write operation has a predictable performance, since a single seek is required to retrieve any value or to append a new record to the end of the current file that is open for writing. Moreover, since it is not required for data to be sorted, I/O and disk bandwidth are not satured that easily.
Also, in the event of a power loss and data corruption, at most two files must be repaired (the one used by the write thread and the one used by the compaction thread), thus allowing for fast recovery times.


# In-memory structure

Eagle uses an hash table to store the key space, where collisions are solved by chaining. Hence, each lookup, insert and remove operation is espected to take O(1) time on the average. When the number of elements store in the table grows, buckets are resized through the **incremental rehashing** technique used by [Redis](https://kousiknath.medium.com/a-little-internal-on-redis-key-value-storage-implementation-fdf96bac7453). Moreover, to allow for concurrent usage, the hash table is partitioned into multiple indipendent thread-safe tables.


# Strong encryption

Encryption is a fundamental requirement for protecting the privacy of data. Hence, any modern database is espected to natively support enctyption. When enabled, Eagle encrypts data through the **AES algorithm**, using the **Counter (CTR) mode of operation**, which is an industry standard. Such mode implies an additional 16 bytes **initialization vector (IV)**, which ensures that the cyphertext will be different, even when encrypting the same block multiple times.
However, generating a new IV for each piece of data which is appended to the log or index files would waste a lot of space. If you consider that the average size of an index entry is around 20 bytes, prepending a new IV to each entry would cause a storage overhead of 80%. To avoid wasting so much space, Eagle uses an technique which have been used by the [Badger](https://github.com/dgraph-io/badger) database. For each log file (or index file), a unique 12-byte IV is used used to encrypt all the entries in the file. Before crypting an entry, the 12-byte IV is attached a 4-byte unique value, representing the offset of the value being written, thus forming a 16-byte full IV. Note that the overhead of this method is less than adding an additional entry to each file.

# Notes

This project is still under development and not ready for production use. If you want, feel free to contribute.