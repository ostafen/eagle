# EagleDB

EagleDB is a fast and simple key-value store written in Golang. Design choices have been inspired by [HaloDB](https://github.com/yahoo/HaloDB).

# Features
- Intuitive API
- Built-in support for encryption

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

# Notes

This project is still under development and not ready for production use. If you want, feel free to contribute.