package eagle

import (
	"encoding/binary"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func RandBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

func serialize(value uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	return buf
}

const nRuns = 1000

func insertMany(e *DB, n int, wg *sync.WaitGroup) {
	for i := 0; i < nRuns; i++ {
		go func(i int) {
			key := serialize(uint32(i))
			value := serialize(uint32(i + 1))
			err := e.Put(key, value)
			if err != nil {
				log.Fatal("put", err)
			}
			wg.Done()
		}(i)
	}
}

func removeEvenKeys(e *DB, n int, wg *sync.WaitGroup) {
	for i := 0; i < nRuns; i += 2 {
		go func(i int) {
			key := serialize(uint32(i))

			value, err := e.Get(key)
			if err != nil {
				log.Fatal(err)
			}

			for value == nil { // this is necessary, because the key may have not been inserted at the time we execute remove
				value, err = e.Get(key)
				if err != nil {
					log.Fatal(err)
				}
				time.Sleep(time.Millisecond * 500)

			}

			err = e.Remove(key)
			if err != nil {
				log.Fatal(err)
			}

			wg.Done()
		}(i)
	}
}

func TestWritesAndReload(t *testing.T) {
	dbDir := "./db"
	defer os.RemoveAll(dbDir)

	key := RandBytes(16)

	opts := DefaultOptions(dbDir).WithEncryptionKey(key)
	e, err := Open(opts)
	if err != nil {
		t.Fatal("open", err)
	}
	rand.Seed(time.Now().UnixNano())

	wg := &sync.WaitGroup{}
	wg.Add(nRuns)
	insertMany(e, nRuns, wg)
	wg.Wait()

	runCheck := func(db *DB) {
		for i := 0; i < nRuns; i++ {
			key := serialize(uint32(i))
			value, err := e.Get(key)
			if err != nil {
				log.Fatal("get", err)
			}

			deser := binary.BigEndian.Uint32(value)
			if deser != uint32(i+1) {
				t.Fatalf("expected %d, found %d\n", i+1, deser)
			}
		}
	}
	runCheck(e)

	log.Println("check ok")

	err = e.Close()
	if err != nil {
		log.Fatal("close", err)
	}

	e, err = Open(opts)
	if err != nil {
		t.Fatal("open", err)
	}

	runCheck(e)

	err = e.Close()
	if err != nil {
		log.Fatal("close", err)
	}
}

func TestInsertRemove(t *testing.T) {
	dbDir, err := ioutil.TempDir(".", "db")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dbDir)

	key := RandBytes(16)

	opts := DefaultOptions(dbDir).WithEncryptionKey(key)

	e, err := Open(opts)
	if err != nil {
		t.Fatal("get", err)
	}
	rand.Seed(time.Now().UnixNano())

	wg := &sync.WaitGroup{}
	wg.Add(nRuns + (nRuns / 2))

	insertMany(e, nRuns, wg)

	time.Sleep(time.Second * 6)
	removeEvenKeys(e, nRuns, wg)

	wg.Wait()

	runCheck := func(e *DB) {
		for i := 0; i < nRuns; i++ {
			key := serialize(uint32(i))

			value, err := e.Get(key)
			if err != nil {
				log.Fatal("get", err)
			}

			even := i%2 == 0
			if even && value != nil {
				t.Fatalf("key %d should not be present!\n", i)
			}

			if !even {
				if value == nil {
					log.Fatal("nil value found", i)
				}

				deser := binary.BigEndian.Uint32(value)
				if deser != uint32(i+1) {
					t.Fatalf("expected %d, found %d\n", i+1, deser)
				}
			}
		}
	}

	runCheck(e)

	err = e.Close()
	if err != nil {
		t.Fatal("get", err)
	}

	e, err = Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	runCheck(e)

	err = e.Close()
	if err != nil {
		t.Fatal("get", err)
	}
}
