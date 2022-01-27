package eagle

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type initFunc func(t *testing.T, db *DB) (interface{}, error)
type checkFunc func(t *testing.T, db *DB, data interface{})

func runEagleCheck(t *testing.T, opts *Options, init initFunc, check checkFunc) {
	dir, err := ioutil.TempDir("", "eagle-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	opts.RootDir = dir

	db, err := Open(*opts)
	require.NoError(t, err)

	data, err := init(t, db)
	require.NoError(t, err)
	check(t, db, data)

	sizeBeforeClose := db.Size()

	require.NoError(t, db.Close())

	db, err = Open(*opts)
	require.NoError(t, err)

	sizeAfterOpen := db.Size()

	if sizeAfterOpen != sizeBeforeClose {
		t.Fatal("size mismatch")
	}

	check(t, db, data)
	require.NoError(t, db.Close())
}

func genRandBytes(n int) []byte {
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

func putMany(e *DB, n int, wg *sync.WaitGroup, t *testing.T) {
	var errors uint32

	for i := 0; i < nRuns; i++ {
		go func(i int) {
			key := serialize(uint32(i))
			value := serialize(uint32(i + 1))
			err := e.Put(key, value)
			if err != nil {
				atomic.AddUint32(&errors, 1)
			}
			wg.Done()
		}(i)
	}

	require.Equal(t, errors, uint32(0))
}

func removeEvenKeys(e *DB, n int, wg *sync.WaitGroup, t *testing.T) {
	var errors uint32

	for i := 0; i < nRuns; i += 2 {
		go func(i int) {
			key := serialize(uint32(i))

			value, err := e.Get(key)
			if err != nil {
				atomic.AddUint32(&errors, 1)
			}

			for value == nil { // this is necessary, because the key may have not been inserted at the time we execute remove
				value, err = e.Get(key)
				if err != nil {
					atomic.AddUint32(&errors, 1)
				}
				time.Sleep(time.Millisecond * 500)
			}

			err = e.Remove(key)
			if err != nil {
				atomic.AddUint32(&errors, 1)
			}

			wg.Done()
		}(i)
	}

	require.Equal(t, errors, uint32(0))
}

func TestConcurrentPut(t *testing.T) {
	key := genRandBytes(16)
	opts := DefaultOptions("").WithEncryptionKey(key)

	init := func(t *testing.T, db *DB) (interface{}, error) {
		wg := &sync.WaitGroup{}
		wg.Add(nRuns)
		putMany(db, nRuns, wg, t)
		wg.Wait()
		return nil, nil
	}

	runCheck := func(t *testing.T, db *DB, data interface{}) {
		for i := 0; i < nRuns; i++ {
			key := serialize(uint32(i))
			value, err := db.Get(key)
			if err != nil {
				t.Fatal("get", err)
			}

			deser := binary.BigEndian.Uint32(value)
			if deser != uint32(i+1) {
				t.Fatalf("expected %d, found %d\n", i+1, deser)
			}
		}
	}
	runEagleCheck(t, &opts, init, runCheck)
}

func TestConcurrentPutAndRemove(t *testing.T) {
	key := genRandBytes(16)
	opts := DefaultOptions("").WithEncryptionKey(key)

	init := func(t *testing.T, db *DB) (interface{}, error) {
		wg := &sync.WaitGroup{}
		wg.Add(nRuns + (nRuns / 2))

		putMany(db, nRuns, wg, t)

		removeEvenKeys(db, nRuns, wg, t)
		wg.Wait()
		return nil, nil
	}

	runCheck := func(t *testing.T, db *DB, data interface{}) {
		for i := 0; i < nRuns; i++ {
			key := serialize(uint32(i))

			value, err := db.Get(key)
			if err != nil {
				t.Fatal("get", err)
			}

			even := i%2 == 0
			if even && value != nil {
				t.Fatalf("key %d should not be present!\n", i)
			}

			if !even {
				if value == nil {
					t.Fatal("nil value found", i)
				}

				deser := binary.BigEndian.Uint32(value)
				if deser != uint32(i+1) {
					t.Fatalf("expected %d, found %d\n", i+1, deser)
				}
			}
		}
	}
	runEagleCheck(t, &opts, init, runCheck)
}

type Bytes []byte

func TestUpdateSameKeys(t *testing.T) {
	key := genRandBytes(16)

	// ensure to trigger file compaction very often
	opts := DefaultOptions("").WithEncryptionKey(key).WithMaxFileSize(4096).WithFileCompactionThreshold(0.1)

	rand.Seed(time.Now().UnixNano())

	init := func(t *testing.T, db *DB) (interface{}, error) {
		keySpace := 100
		keySize := 10
		valueSize := 25

		keys := make([]Bytes, keySpace)
		for i := 0; i < keySpace; i++ {
			keys[i] = genRandBytes(keySize)
		}

		oracleMap := make(map[string]Bytes)
		for i := 0; i < nRuns; i++ {
			key := keys[rand.Intn(keySpace)]

			if !bytes.Equal([]byte(string(key)), key) {
				t.Fatal("conversion problem")
			}

			var value []byte = nil
			if rand.Int()%2 == 0 {
				value = genRandBytes(valueSize)
				oracleMap[string(key)] = value

				if err := db.Put(key, value); err != nil {
					t.Fatal(err)
				}
			} else {
				oracleMap[string(key)] = nil

				if err := db.Remove(key); err != nil {
					t.Fatal(err)
				}
			}
		}
		return oracleMap, nil
	}

	runCheck := func(t *testing.T, db *DB, data interface{}) {
		oracleMap := data.(map[string]Bytes)

		for key, value := range oracleMap {
			retrievedValue, err := db.Get([]byte(key))
			if err != nil {
				t.Fatal(err)
			}

			if !bytes.Equal(retrievedValue, value) {
				log.Println(retrievedValue, value)
				t.Fatal("value mismatch")
			}
		}
	}
	runEagleCheck(t, &opts, init, runCheck)
}
