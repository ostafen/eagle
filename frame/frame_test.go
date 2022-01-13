package frame

import (
	"bytes"
	"log"
	"math/rand"
	"testing"

	"github.com/ostafen/eagle/crypto"
)

const bufSize = 1024
const nFrames = 1024

func getRandomBytes(size int) []byte {
	data := make([]byte, size)
	rand.Read(data)
	return data
}

type payload []byte

func TestFrameReadWriter(t *testing.T) {
	secret := getRandomBytes(16)

	iv, err := crypto.GenerateIV()
	if err != nil {
		log.Fatal(err)
	}

	offset := int64(0)
	var buf bytes.Buffer
	frames := make([]payload, 0, nFrames)
	for i := 0; i < nFrames; i++ {
		cipher := crypto.NewCipher(secret, iv)

		data := getRandomBytes(rand.Intn(bufSize) + 10)

		frames = append(frames, payload(data))
		n, err := Encode(&buf, data, cipher)
		if err != nil {
			t.Fatal(err)
		}
		offset += int64(n)
	}

	offset = int64(0)
	for i := 0; i < nFrames; i++ {
		cipher := crypto.NewCipher(secret, iv)

		n, frame, err := Decode(&buf, cipher)
		if err != nil {
			t.Fatal(err, i)
		}

		if !bytes.Equal(frame, frames[i]) {
			t.Fatal("frame mismatch")
		}
		offset += int64(n)
	}
}
