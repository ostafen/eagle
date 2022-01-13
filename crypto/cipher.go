package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
)

type Cipher struct {
	key []byte
	iv  []byte
}

func NewCipher(key []byte, iv []byte) *Cipher {
	return &Cipher{
		key: key,
		iv:  iv,
	}
}

func xor(src, key, iv []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	dst := make([]byte, len(src))
	stream := cipher.NewCTR(block, iv)
	stream.XORKeyStream(dst, src)
	return dst, err
}

func (cipher *Cipher) Encrypt(plaintext []byte) ([]byte, error) {
	return xor(plaintext, cipher.key, cipher.iv)
}

func (cipher *Cipher) Decrypt(cyphertext []byte) ([]byte, error) {
	return xor(cyphertext, cipher.key, cipher.iv)
}

// GenerateIV generates IV.
func GenerateIV() ([]byte, error) {
	iv := make([]byte, aes.BlockSize)
	_, err := rand.Read(iv)
	return iv, err
}
