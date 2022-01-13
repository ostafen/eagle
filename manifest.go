package eagle

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"os"

	"github.com/ostafen/eagle/crypto"
)

func decryptFileIfNeeded(data []byte) ([]byte, error) {
	if !dbOptions.UseEncryption() {
		return data, nil
	}

	iv, encryptedData := data[:16], data[16:]
	cipher := crypto.NewCipher(dbOptions.EncryptKey, iv)
	return cipher.Decrypt(encryptedData)
}

func readFile(filename string, value interface{}) error {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		return nil
	}

	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}

	data, err = decryptFileIfNeeded(data)
	if err != nil {
		return err
	}

	err = binary.Read(bytes.NewBuffer(data), binary.BigEndian, value)
	return err
}

func encryptFileIfNeeded(bytes []byte) ([]byte, error) {
	if !dbOptions.UseEncryption() {
		return bytes, nil
	}

	iv, err := crypto.GenerateIV()
	if err != nil {
		return bytes, err
	}

	cipher := crypto.NewCipher(dbOptions.EncryptKey, iv)
	encrypted, err := cipher.Encrypt(bytes)
	if err != nil {
		return nil, err
	}

	withIV := make([]byte, len(encrypted)+len(iv))
	copy(withIV, iv)
	copy(withIV[len(iv):], encrypted)

	return withIV, nil
}

const manifestFilename = "MANIFEST"

func SaveToFile(filename string, value interface{}) error {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, value)

	bytes, err := encryptFileIfNeeded(buf.Bytes())
	if err != nil {
		return err
	}

	file, err := ioutil.TempFile("", manifestFilename)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()

	if _, err = file.Write(bytes); err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return os.Rename(file.Name(), filename)
}

type manifest struct {
	DBOpen  bool
	IOError bool
}

func openManifestFile(path string) (*manifest, error) {
	manifest := &manifest{
		DBOpen:  false,
		IOError: false,
	}
	err := readFile(path+"/"+manifestFilename, manifest)
	return manifest, err
}

func (m *manifest) Save(path string) error {
	return SaveToFile(path+"/"+manifestFilename, m)
}
