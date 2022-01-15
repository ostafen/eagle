package eagle

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/ostafen/eagle/crypto"
)

type dbFile struct {
	*os.File
	size uint32
	iv   []byte
}

var ErrInvalidFile = errors.New("invalid file")

func getIV(iv []byte, offset uint32) []byte {
	newIV := make([]byte, 16)
	copy(newIV, iv)

	binary.BigEndian.PutUint32(newIV[12:], offset)
	return newIV
}

func getCypher(iv []byte) *crypto.Cipher {
	if !dbOptions.UseEncryption() {
		return nil
	}
	return crypto.NewCipher(dbOptions.EncryptKey, iv)
}

func getSize(file *os.File) (uint32, error) {
	if stat, err := file.Stat(); err != nil {
		return 0, err
	} else {
		return uint32(stat.Size()), nil
	}
}

func readIV(file *dbFile) error {
	if file.size < 12 {
		return ErrInvalidFile
	}

	iv := make([]byte, 12)
	_, err := io.ReadFull(file, iv)

	file.iv = iv
	return err
}

func openDBFile(filename string) (*dbFile, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	size, err := getSize(file)
	if err != nil {
		return nil, err
	}

	appendFile := &dbFile{
		File: file,
		size: size,
	}

	if dbOptions.UseEncryption() {
		err := readIV(appendFile)
		return appendFile, err
	}

	return appendFile, nil

}

func writeIV(writer io.Writer) ([]byte, error) {
	iv, err := crypto.GenerateIV()
	if err != nil {
		return nil, err
	}

	_, err = writer.Write(iv[:12])
	if err != nil {
		return nil, err
	}
	return iv[:12], nil
}

func createDBFile(filename string) (*dbFile, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	var size uint32 = 0
	var iv []byte = nil

	if dbOptions.UseEncryption() {
		iv, err = writeIV(file)
		if err != nil {
			return nil, err
		}
		size = uint32(len(iv))
	}

	return &dbFile{
		File: file,
		size: size,
		iv:   iv,
	}, nil
}

func (bf *dbFile) Write(p []byte) (int, error) {
	n, err := bf.File.Write(p)
	if n > 0 {
		bf.size += uint32(n)
	}
	return n, err
}

func (f *dbFile) SyncAndClose() error {
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func (f *dbFile) Remove() error {
	if err := f.Close(); err != nil {
		return err
	}
	return os.Remove(f.Name())
}

func (f *dbFile) Size() uint32 {
	return f.size
}

const repairFileExt = ".repair"

func (file *dbFile) createRepairFile() (*dbFile, error) {
	repairFile, err := os.Create(file.File.Name() + repairFileExt)
	if err != nil {
		return nil, err
	}

	if len(file.iv) > 0 {
		if _, err := repairFile.Write(file.iv); err != nil {
			return nil, err
		}
	}

	return &dbFile{
		File: repairFile,
		size: uint32(len(file.iv)),
		iv:   file.iv,
	}, nil
}
