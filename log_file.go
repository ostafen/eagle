package eagle

import (
	"log"
	"os"
	"path/filepath"
)

type logFile struct {
	FileId    uint32
	valueFile *valueLog
	keyFile   *keyLog
}

func createLogFile(path string, fileId uint32) (*logFile, error) {
	dataFile, err := createDataFile(path, fileId)
	if err != nil {
		return nil, err
	}

	indexFile, err := createKeyLogFile(path, fileId)
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    uint32(fileId),
		valueFile: dataFile,
		keyFile:   indexFile,
	}, nil
}

func openLogFile(path string, fileId uint32) (*logFile, error) {
	dataFile, err := openDataFile(path, fileId)
	if err != nil {
		return nil, err
	}

	indexFile, err := openKeyLogFile(path, fileId)
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    uint32(fileId),
		valueFile: dataFile,
		keyFile:   indexFile,
	}, nil
}

func (lf *logFile) IterateKeys() *keyFileIterator {
	return lf.keyFile.Iterator()
}

func (lf *logFile) ReadPointer(ptr *ValuePointer) ([]byte, error) {
	value := make([]byte, ptr.valueSize)
	_, err := lf.valueFile.ReadAt(value, int64(ptr.valueOffset))
	if err != nil {
		return nil, err
	}

	if dbOptions.UseEncryption() {
		cipher := getCypher(getIV(lf.valueFile.iv, ptr.valueOffset))
		value, err = cipher.Decrypt(value)
		if err != nil {
			return nil, err
		}
	}

	return value, nil
}

func (lf *logFile) AppendRecord(r *Record) (*ValuePointer, error) {
	valueOffset := lf.valueFile.size

	e := &klEntry{
		SeqNumber:   r.SeqNumber,
		Key:         r.Key,
		ValueOffset: valueOffset,
		ValueSize:   uint32(len(r.Value)),
	}

	value := r.Value

	cipher := getCypher(getIV(lf.valueFile.iv, valueOffset))
	if cipher != nil {
		var err error
		if value, err = cipher.Encrypt(value); err != nil {
			return nil, err
		}
	}

	if _, err := lf.keyFile.AppendEntry(e, value); err != nil {
		return nil, err
	}

	if len(r.Value) == 0 { // writing tombstone record
		return nil, nil
	}

	ptr := &ValuePointer{
		FileId:      lf.FileId,
		valueOffset: valueOffset,
		valueSize:   uint32(len(r.Value)),
	}

	_, err := lf.valueFile.Write(value)
	return ptr, err
}

func (lf *logFile) Sync() error {
	if err := lf.valueFile.Sync(); err != nil {
		return err
	}
	return lf.keyFile.Sync()
}

func (lf *logFile) SyncAndClose() error {
	if err := lf.valueFile.SyncAndClose(); err != nil {
		return err
	}

	return lf.keyFile.SyncAndClose()
}

func (lf *logFile) UpdateStaleSize(size uint32) {
	lf.valueFile.staleDataSize += size
}

func (lf *logFile) Size() uint32 {
	return lf.keyFile.Size() + lf.valueFile.Size()
}

func (lf *logFile) Close() error {
	if err := lf.valueFile.Close(); err != nil {
		return err
	}
	return lf.keyFile.Close()
}

func (lf *logFile) remove() error {
	if err := lf.keyFile.Remove(); err != nil {
		return err
	}
	return lf.valueFile.Remove()
}

func (lf *logFile) createRepairFile() (*logFile, error) {
	valueFile, err := lf.valueFile.createRepairFile()
	if err != nil {
		return nil, err
	}
	keyFile, err := lf.keyFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    lf.FileId,
		valueFile: valueFile,
		keyFile:   keyFile,
	}, nil
}

func (lf *logFile) repair() (*logFile, error) {
	newFile, err := lf.createRepairFile()
	if err != nil {
		return nil, err
	}

	nCopied := 0

	it := lf.IterateKeys()
	for it.HasNext() {
		entry, checksum, err := it.Next()
		if err != nil {
			break
		}

		// the entry is damaged and report an incorrect size
		if entry.ValueOffset+entry.ValueSize > lf.valueFile.size {
			break
		}

		value := make([]byte, entry.ValueSize)
		if _, err = lf.valueFile.Read(value); err != nil {
			return nil, err
		}

		if computeChecksum(getHeaderFromEntry(entry), entry.Key, value) != checksum {
			break
		}

		if _, err = newFile.keyFile.AppendEntry(entry, value); err != nil {
			return nil, err
		}

		if _, err = newFile.valueFile.Write(value); err != nil {
			return nil, err
		}

		nCopied++
	}

	log.Printf("Repairing file: %d. Copied %d records\n", lf.FileId, nCopied)

	if err := newFile.SyncAndClose(); err != nil {
		return nil, err
	}

	err = os.Rename(newFile.valueFile.Name(), lf.valueFile.Name())
	if err != nil {
		return nil, err
	}

	err = os.Rename(newFile.keyFile.Name(), lf.keyFile.Name())
	if err != nil {
		return nil, err
	}

	lf.Close()

	return openLogFile(filepath.Dir(lf.valueFile.Name()), newFile.FileId)
}
