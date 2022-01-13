package eagle

import (
	"log"
	"os"
	"path/filepath"
)

type logFile struct {
	FileId    uint32
	dataFile  *DataFile
	indexFile *indexFile
}

func createLogFile(path string, fileId uint32) (*logFile, error) {
	dataFile, err := createDataFile(path, fileId)
	if err != nil {
		return nil, err
	}

	indexFile, err := createIndexFile(path, fileId)
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    uint32(fileId),
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

func openLogFile(path string, fileId uint32) (*logFile, error) {
	dataFile, err := openDataFile(path, fileId)
	if err != nil {
		return nil, err
	}

	indexFile, err := openIndexFile(path, fileId)
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    uint32(fileId),
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

func (lf *logFile) IterateData() *DataFileIterator {
	return lf.dataFile.Iterator()
}

func (lf *logFile) IterateIndex() *indexFileIterator {
	return lf.indexFile.Iterator()
}

func (lf *logFile) ReadPointer(ptr *DiskPointer) ([]byte, error) {
	return lf.dataFile.ReadPointer(ptr)
}

func pointerToIndexEntry(key []byte, seqNumber uint64, ptr *DiskPointer) *indexEntry {
	return &indexEntry{
		SeqNumber:   seqNumber,
		FrameOffset: ptr.frameOffset,
		FrameSize:   ptr.frameSize,
		Key:         key,
	}
}

func (lf *logFile) AppendRecord(r *Record) (*DiskPointer, error) {
	ptr, err := lf.dataFile.AppendRecord(r)
	if err != nil {
		return nil, err
	}

	e := pointerToIndexEntry(r.Key, r.SeqNumber, ptr)
	_, err = lf.indexFile.AppendEntry(e)
	return ptr, err
}

func (lf *logFile) Sync() error {
	if err := lf.dataFile.Sync(); err != nil {
		return err
	}
	return lf.indexFile.Sync()
}

func (lf *logFile) SyncAndClose() error {
	if err := lf.dataFile.SyncAndClose(); err != nil {
		return err
	}

	return lf.indexFile.SyncAndClose()
}

func (lf *logFile) UpdateStaleSize(size uint32) {
	lf.dataFile.staleDataSize += size
}

func (lf *logFile) Size() uint32 {
	return lf.dataFile.Size()
}

func (lf *logFile) Close() error {
	if err := lf.dataFile.Close(); err != nil {
		return err
	}
	return lf.indexFile.Close()
}

func (lf *logFile) remove() error {
	lf.Close()

	if err := lf.indexFile.Remove(); err != nil {
		return err
	}

	return lf.dataFile.Remove()
}

func (lf *logFile) createRepairFile() (*logFile, error) {
	dataFile, err := lf.dataFile.createRepairFile()
	if err != nil {
		return nil, err
	}
	indexFile, err := lf.indexFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &logFile{
		FileId:    lf.FileId,
		dataFile:  dataFile,
		indexFile: indexFile,
	}, nil
}

func (lf *logFile) repair() (*logFile, error) {
	newFile, err := lf.createRepairFile()
	if err != nil {
		return nil, err
	}

	nCopied := 0

	it := lf.dataFile.Iterator()
	for it.HasNext() {
		_, r, err := it.Next()
		if err != nil {
			break
		}

		_, err = newFile.AppendRecord(r)
		if err != nil {
			return nil, err
		}
		nCopied++
	}
	log.Printf("Repairing file: %d. Copied %d records\n", lf.FileId, nCopied)

	if err := newFile.SyncAndClose(); err != nil {
		return nil, err
	}

	err = os.Rename(newFile.dataFile.Name(), lf.dataFile.Name())
	if err != nil {
		return nil, err
	}

	err = os.Rename(newFile.indexFile.Name(), lf.indexFile.Name())
	if err != nil {
		return nil, err
	}

	lf.Close()

	return openLogFile(filepath.Dir(lf.dataFile.Name()), newFile.FileId)
}
