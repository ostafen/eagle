package eagle

import (
	"log"
	"sync"

	"github.com/ostafen/eagle/util"
)

const compactFilename = "COMPACT"

type compactRegistry struct {
	currWriteFileId     int
	lastCompactedFileId int
	compacting          bool
}

func readCompactFile(path string) (*compactRegistry, error) {
	registry := &compactRegistry{
		currWriteFileId:     -1,
		lastCompactedFileId: -1,
	}
	err := readFile(path+"/"+compactFilename, registry)
	return registry, err
}

func (r *compactRegistry) Save(path string) (*compactRegistry, error) {
	err := SaveToFile(path+"/"+compactFilename, r)
	return r, err
}

type fileCompactor struct {
	wg       sync.WaitGroup
	db       *DB
	quit     chan struct{}
	fileChan chan *logFile

	currFileId    util.AtomicUint32
	currWriteFile *logFile
	lock          sync.Mutex

	compactionMap map[uint32]struct{}
}

func newFileCompactor(db *DB) *fileCompactor {
	return &fileCompactor{
		db:            db,
		quit:          make(chan struct{}, 1),
		fileChan:      make(chan *logFile, 100),
		compactionMap: make(map[uint32]struct{}),
		currWriteFile: nil,
	}
}

func (fc *fileCompactor) rotateCurrentFile() error {
	if fc.currWriteFile != nil {
		if err := fc.currWriteFile.Sync(); err != nil {
			return err
		}
	}

	newFile, err := fc.db.createNewLogFile()
	if err != nil {
		return err
	}
	fc.currFileId.Set(newFile.FileId)
	fc.currWriteFile = newFile
	fc.setCompacting(fc.currWriteFile)
	return nil
}

func (fc *fileCompactor) setCompacting(file *logFile) error {
	if fc.currWriteFile != nil {
		compactRegistry := &compactRegistry{
			currWriteFileId:     int(fc.currWriteFile.FileId),
			compacting:          true,
			lastCompactedFileId: -1,
		}
		return SaveToFile(fc.db.rootDir+"/"+compactFilename, compactRegistry)
	}
	return nil
}

func (fc *fileCompactor) setCompacted(file *logFile) error {
	if fc.currWriteFile != nil {
		compactRegistry := &compactRegistry{
			currWriteFileId:     int(fc.currWriteFile.FileId), // unuseful in this case
			compacting:          false,
			lastCompactedFileId: int(file.FileId),
		}
		return SaveToFile(fc.db.rootDir+"/"+compactFilename, compactRegistry)
	}
	return nil
}

func (fc *fileCompactor) ensureRoomForWrite(size uint32) error {
	if fc.currWriteFile == nil || fc.currWriteFile.Size()+size > fc.db.opts.MaxFileSize {
		if err := fc.rotateCurrentFile(); err != nil {
			return err
		}
	}
	return nil
}

func (fc *fileCompactor) compactFile(file *logFile) error {
	if err := fc.setCompacting(file); err != nil {
		return err
	}

	nRecordCopied := 0
	it := file.keyFile.Iterator()
	for it.HasNext() {
		entry, _, err := it.Next()
		if err != nil {
			return err
		}

		rInfo := fc.db.table.Get(entry.Key)
		if rInfo != nil && entry.SeqNumber == rInfo.seqNumber { // record is not stale
			if err := fc.ensureRoomForWrite(recordSize(len(entry.Key), int(entry.ValueSize))); err != nil {
				return err
			}

			newInfo := &recordInfo{seqNumber: entry.SeqNumber, ptr: nil}

			var value []byte = nil
			if entry.ValueSize > 0 {
				if value, err = file.ReadPointer(rInfo.ptr); err != nil {
					return err
				}
			}

			writeOffset := fc.currWriteFile.valueFile.size
			r := &Record{Key: entry.Key, Value: value, SeqNumber: entry.SeqNumber}
			if _, err = fc.currWriteFile.AppendRecord(r); err != nil {
				return err
			}

			if entry.ValueSize > 0 {
				newInfo.ptr = &ValuePointer{
					FileId:      fc.currWriteFile.FileId,
					valueOffset: writeOffset,
					valueSize:   entry.ValueSize,
				}
			}

			if _, updated := fc.db.table.Update(entry.Key, newInfo); updated {
				nRecordCopied++
			} else {
				fc.db.markPreviousAsStale(fc.currWriteFile.FileId, entry.ValueSize)
			}
		}
	}

	if nRecordCopied > 0 {
		if err := fc.currWriteFile.Sync(); err != nil {
			return err
		}
	}

	if err := fc.db.removeFile(file.FileId); err != nil {
		return err
	}

	log.Printf("completed compaction of file with id %d: copied %d records.\n", file.FileId, nRecordCopied)

	return fc.setCompacted(file)
}

func (fc *fileCompactor) start() {
	fc.wg.Add(1)

	go func() {
		defer fc.wg.Done()

		for {
			select {
			case file := <-fc.fileChan:
				err := fc.compactFile(file)
				if err != nil {
					log.Printf("an error occurred while compacting file %d: %s\n", file.FileId, err.Error())
				}
				fc.markAsCompacted(file.FileId)
			case <-fc.quit:
				return
			}
		}
	}()
}

func (fc *fileCompactor) stop() error {
	fc.quit <- struct{}{}
	fc.wg.Wait()
	return nil
}

func (fc *fileCompactor) markAsCompacted(fileId uint32) {
	fc.lock.Lock()
	delete(fc.compactionMap, fileId)
	fc.lock.Unlock()
}

func (fc *fileCompactor) sendFile(file *logFile) bool {
	fc.lock.Lock()
	defer fc.lock.Unlock()

	if fc.currFileId.Get() == file.FileId {
		return false
	}

	_, ok := fc.compactionMap[file.FileId]
	if !ok {
		select {
		case fc.fileChan <- file:
			fc.compactionMap[file.FileId] = struct{}{}
			return true
		default:
			return false
		}
	}
	return false
}
