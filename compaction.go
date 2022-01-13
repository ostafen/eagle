package eagle

import (
	"log"
	"sync"
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

	currWriteFile *logFile
	lock          sync.Mutex

	compactionMap map[uint32]struct{}
}

func newFileCompactor(db *DB) *fileCompactor {
	return &fileCompactor{
		db:            db,
		quit:          make(chan struct{}, 1),
		fileChan:      make(chan *logFile, 100),
		currWriteFile: nil,
	}
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

func (fc *fileCompactor) compactFile(file *logFile) error {
	if err := fc.setCompacting(file); err != nil {
		return err
	}

	nProcessed := 0
	it := file.indexFile.Iterator()
	for it.HasNext() {
		_, err := it.Next()

		if err != nil {
			log.Println("err: ", err)
			return err
		}

		nProcessed++
	}

	log.Println("compaction: processed ", nProcessed)

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

func (fc *fileCompactor) closeFile() error {
	if fc.currWriteFile != nil {
		return fc.currWriteFile.SyncAndClose()
	}
	return nil
}

func (fc *fileCompactor) stop() error {
	fc.quit <- struct{}{}
	fc.wg.Wait()
	return fc.closeFile()
}

func (fc *fileCompactor) markAsCompacted(fileId uint32) {
	fc.lock.Lock()
	delete(fc.compactionMap, fileId)
	fc.lock.Unlock()
}

func (fc *fileCompactor) sendFile(file *logFile) bool {

	fc.lock.Lock()
	defer fc.lock.Unlock()

	_, ok := fc.compactionMap[file.FileId]
	if !ok {
		select {
		case fc.fileChan <- file:
			return true
		default:
			return false
		}
	}
	return true
}
