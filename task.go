package eagle

import (
	"log"
	"sync"
)

type indexFileTask struct {
	db *DB

	fileChan chan *indexFile
	err      error
	wg       *sync.WaitGroup

	maxSeqNumber uint64
}

func (task *indexFileTask) processFile(file *indexFile) error {
	it := file.Iterator()

	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			log.Println("error", err)
			return err
		}

		ptr := indexEntryToDiskPointer(e, file.fileId)

		swapPtr, _ := task.db.table.Swap(e.Key, e.SeqNumber, ptr)
		if swapPtr != nil {
			task.db.markPreviousAsStale(file.fileId, swapPtr.frameSize)
		}

		if e.SeqNumber > task.maxSeqNumber {
			task.maxSeqNumber = e.SeqNumber
		}
	}

	return nil
}

const chanSize = 100

func newIndexTask(db *DB, wg *sync.WaitGroup) *indexFileTask {
	return &indexFileTask{
		db:           db,
		wg:           wg,
		err:          nil,
		fileChan:     make(chan *indexFile, chanSize),
		maxSeqNumber: 0,
	}
}

func (task *indexFileTask) start() {
	go func() {
		defer task.wg.Done()

		for {
			file := <-task.fileChan
			if file == nil { // quit signal
				return
			}

			err := task.processFile(file)
			if err != nil {
				task.err = err
				return
			}
		}
	}()
}

type tombstoneFileTask struct {
	db *DB

	fileChan             chan string
	err                  error
	wg                   *sync.WaitGroup
	nProcessedTombstones int
	maxSeqNumber         uint64
}

func newTombstoneFileTask(db *DB, wg *sync.WaitGroup) *tombstoneFileTask {
	return &tombstoneFileTask{
		db:           db,
		wg:           wg,
		err:          nil,
		fileChan:     make(chan string, chanSize),
		maxSeqNumber: 0,
	}
}

func (task *tombstoneFileTask) processFile(filename string) error {
	fileId, _ := getFileId(filename)

	file, err := openTombstoneFile(task.db.rootDir, fileId)
	if err != nil {
		return err
	}

	it := file.Iterator()

	nProcessed := 0
	for it.HasNext() {
		e, err := it.Next()
		if err != nil {
			log.Println("tombstone", err, nProcessed)
			return err
		}

		nProcessed++

		ptr, seqNum := task.db.table.Get(e.Key)
		if ptr != nil && e.SeqNumber >= seqNum { // ptr should always be != nil
			task.db.table.Remove(e.Key)
			task.db.markPreviousAsStale(ptr.FileId, ptr.frameSize)
		}

		if e.SeqNumber > task.maxSeqNumber {
			task.maxSeqNumber = e.SeqNumber
		}
	}

	task.nProcessedTombstones = nProcessed
	return nil
}

func (task *tombstoneFileTask) start() {
	go func() {
		defer task.wg.Done()

		for {
			filename := <-task.fileChan
			if filename == "quit" { // quit signal
				return
			}

			err := task.processFile(filename)
			if err != nil {
				task.err = err
				return
			}
		}
	}()
}
