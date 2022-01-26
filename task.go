package eagle

import (
	"log"
	"sync"
)

type keyFileProcessTask struct {
	db *DB

	fileChan chan *keyLog
	err      error
	wg       *sync.WaitGroup

	maxSeqNumber uint64
}

func (task *keyFileProcessTask) processFile(file *keyLog) error {
	it := file.Iterator()

	for it.HasNext() {
		e, _, err := it.Next()
		if err != nil {
			log.Println("error", err)
			return err
		}

		ptr := getPointerFromEntry(e, file.fileId)

		var prevInfo *recordInfo

		if e.ValueSize > 0 {
			prevInfo, _ = task.db.table.Update(e.Key, &recordInfo{seqNumber: e.SeqNumber, ptr: ptr})
		} else {
			prevInfo = task.db.table.Remove(e.Key, e.SeqNumber)
		}

		if prevInfo != nil {
			task.db.markPreviousAsStale(file.fileId, recordSize(len(e.Key), int(e.ValueSize)))
		}

		if e.SeqNumber > task.maxSeqNumber {
			task.maxSeqNumber = e.SeqNumber
		}
	}

	return nil
}

const chanSize = 100

func newKeyFileProcessTask(db *DB, wg *sync.WaitGroup) *keyFileProcessTask {
	return &keyFileProcessTask{
		db:           db,
		wg:           wg,
		err:          nil,
		fileChan:     make(chan *keyLog, chanSize),
		maxSeqNumber: 0,
	}
}

func (task *keyFileProcessTask) start() {
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
