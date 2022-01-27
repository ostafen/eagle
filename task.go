package eagle

import (
	"log"
	"sync"
)

type keyFileLoader struct {
	db *DB

	fileChan chan *keyLog
	err      error
	wg       *sync.WaitGroup

	nProcessed   int
	nTombstones  int
	maxSeqNumber uint64
}

func (task *keyFileLoader) loadFile(file *keyLog) error {
	it := file.Iterator()

	nProcessed := 0
	nTombstones := 0
	for it.HasNext() {
		e, _, err := it.Next()
		if err != nil {
			log.Println("error ", file.fileId, file.size, it.readOffset)
			return err
		}

		nProcessed++
		ptr := getPointerFromEntry(e, file.fileId)

		var prevInfo *recordInfo
		rInfo := &recordInfo{seqNumber: e.SeqNumber, ptr: ptr}

		if e.ValueSize == 0 {
			rInfo = rInfo.markDeleted(e.SeqNumber)
			nTombstones++
		}

		prevInfo, _ = task.db.table.Update(e.Key, rInfo)

		if prevInfo != nil {
			task.db.markPreviousAsStale(file.fileId, recordSize(len(e.Key), int(e.ValueSize)))
		}

		if e.SeqNumber > task.maxSeqNumber {
			task.maxSeqNumber = e.SeqNumber
		}
	}

	task.nProcessed += nProcessed
	task.nTombstones += nTombstones
	return nil
}

const chanSize = 100

func newKeyFileProcessTask(db *DB, wg *sync.WaitGroup) *keyFileLoader {
	return &keyFileLoader{
		db:           db,
		wg:           wg,
		err:          nil,
		fileChan:     make(chan *keyLog, chanSize),
		maxSeqNumber: 0,
	}
}

func (task *keyFileLoader) start() {
	go func() {
		defer task.wg.Done()

		for {
			file := <-task.fileChan
			if file == nil { // quit signal
				return
			}

			err := task.loadFile(file)
			if err != nil {
				task.err = err
				return
			}
		}
	}()
}
