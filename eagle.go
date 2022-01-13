package eagle

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ostafen/eagle/util"
)

var (
	errKeySize         = errors.New("key is too large")
	errNoEncryptionKey = errors.New("no encryption key provided")
)

var dbOptions Options

type DB struct {
	rootDir string

	manifest      *manifest
	writeLock     sync.Mutex
	nextSeqNumber uint64
	nextFileId    util.AtomicInt32

	currTombstoneFile *tombstoneFile
	currWriteFile     *logFile

	compactor   *fileCompactor
	fileMapLock sync.Mutex
	fileMap     map[uint32]*logFile

	staleDataMap map[uint32]uint32
	table        *memTable
	closed       bool
	opts         Options
}

func getFileName(fileId uint32) string {
	return zeroPad(strconv.Itoa(int(fileId)), fileNameLen)
}

func getFileId(fileName string) (uint32, error) {
	baseName := filepath.Base(fileName)
	baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
	v, err := strconv.Atoi(baseName)
	return uint32(v), err
}

func zeroPad(s string, n int) string {
	for len(s) < n {
		s = "0" + s
	}
	return s
}

func (db *DB) rotateLogFile() error {
	if db.currWriteFile != nil {
		if err := db.currWriteFile.Sync(); err != nil {
			return err
		}
	}

	newFile, err := createLogFile(db.rootDir, db.getNextFileId())
	if err != nil {
		return err
	}
	db.currWriteFile = newFile
	db.addFileToMap(newFile)
	return nil
}

func (db *DB) rotateTombstoneFile() error {
	if db.currTombstoneFile != nil {
		if err := db.currTombstoneFile.SyncAndClose(); err != nil {
			return err
		}
	}

	newFile, err := createTombstoneFile(db.rootDir, db.getNextFileId())
	if err != nil {
		return err
	}
	db.currTombstoneFile = newFile
	return nil
}

func (db *DB) compactIfNeeded(fileId uint32, fileStaleSize uint32, fileSize uint32) {
	if float32(fileStaleSize) >= float32(fileSize)*db.opts.FileCompactionThreshold {
		if db.compactor.sendFile(db.fileMap[fileId]) {
			delete(db.staleDataMap, fileId)
		}
	}
}

func (db *DB) markPreviousAsStale(fileId, size uint32) {
	db.fileMapLock.Lock()
	defer db.fileMapLock.Unlock()

	file, hasFile := db.fileMap[fileId]
	if !hasFile {
		return
	}

	db.staleDataMap[fileId] += size
	if db.currWriteFile == nil || fileId != db.currWriteFile.FileId {
		db.compactIfNeeded(fileId, db.staleDataMap[fileId], uint32(file.Size()))
	}
}

func (db *DB) addFileToMap(file *logFile) {
	db.fileMapLock.Lock()
	db.fileMap[file.FileId] = file
	db.fileMapLock.Unlock()
}

func (db *DB) openLogFiles() error {
	filenames, err := util.ListDir(db.rootDir, dataFileExt)
	if err != nil {
		return err
	}

	for _, filename := range filenames {
		fileId, _ := getFileId(filename)
		file, err := openLogFile(db.rootDir, fileId)
		if err != nil {
			return err
		}

		db.fileMap[fileId] = file
	}
	return nil
}

func (db *DB) processIndexFiles() error {
	nTasks := runtime.NumCPU()
	tasks := make([]*indexFileTask, nTasks)

	wg := &sync.WaitGroup{}
	wg.Add(nTasks)
	for i := 0; i < nTasks; i++ {
		tasks[i] = newIndexTask(db, wg)
		tasks[i].start()
	}

	i := 0
	for _, file := range db.fileMap {
		tasks[i].fileChan <- file.indexFile
		i = (i + 1) % nTasks
	}

	for i := 0; i < nTasks; i++ {
		tasks[i].fileChan <- nil
	}

	start := time.Now()
	// waiting for all loaders to exit successfully
	wg.Wait()

	for _, t := range tasks {
		if t.err != nil {
			return t.err
		}

		if t.maxSeqNumber > db.nextSeqNumber {
			db.nextSeqNumber = t.maxSeqNumber
		}
	}

	loadTime := time.Since(start)
	log.Printf("loaded %d elements in %f seconds", db.table.Size(), loadTime.Seconds())
	return nil
}

func (db *DB) processTombstoneFiles() error {
	filenames, err := util.ListDir(db.rootDir, tombstoneFileExt)
	if err != nil {
		return err
	}

	nTasks := runtime.NumCPU()
	tasks := make([]*tombstoneFileTask, nTasks)

	wg := &sync.WaitGroup{}
	wg.Add(nTasks)
	for i := 0; i < nTasks; i++ {
		tasks[i] = newTombstoneFileTask(db, wg)
		tasks[i].start()
	}

	i := 0
	for _, file := range filenames {
		tasks[i].fileChan <- file
		i = (i + 1) % nTasks
	}

	for i := 0; i < nTasks; i++ {
		tasks[i].fileChan <- "quit"
	}

	// waiting for all loaders to exit successfully
	wg.Wait()

	for _, t := range tasks {
		if t.err != nil {
			return t.err
		}

		if t.maxSeqNumber > db.nextSeqNumber {
			db.nextSeqNumber = t.maxSeqNumber
		}
	}
	return nil

}

func (db *DB) recoverFromDisk() error {
	if err := db.processIndexFiles(); err != nil {
		return err
	}

	return db.processTombstoneFiles()
}

// Open create a new Eagle instance in the target directory and loads existing data from disk.
func Open(opts Options) (*DB, error) {
	if err := util.MakeDirIfNotExists(opts.RootDir); err != nil {
		return nil, err
	}

	dbOptions = opts

	db := &DB{
		closed:       false,
		fileMap:      make(map[uint32]*logFile),
		staleDataMap: make(map[uint32]uint32),
		opts:         opts,
		rootDir:      opts.RootDir,
		table:        newMemTable(),
	}
	db.compactor = newFileCompactor(db)

	err := db.openLogFiles()
	if err != nil {
		return nil, err
	}

	manifest, err := openManifestFile(db.rootDir)
	if err != nil {
		return nil, err
	}
	db.manifest = manifest

	if manifest.DBOpen || manifest.IOError {
		log.Println("DB was not correctly closed. Repairing all files.", manifest.DBOpen, manifest.IOError)

		if err := db.repairFiles(); err != nil {
			return nil, err
		}
	}

	manifest.DBOpen = true
	manifest.IOError = false
	if err := manifest.Save(db.rootDir); err != nil {
		return nil, err
	}

	if err := db.recoverFromDisk(); err != nil {
		return nil, err
	}

	db.nextSeqNumber++

	db.compactor.start()

	return db, nil
}

func (db *DB) removeFile(fileId uint32) error {
	db.fileMapLock.Lock()
	defer db.fileMapLock.Unlock()

	file, ok := db.fileMap[fileId]
	if ok {
		delete(db.fileMap, fileId)
		return file.remove()
	}
	return nil
}

func (db *DB) repairLastCompactedFile() error {
	compactRegistry, _ := readCompactFile(db.rootDir)
	if compactRegistry.compacting {
		file, ok := db.fileMap[uint32(compactRegistry.currWriteFileId)]
		if ok {
			repairedFile, err := file.repair()
			db.fileMap[repairedFile.FileId] = repairedFile
			return err
		}
	} else {
		db.removeFile(uint32(compactRegistry.lastCompactedFileId)) // ignore errors
	}
	return nil
}

func (db *DB) repairFiles() error {
	file := db.getLatestFile()

	if file != nil {
		repairedFile, err := file.repair()
		db.fileMap[repairedFile.FileId] = repairedFile
		return err
	}

	return db.repairLastCompactedFile()
}

func (db *DB) closeAllFiles() error {
	db.fileMapLock.Lock()
	for _, file := range db.fileMap {
		err := file.Close()
		if err != nil {
			return err
		}
	}
	db.fileMapLock.Unlock()
	return nil
}

func (db *DB) getLatestFile() *logFile {
	db.fileMapLock.Lock()
	defer db.fileMapLock.Unlock()

	maxFileId := -1
	var latestFile *logFile = nil
	for fileId, file := range db.fileMap {
		if int(fileId) > maxFileId {
			maxFileId = int(fileId)
			latestFile = file
		}
	}
	return latestFile
}

// Close closed the current instance of the db. Any subsequent map operation will no more be synched on disk
func (db *DB) Close() error {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	db.compactor.stop()

	if db.currWriteFile != nil {
		if err := db.currWriteFile.Sync(); err != nil { // file will be closed in closeAllFiles
			db.manifest.IOError = true
		}
	}

	db.closeAllFiles()

	if db.currTombstoneFile != nil {
		if err := db.currTombstoneFile.SyncAndClose(); err != nil {
			db.manifest.IOError = true
		}
	}

	db.manifest.DBOpen = false
	return db.manifest.Save(db.rootDir)
}

func (db *DB) getSeqNumber() uint64 {
	seqNum := db.nextSeqNumber
	db.nextSeqNumber++
	return seqNum
}

func (db *DB) getNextFileId() uint32 {
	return uint32(db.nextFileId.Inc())
}

func (db *DB) ensureRoomForWrite(size uint32) error {
	if db.currWriteFile == nil || db.currWriteFile.Size()+size > db.opts.MaxFileSize {
		if err := db.rotateLogFile(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) writeRecordToFile(r *Record) (*DiskPointer, error) {
	if err := db.ensureRoomForWrite(uint32(RecordSize(len(r.Key), len(r.Value)))); err != nil {
		return nil, err
	}
	return db.currWriteFile.AppendRecord(r)
}

// Associates the specified value with the specified key in this map (optional operation).
// If the map previously contained a mapping for the key, the old value is replaced by the specified value.
func (db *DB) Put(key []byte, value []byte) error {
	if len(key) > keySizeMax {
		return errKeySize
	}

	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	seqNumber := db.getSeqNumber()
	ptr, err := db.writeRecordToFile(&Record{Key: key, Value: value, SeqNumber: seqNumber})
	oldPtr, _ := db.table.Swap(key, seqNumber, ptr)
	if oldPtr != nil {
		db.markPreviousAsStale(oldPtr.FileId, oldPtr.frameSize)
	}
	return err
}

func (db *DB) readFile(ptr *DiskPointer) ([]byte, error) {
	db.fileMapLock.Lock()
	file := db.fileMap[ptr.FileId]
	db.fileMapLock.Unlock()

	return file.ReadPointer(ptr)
}

// Get returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
func (db *DB) Get(key []byte) ([]byte, error) {
	if len(key) > keySizeMax {
		return nil, nil
	}

	ptr, _ := db.table.Get(key)
	if ptr == nil {
		return nil, nil
	}

	value, err := db.readFile(ptr)
	if errors.Is(err, os.ErrClosed) {
		// retry
	}
	return value, err
}

func (db *DB) ensureRoomForWriteTombstone(entrySize uint32) error {
	if db.currTombstoneFile == nil || db.currTombstoneFile.Size()+entrySize > db.opts.MaxTombstoneFileSize {
		return db.rotateTombstoneFile()
	}
	return nil
}

func (db *DB) writeTombstone(key []byte, seqNumber uint64) error {
	e := tombstoneEntry{Key: key, SeqNumber: seqNumber}

	if err := db.ensureRoomForWriteTombstone(tombstoneFrameSize(len(key))); err != nil {
		return err
	}

	return db.currTombstoneFile.WriteEntry(&e)
}

// Remove removes the mapping for a key from the map if it is present.
func (db *DB) Remove(key []byte) error {
	if len(key) > keySizeMax {
		return errKeySize
	}
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	ptr := db.table.Remove(key)
	if ptr == nil {
		return nil
	}

	db.markPreviousAsStale(ptr.FileId, ptr.frameSize)
	return db.writeTombstone(key, db.getSeqNumber())
}

// ContainsKey returns true if this map contains a mapping for the specified key.
func (e *DB) ContainsKey(key []byte) bool {
	return e.table.ContainsKey(key)
}
