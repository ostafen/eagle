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
	errKeySize        = errors.New("key is too large")
	errGetMaxAttempts = errors.New("max get attempts exceeded")
)

var dbOptions Options

type DB struct {
	rootDir string

	manifest      *manifest
	writeLock     sync.Mutex
	nextSeqNumber uint64
	nextFileId    util.AtomicInt32

	currWriteFile *logFile

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

func (db *DB) createNewLogFile() (*logFile, error) {
	newFile, err := createLogFile(db.rootDir, db.getNextFileId())
	if err != nil {
		return nil, err
	}
	db.addFileToMap(newFile)
	return newFile, nil
}

func (db *DB) rotateLogFile() error {
	if db.currWriteFile != nil {
		if err := db.currWriteFile.Sync(); err != nil {
			return err
		}
	}

	newFile, err := db.createNewLogFile()
	if err != nil {
		return err
	}
	db.currWriteFile = newFile
	return nil
}

func (db *DB) compactIfNeeded(fileId uint32, fileStaleSize uint32, fileSize uint32) {
	if float64(fileStaleSize) >= float64(fileSize)*db.opts.FileCompactionThreshold {
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

func (db *DB) openLogFiles() (uint32, error) {
	filenames, err := util.ListDir(db.rootDir, valueLogFileExt)
	if err != nil {
		return 0, err
	}

	maxFileId := uint32(0)
	for _, filename := range filenames {
		fileId, _ := getFileId(filename)
		file, err := openLogFile(db.rootDir, fileId)
		if err != nil {
			return 0, err
		}

		db.fileMap[fileId] = file

		if fileId > maxFileId {
			maxFileId = fileId
		}
	}
	return maxFileId, nil
}

func (db *DB) processIndexFiles() error {
	nTasks := runtime.NumCPU()
	tasks := make([]*keyFileLoader, nTasks)

	wg := &sync.WaitGroup{}
	wg.Add(nTasks)
	for i := 0; i < nTasks; i++ {
		tasks[i] = newKeyFileProcessTask(db, wg)
		tasks[i].start()
	}

	i := 0
	for _, file := range db.fileMap {
		tasks[i].fileChan <- file.keyFile
		i = (i + 1) % nTasks
	}

	for i := 0; i < nTasks; i++ {
		tasks[i].fileChan <- nil
	}

	start := time.Now()
	// waiting for all loaders to exit successfully
	wg.Wait()

	nProcessed := 0
	nTombstones := 0
	for _, t := range tasks {
		if t.err != nil {
			return t.err
		}

		nProcessed += t.nProcessed
		nTombstones += t.nTombstones
		if t.maxSeqNumber > db.nextSeqNumber {
			db.nextSeqNumber = t.maxSeqNumber
		}
	}

	loadTime := time.Since(start)
	log.Printf("loaded %d elements in %f seconds", db.table.Size(), loadTime.Seconds())
	log.Printf("found %d tombstones\n", nTombstones)
	log.Printf("total processed: %d\n", nProcessed)
	return nil
}

func (db *DB) recoverFromDisk() error {
	return db.processIndexFiles()
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

	maxFileId, err := db.openLogFiles()
	if err != nil {
		return nil, err
	}
	db.nextFileId.Set(int32(maxFileId))

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
		if err == nil {
			db.fileMap[repairedFile.FileId] = repairedFile
		}
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

	if err := db.closeAllFiles(); err != nil {
		db.manifest.IOError = true
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

func (db *DB) writeRecordToFile(r *Record) (*ValuePointer, error) {
	if err := db.ensureRoomForWrite(uint32(recordSize(len(r.Key), len(r.Value)))); err != nil {
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
	oldInfo, updated := db.table.Update(key, &recordInfo{ptr: ptr, seqNumber: seqNumber})
	if !updated {
		panic("key has not been updated")
	}

	if oldInfo != nil {
		db.markPreviousAsStale(oldInfo.ptr.FileId, recordSize(len(key), int(oldInfo.ptr.valueSize)))
	}
	return err
}

func (db *DB) readFile(ptr *ValuePointer) ([]byte, error) {
	db.fileMapLock.Lock()
	file := db.fileMap[ptr.FileId]
	db.fileMapLock.Unlock()

	return file.ReadPointer(ptr)
}

const (
	maxGetAttempts        = 5
	getAttemptSleepMillis = 100
)

// Get returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
func (db *DB) get(key []byte, attempt int) ([]byte, error) {
	if attempt > maxGetAttempts {
		return nil, errGetMaxAttempts
	}

	info := db.table.Get(key)
	if info == nil || info.deleted() {
		return nil, nil
	}

	value, err := db.readFile(info.ptr)
	if errors.Is(err, os.ErrClosed) {
		time.Sleep(time.Millisecond * getAttemptSleepMillis)
		return db.get(key, attempt+1)
	}
	return value, err
}

// Get returns the value to which the specified key is mapped, or null if this map contains no mapping for the key.
func (db *DB) Get(key []byte) ([]byte, error) {
	return db.get(key, 0)
}

// Remove removes the mapping for a key from the map if it is present.
func (db *DB) Remove(key []byte) error {
	db.writeLock.Lock()
	defer db.writeLock.Unlock()

	seqNumber := db.getSeqNumber()
	info := db.table.MarkDeleted(key, seqNumber)
	if info == nil {
		return nil
	}

	db.markPreviousAsStale(info.ptr.FileId, recordSize(len(key), int(info.ptr.valueSize)))
	_, err := db.writeRecordToFile(&Record{Key: key, Value: nil, SeqNumber: seqNumber})
	return err
}

// ContainsKey returns true if this map contains a mapping for the specified key.
func (db *DB) ContainsKey(key []byte) bool {
	return db.table.ContainsKey(key)
}

// Size returns the numbers of mappings actually stored in the database.
func (db *DB) Size() int {
	return db.table.Size()
}
