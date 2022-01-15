package eagle

type Record struct {
	SeqNumber uint64
	Key       []byte
	Value     []byte
}

type ValuePointer struct {
	FileId      uint32
	frameOffset uint32
	frameSize   uint32
	keySize     byte
}

type valueLog struct {
	*dbFile
	fileId        uint32
	staleDataSize uint32
}

const valueLogFileExt = ".vlog"

func getValueLogFilename(path string, fileId uint32) string {
	return path + "/" + getFileName(fileId) + valueLogFileExt
}

func createDataFile(path string, fileId uint32) (*valueLog, error) {
	dbFile, err := createDBFile(getValueLogFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &valueLog{
		fileId: uint32(fileId),
		dbFile: dbFile,
	}, nil
}

func (file *valueLog) createRepairFile() (*valueLog, error) {
	dbFile, err := file.dbFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &valueLog{
		fileId: uint32(file.fileId),
		dbFile: dbFile,
	}, nil
}

func openDataFile(path string, fileId uint32) (*valueLog, error) {
	appendFile, err := openDBFile(getValueLogFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &valueLog{
		fileId:        uint32(fileId),
		dbFile:        appendFile,
		staleDataSize: 0,
	}, nil
}

func RecordSize(keySize int, valueSize int) uint32 {
	return uint32(4 + entryHeaderSize + keySize + valueSize)
}
