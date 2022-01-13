package eagle

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ostafen/eagle/frame"
)

type indexFile struct {
	*dbFile
	fileId uint32
	buf    bytes.Buffer
}

type indexEntryHeader struct {
	SeqNumber   uint64
	KeySize     byte
	FrameOffset uint32
	FrameSize   uint32
}

type indexEntry struct {
	SeqNumber   uint64
	FrameOffset uint32
	FrameSize   uint32
	Key         []byte
}

func serializeIndexEntry(e *indexEntry) []byte {
	var buf bytes.Buffer

	hdr := &indexEntryHeader{
		SeqNumber:   e.SeqNumber,
		KeySize:     byte(len(e.Key)),
		FrameOffset: e.FrameOffset,
		FrameSize:   e.FrameSize,
	}

	binary.Write(&buf, binary.BigEndian, hdr)
	buf.Write(e.Key)
	return buf.Bytes()
}

func deserializeIndexEntry(data []byte) (*indexEntry, error) {
	buf := bytes.NewBuffer(data)

	hdr := &indexEntryHeader{}

	err := binary.Read(buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	key := make([]byte, hdr.KeySize)
	_, err = buf.Read(key)
	if err != nil {
		return nil, err
	}

	return &indexEntry{
		SeqNumber:   hdr.SeqNumber,
		Key:         key,
		FrameOffset: hdr.FrameOffset,
		FrameSize:   hdr.FrameSize,
	}, nil
}

const indexFileExt = ".index"

func getIndexFileName(path string, fileId uint32) string {
	return path + "/" + getFileName(fileId) + indexFileExt
}

func openIndexFile(path string, fileId uint32) (*indexFile, error) {
	appendFile, err := openDBFile(getIndexFileName(path, fileId))
	if err != nil {
		return nil, err
	}

	return &indexFile{
		fileId: uint32(fileId),
		dbFile: appendFile,
	}, nil
}

func createIndexFile(path string, fileId uint32) (*indexFile, error) {
	appendFile, err := createDBFile(getIndexFileName(path, fileId))
	if err != nil {
		return nil, err
	}

	return &indexFile{
		fileId: uint32(fileId),
		dbFile: appendFile,
	}, nil
}

func (file *indexFile) createRepairFile() (*indexFile, error) {
	dbFile, err := file.dbFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &indexFile{
		fileId: uint32(file.fileId),
		dbFile: dbFile,
	}, nil
}

func (file *indexFile) AppendEntry(e *indexEntry) (int, error) {
	file.buf.Reset()

	frame.Encode(&file.buf, serializeIndexEntry(e), getCypher(getIV(file.iv, file.size)))

	return file.Write(file.buf.Bytes())
}

type indexFileIterator struct {
	file       *indexFile
	readOffset int
	seeked     bool
}

func (file *indexFile) Iterator() *indexFileIterator {
	return &indexFileIterator{
		file:       file,
		readOffset: len(file.iv),
		seeked:     false,
	}
}

func (it *indexFileIterator) HasNext() bool {
	return uint32(it.readOffset) < it.file.Size()
}

func (it *indexFileIterator) Next() (*indexEntry, error) {
	if !it.seeked {
		if _, err := it.file.Seek(int64(len(it.file.iv)), io.SeekStart); err != nil {
			return nil, err
		}
		it.seeked = true
	}

	n, data, err := frame.Decode(it.file, getCypher(getIV(it.file.iv, uint32(it.readOffset))))

	if err == nil {
		it.readOffset += n

		if uint32(it.readOffset) > it.file.Size() {
			return nil, io.ErrUnexpectedEOF
		}
	}

	return deserializeIndexEntry(data)
}

func indexEntryToDiskPointer(e *indexEntry, fileId uint32) *DiskPointer {
	return &DiskPointer{
		FileId:      fileId,
		frameOffset: uint32(e.FrameOffset),
		frameSize:   e.FrameSize,
		keySize:     byte(len(e.Key)),
	}
}
