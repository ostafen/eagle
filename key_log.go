package eagle

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/ostafen/eagle/crypto"
)

type keyLog struct {
	*dbFile
	fileId uint32
	buf    bytes.Buffer
}

const entryHeaderSize = 8 + 1 + 4 + 4

type klEntryHeader struct {
	SeqNumber   uint64
	KeySize     byte
	ValueOffset uint32
	ValueSize   uint32
}

type klEntry struct {
	SeqNumber   uint64
	ValueOffset uint32
	ValueSize   uint32
	Key         []byte
}

const keyLogFileExt = ".klog"

func getKeyLogFile(path string, fileId uint32) string {
	return path + "/" + getFileName(fileId) + keyLogFileExt
}

func openKeyLogFile(path string, fileId uint32) (*keyLog, error) {
	appendFile, err := openDBFile(getKeyLogFile(path, fileId))
	if err != nil {
		return nil, err
	}

	return &keyLog{
		fileId: uint32(fileId),
		dbFile: appendFile,
	}, nil
}

func createKeyLogFile(path string, fileId uint32) (*keyLog, error) {
	appendFile, err := createDBFile(getKeyLogFile(path, fileId))
	if err != nil {
		return nil, err
	}

	return &keyLog{
		fileId: uint32(fileId),
		dbFile: appendFile,
	}, nil
}

func (file *keyLog) createRepairFile() (*keyLog, error) {
	dbFile, err := file.dbFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &keyLog{
		fileId: uint32(file.fileId),
		dbFile: dbFile,
	}, nil
}

func computeChecksum(hdr *klEntryHeader, key []byte, value []byte) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	binary.Write(crc, binary.BigEndian, hdr)
	crc.Write(key)
	crc.Write(value)
	return crc.Sum32()
}

func serializeHeader(checksum uint32, hdr *klEntryHeader, cipher *crypto.Cipher) ([]byte, error) {
	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, &checksum)
	binary.Write(&buf, binary.BigEndian, hdr)

	bytes := buf.Bytes()
	if cipher != nil {
		var err error
		if bytes, err = cipher.Encrypt(bytes); err != nil {
			return nil, err
		}
	}
	return bytes, nil
}

func getHeaderFromEntry(e *klEntry) *klEntryHeader {
	return &klEntryHeader{
		SeqNumber:   e.SeqNumber,
		KeySize:     byte(len(e.Key)),
		ValueOffset: e.ValueOffset,
		ValueSize:   e.ValueSize,
	}
}

func (file *keyLog) AppendEntry(e *klEntry, value []byte) (int, error) {
	file.buf.Reset()

	hdr := getHeaderFromEntry(e)

	cipher := getCypher(getIV(file.iv, file.size))

	checksum := computeChecksum(hdr, e.Key, value)
	headerBytes, err := serializeHeader(checksum, hdr, cipher)
	if err != nil {
		return -1, err
	}

	key := e.Key
	if cipher != nil {
		key, _ = cipher.Encrypt(key)
	}

	file.buf.Write(headerBytes)
	file.buf.Write(key)

	return file.Write(file.buf.Bytes())
}

type keyFileIterator struct {
	file       *keyLog
	readOffset int
	seeked     bool
}

func (file *keyLog) Iterator() *keyFileIterator {
	return &keyFileIterator{
		file:       file,
		readOffset: len(file.iv),
		seeked:     false,
	}
}

func (it *keyFileIterator) HasNext() bool {
	return uint32(it.readOffset) < it.file.Size()
}

func readHeader(reader io.Reader, cipher *crypto.Cipher) (*klEntryHeader, uint32, error) {
	headerBytes := make([]byte, entryHeaderSize+4)
	_, err := io.ReadFull(reader, headerBytes)
	if err != nil {
		return nil, 0, err
	}

	if cipher != nil {
		headerBytes, err = cipher.Decrypt(headerBytes)
		if err != nil {
			return nil, 0, err
		}
	}

	var buf bytes.Buffer
	buf.Write(headerBytes)

	hdr := &klEntryHeader{}
	var checksum uint32
	binary.Read(&buf, binary.BigEndian, &checksum)
	binary.Read(&buf, binary.BigEndian, hdr)
	return hdr, checksum, nil
}

func (it *keyFileIterator) Next() (*klEntry, uint32, error) {
	if !it.seeked {
		if _, err := it.file.Seek(int64(len(it.file.iv)), io.SeekStart); err != nil {
			return nil, 0, err
		}
		it.seeked = true
	}

	cipher := getCypher(getIV(it.file.iv, uint32(it.readOffset)))

	hdr, checksum, err := readHeader(it.file, cipher)
	if err != nil {
		return nil, 0, err
	}

	key := make([]byte, hdr.KeySize)
	if _, err = io.ReadFull(it.file, key); err != nil {
		return nil, 0, err
	}

	if cipher != nil {
		if key, err = cipher.Decrypt(key); err != nil {
			return nil, 0, err
		}
	}

	it.readOffset += int(entryHeaderSize + 4 + hdr.KeySize)

	if uint32(it.readOffset) > it.file.Size() {
		return nil, 0, io.ErrUnexpectedEOF
	}

	return &klEntry{
		SeqNumber:   hdr.SeqNumber,
		ValueOffset: hdr.ValueOffset,
		ValueSize:   hdr.ValueSize,
		Key:         key,
	}, checksum, nil
}

func getPointerFromEntry(e *klEntry, fileId uint32) *ValuePointer {
	return &ValuePointer{
		FileId:      fileId,
		valueOffset: uint32(e.ValueOffset),
		valueSize:   e.ValueSize,
		//keySize:     byte(len(e.Key)),
	}
}
