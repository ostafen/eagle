package eagle

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ostafen/eagle/crypto"
	"github.com/ostafen/eagle/frame"
)

const dataRecordHeaderSize = 8 + 1

type Record struct {
	SeqNumber uint64
	Key       []byte
	Value     []byte
}

type DiskPointer struct {
	FileId      uint32
	frameOffset uint32
	frameSize   uint32
	keySize     byte
}

type DataFile struct {
	*dbFile
	fileId        uint32
	staleDataSize uint32
	buf           bytes.Buffer
}

type DataFileIterator struct {
	readOffset int
	reader     *bufio.Reader
	file       *DataFile
}

func (df *DataFile) Iterator() *DataFileIterator {
	return &DataFileIterator{
		reader:     bufio.NewReader(df),
		readOffset: len(df.iv),
		file:       df,
	}
}

func (it *DataFileIterator) HasNext() bool {
	return uint32(it.readOffset) < it.file.Size()
}

func (it *DataFileIterator) Next() (*DiskPointer, *Record, error) {
	readOffset := it.readOffset

	n, data, err := frame.Decode(it.file, getCypher(getIV(it.file.iv, uint32(readOffset))))

	if err == nil {
		it.readOffset += n

		if uint32(it.readOffset) > it.file.Size() {
			return nil, nil, io.ErrUnexpectedEOF
		}
	}

	rec, err := deserializeRecord(data)
	if err != nil {
		return nil, nil, err
	}

	ptr := &DiskPointer{
		FileId:      it.file.fileId,
		frameOffset: uint32(readOffset),
		frameSize:   uint32(n),
		keySize:     byte(len(rec.Key)),
	}

	return ptr, rec, err
}

func getDataFilename(path string, fileId uint32) string {
	return path + "/" + getFileName(fileId) + dataFileExt
}

func createDataFile(path string, fileId uint32) (*DataFile, error) {
	dbFile, err := createDBFile(getDataFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &DataFile{
		fileId: uint32(fileId),
		dbFile: dbFile,
	}, nil
}

func (file *DataFile) createRepairFile() (*DataFile, error) {
	dbFile, err := file.dbFile.createRepairFile()
	if err != nil {
		return nil, err
	}

	return &DataFile{
		fileId: uint32(file.fileId),
		dbFile: dbFile,
	}, nil
}

func openDataFile(path string, fileId uint32) (*DataFile, error) {
	appendFile, err := openDBFile(getDataFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &DataFile{
		fileId:        uint32(fileId),
		dbFile:        appendFile,
		staleDataSize: 0,
	}, nil
}

func RecordSize(keySize int, valueSize int) int {
	return frame.HeaderSize + dataRecordHeaderSize + keySize + valueSize
}

func serializeRecord(r *Record) []byte {
	var buf bytes.Buffer
	hdr := &struct {
		SeqNumber uint64
		KeySize   byte
	}{r.SeqNumber, byte(len(r.Key))}

	binary.Write(&buf, binary.BigEndian, hdr)
	buf.Write(r.Key)
	buf.Write(r.Value)
	return buf.Bytes()
}

func deserializeRecord(data []byte) (*Record, error) {
	hdr := &struct {
		SeqNumber uint64
		KeySize   byte
	}{}

	buf := bytes.NewBuffer(data)
	err := binary.Read(buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	key := make([]byte, hdr.KeySize)
	_, err = buf.Read(key)
	if err != nil {
		return nil, err
	}

	var value []byte = nil
	if buf.Len() > 0 {
		value = make([]byte, buf.Len())
		_, err = buf.Read(value)
		if err != nil {
			return nil, err
		}
	}
	return &Record{SeqNumber: hdr.SeqNumber, Key: key, Value: value}, nil
}

func (file *DataFile) AppendRecord(rec *Record) (*DiskPointer, error) {
	file.buf.Reset()

	writeOffset := file.Size()

	bytes := serializeRecord(rec)
	n, _ := frame.Encode(&file.buf, bytes, getCypher(getIV(file.iv, writeOffset)))

	ptr := &DiskPointer{
		FileId:      file.fileId,
		frameOffset: uint32(writeOffset),
		frameSize:   uint32(n),
		keySize:     byte(len(rec.Key)),
	}

	_, err := file.Write(file.buf.Bytes())
	return ptr, err
}

func (dataFile *DataFile) decryptAndGetValue(data []byte, cipher *crypto.Cipher) ([]byte, error) {
	decryptedData, err := cipher.Decrypt(data)
	if err != nil {
		return nil, err
	}

	record, err := deserializeRecord(decryptedData)
	return record.Value, err
}

func getReadOffsetAndSize(ptr *DiskPointer, useEncryption bool) (uint32, uint32) {
	if !useEncryption {
		valueOffset := uint32(ptr.frameOffset+frame.HeaderSize+dataRecordHeaderSize) + uint32(ptr.keySize)
		return valueOffset, uint32(ptr.frameOffset + ptr.frameSize - valueOffset)
	}
	return ptr.frameOffset + frame.HeaderSize, ptr.frameSize - frame.HeaderSize
}

func (dataFile *DataFile) ReadPointer(ptr *DiskPointer) ([]byte, error) {
	readOffset, size := getReadOffsetAndSize(ptr, dbOptions.UseEncryption())

	data := make([]byte, size)
	_, err := dataFile.ReadAt(data, int64(readOffset))

	if err != nil {
		return nil, err
	}

	if !dbOptions.UseEncryption() {
		return data, nil
	}

	return dataFile.decryptAndGetValue(data, getCypher(getIV(dataFile.iv, ptr.frameOffset)))
}
