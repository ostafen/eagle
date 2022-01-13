package eagle

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/ostafen/eagle/frame"
)

type tombstoneFile struct {
	fileId uint32
	*dbFile
}

type tombstoneEntry struct {
	SeqNumber uint64
	Key       []byte
}

func tombstoneFrameSize(keySize int) uint32 {
	return uint32(frame.HeaderSize + 9 + keySize)
}

func getTombstoneFilename(path string, fileId uint32) string {
	return path + "/" + getFileName(fileId) + tombstoneFileExt
}

func openTombstoneFile(path string, fileId uint32) (*tombstoneFile, error) {
	file, err := openDBFile(getTombstoneFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &tombstoneFile{
		fileId: fileId,
		dbFile: file,
	}, nil
}

func createTombstoneFile(path string, fileId uint32) (*tombstoneFile, error) {
	file, err := createDBFile(getTombstoneFilename(path, fileId))
	if err != nil {
		return nil, err
	}

	return &tombstoneFile{
		fileId: fileId,
		dbFile: file,
	}, nil
}

func serializeTombstoneEntry(e *tombstoneEntry) []byte {
	var buf bytes.Buffer
	hdr := &struct {
		SeqNumber uint64
		KeySize   byte
	}{e.SeqNumber, byte(len(e.Key))}

	binary.Write(&buf, binary.BigEndian, hdr)
	buf.Write(e.Key)
	return buf.Bytes()
}

func deserializeTombstoneEntry(data []byte) (*tombstoneEntry, error) {
	buf := bytes.NewBuffer(data)
	hdr := &struct {
		SeqNumber uint64
		KeySize   byte
	}{}

	err := binary.Read(buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	key := make([]byte, hdr.KeySize)
	_, err = buf.Read(key)
	if err != nil {
		return nil, err
	}
	return &tombstoneEntry{SeqNumber: hdr.SeqNumber, Key: key}, nil
}

func (tf *tombstoneFile) WriteEntry(e *tombstoneEntry) error {
	_, err := frame.Encode(tf, serializeTombstoneEntry(e), getCypher(getIV(tf.iv, tf.size)))
	return err
}

type tombstoneFileIterator struct {
	file       *tombstoneFile
	readOffset uint32
}

func (tf *tombstoneFile) Iterator() *tombstoneFileIterator {
	return &tombstoneFileIterator{
		file:       tf,
		readOffset: uint32(len(tf.iv)),
	}
}

func (it *tombstoneFileIterator) HasNext() bool {
	return it.readOffset < it.file.size
}

// TODO: use a buffered reader in iterator

func (it *tombstoneFileIterator) Next() (*tombstoneEntry, error) {
	n, data, err := frame.Decode(it.file, getCypher(getIV(it.file.iv, it.readOffset)))

	if err != nil {
		return nil, err
	}

	it.readOffset += uint32(n)
	if uint32(it.readOffset) > it.file.Size() {
		return nil, io.ErrUnexpectedEOF
	}
	return deserializeTombstoneEntry(data)
}
