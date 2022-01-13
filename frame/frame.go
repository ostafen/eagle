package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"

	"github.com/ostafen/eagle/crypto"
)

var ErrInvalidChecksum = errors.New("checksum error")

type frameHeader struct {
	Checksum uint32
	Size     uint32
}

const HeaderSize = 8

// checksum is computed on plain frame, even when using encryption
func computeChecksum(data []byte) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	size := uint32(len(data))
	binary.Write(crc, binary.BigEndian, &size)
	crc.Write(data)
	return crc.Sum32()
}

func writeFrameData(writer io.Writer, data []byte, cipher *crypto.Cipher) error {
	if cipher == nil {
		_, err := writer.Write(data)
		return err
	}
	cyphertext, err := cipher.Encrypt(data)
	if err != nil {
		return err
	}
	_, err = writer.Write(cyphertext)
	return err
}

func writeFrameHeader(writer io.Writer, hdr *frameHeader, cipher *crypto.Cipher) error {
	if cipher == nil {
		return binary.Write(writer, binary.BigEndian, hdr)
	}

	var buf bytes.Buffer
	binary.Write(&buf, binary.BigEndian, hdr)

	encrypted, err := cipher.Encrypt(buf.Bytes())
	if err != nil {
		return err
	}

	_, err = writer.Write(encrypted)
	return err
}

func Encode(writer io.Writer, data []byte, cipher *crypto.Cipher) (int, error) {
	hdr := &frameHeader{
		Checksum: computeChecksum(data),
		Size:     uint32(len(data)),
	}

	err := writeFrameHeader(writer, hdr, cipher)
	if err != nil {
		return -1, err
	}

	err = writeFrameData(writer, data, cipher)
	return len(data) + HeaderSize, err
}

func readFrameHeader(reader io.Reader, cipher *crypto.Cipher) (*frameHeader, error) {
	hdrBuf := make([]byte, HeaderSize)

	_, err := io.ReadFull(reader, hdrBuf)
	if err != nil {
		return nil, err
	}

	if cipher != nil {
		hdrBuf, err = cipher.Decrypt(hdrBuf)
		if err != nil {
			return nil, err
		}
	}

	return &frameHeader{
		Checksum: binary.BigEndian.Uint32(hdrBuf[:4]),
		Size:     binary.BigEndian.Uint32(hdrBuf[4:]),
	}, nil
}

func Decode(reader io.Reader, cipher *crypto.Cipher) (int, []byte, error) {
	hdr, err := readFrameHeader(reader, cipher)
	if err != nil {
		return -1, nil, err
	}

	data := make([]byte, hdr.Size)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return -1, nil, err
	}

	if cipher != nil {
		data, err = cipher.Decrypt(data)
		if err != nil {
			return -1, nil, err
		}
	}

	if computeChecksum(data) != hdr.Checksum {
		return -1, nil, ErrInvalidChecksum
	}

	return len(data) + HeaderSize, data, err
}
