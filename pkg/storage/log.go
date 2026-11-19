package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	v1 "github.com/vasenka88/cloudarch/pkg/proto/v1"
)

var (
	// MagicHeader identifies the start of a valid WAL record frame (0xCAFE).
	MagicHeader = [2]byte{0xCA, 0xFE}

	ErrInvalidMagic     = errors.New("invalid log entry magic header")
	ErrChecksumMismatch = errors.New("log entry CRC32 checksum mismatch")
	ErrCorruptEntry     = errors.New("corrupt log entry")
	ErrInvalidOffset    = errors.New("log entry offset is not monotonic")
)

// LogEntry is the core replication log record.
type LogEntry = v1.LogEntry

// ComputePayloadChecksum calculates the IEEE CRC32 checksum of a data payload.
func ComputePayloadChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// NewLogEntry constructs a new LogEntry with monotonic offset, timestamp, and computed checksum.
func NewLogEntry(offset uint64, term uint64, opType, table, partitionKey, sortKey string, value []byte) *LogEntry {
	return &LogEntry{
		Offset:       offset,
		Term:         term,
		Timestamp:    time.Now().UnixNano(),
		OpType:       opType,
		Table:        table,
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		Value:        value,
		Checksum:     ComputePayloadChecksum(value),
	}
}

// EncodeEntry serializes a LogEntry into a framed binary byte slice:
// [Magic (2B)] [Length (4B Big-Endian)] [CRC32 (4B Big-Endian)] [Protobuf Payload (LB)]
func EncodeEntry(entry *LogEntry) ([]byte, error) {
	if entry == nil {
		return nil, errors.New("cannot encode nil log entry")
	}

	payload, err := proto.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal log entry: %w", err)
	}

	length := uint32(len(payload))
	checksum := crc32.ChecksumIEEE(payload)

	buf := make([]byte, 2+4+4+len(payload))
	buf[0] = MagicHeader[0]
	buf[1] = MagicHeader[1]
	binary.BigEndian.PutUint32(buf[2:6], length)
	binary.BigEndian.PutUint32(buf[6:10], checksum)
	copy(buf[10:], payload)

	return buf, nil
}

// DecodeEntry reads a single framed LogEntry from an io.Reader.
func DecodeEntry(r io.Reader) (*LogEntry, error) {
	var header [10]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	if header[0] != MagicHeader[0] || header[1] != MagicHeader[1] {
		return nil, ErrInvalidMagic
	}

	length := binary.BigEndian.Uint32(header[2:6])
	expectedChecksum := binary.BigEndian.Uint32(header[6:10])

	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("failed to read log payload of length %d: %w", length, err)
	}

	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, ErrChecksumMismatch
	}

	var entry LogEntry
	if err := proto.Unmarshal(payload, &entry); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal log entry protobuf: %v", ErrCorruptEntry, err)
	}

	return &entry, nil
}

// DecodeEntryFromBytes parses a single framed LogEntry from a byte slice.
// Returns the decoded entry and the number of bytes consumed from buf.
func DecodeEntryFromBytes(buf []byte) (*LogEntry, int, error) {
	if len(buf) < 10 {
		return nil, 0, io.ErrUnexpectedEOF
	}

	if buf[0] != MagicHeader[0] || buf[1] != MagicHeader[1] {
		return nil, 0, ErrInvalidMagic
	}

	length := binary.BigEndian.Uint32(buf[2:6])
	expectedChecksum := binary.BigEndian.Uint32(buf[6:10])
	totalSize := 10 + int(length)

	if len(buf) < totalSize {
		return nil, 0, io.ErrUnexpectedEOF
	}

	payload := buf[10:totalSize]
	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, 0, ErrChecksumMismatch
	}

	var entry LogEntry
	if err := proto.Unmarshal(payload, &entry); err != nil {
		return nil, 0, fmt.Errorf("%w: failed to unmarshal log entry protobuf: %v", ErrCorruptEntry, err)
	}

	return &entry, totalSize, nil
}
