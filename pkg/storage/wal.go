package storage

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrOffsetMismatch = errors.New("append offset mismatch: offsets must be strictly monotonic")
	ErrWALClosed       = errors.New("WAL is closed")
)

// WAL represents a durable append-only Write-Ahead Log.
type WAL struct {
	mutex      sync.RWMutex
	filePath   string
	file       *os.File
	lastOffset uint64
	closed     bool
}

// OpenWAL opens or creates a WAL file at the specified path and performs crash recovery.
func OpenWAL(filePath string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file %s: %w", filePath, err)
	}

	wal := &WAL{
		filePath: filePath,
		file:     file,
	}

	if err := wal.recover(); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed during WAL recovery: %w", err)
	}

	return wal, nil
}

// recover scans the WAL from beginning to end, verifying checksums and truncating torn writes.
func (wal *WAL) recover() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if _, err := wal.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek start failed: %w", err)
	}

	var lastValidPos int64 = 0
	var lastOffset uint64 = 0

	for {
		entry, err := DecodeEntry(wal.file)
		if err != nil {
			if errors.Is(err, io.EOF) {
				// Clean EOF
				break
			}
			// Incomplete frame, torn write, or CRC mismatch at tail: truncate to lastValidPos
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, ErrInvalidMagic) || errors.Is(err, ErrChecksumMismatch) || errors.Is(err, ErrCorruptEntry) {
				if err := wal.file.Truncate(lastValidPos); err != nil {
					return fmt.Errorf("failed to truncate corrupted WAL tail: %w", err)
				}
				break
			}
			return fmt.Errorf("unexpected error reading WAL: %w", err)
		}

		lastOffset = entry.Offset
		pos, err := wal.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get current seek pos: %w", err)
		}
		lastValidPos = pos
	}

	wal.lastOffset = lastOffset
	// Ensure file pointer is positioned at EOF for subsequent appends
	if _, err := wal.file.Seek(lastValidPos, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek to WAL end: %w", err)
	}

	return nil
}

// Append writes a new entry to the end of the WAL and syncs it to durable storage.
func (wal *WAL) Append(entry *LogEntry) (uint64, error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.closed {
		return 0, ErrWALClosed
	}

	expectedOffset := wal.lastOffset + 1
	if entry.Offset == 0 {
		entry.Offset = expectedOffset
	} else if entry.Offset != expectedOffset {
		return 0, fmt.Errorf("%w: expected %d, got %d", ErrOffsetMismatch, expectedOffset, entry.Offset)
	}

	encoded, err := EncodeEntry(entry)
	if err != nil {
		return 0, fmt.Errorf("failed to encode entry: %w", err)
	}

	if _, err := wal.file.Write(encoded); err != nil {
		return 0, fmt.Errorf("failed to write WAL entry: %w", err)
	}

	if err := wal.file.Sync(); err != nil {
		return 0, fmt.Errorf("failed to sync WAL file: %w", err)
	}

	wal.lastOffset = entry.Offset
	return entry.Offset, nil
}

// LastOffset returns the highest committed log offset in the WAL.
func (wal *WAL) LastOffset() uint64 {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()
	return wal.lastOffset
}

// ReadFrom reads up to limit entries starting from fromOffset.
func (wal *WAL) ReadFrom(fromOffset uint64, limit int) ([]*LogEntry, error) {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	if wal.closed {
		return nil, ErrWALClosed
	}

	if limit <= 0 {
		limit = 100
	}

	// Open read-only handle to avoid interfering with append file offset
	readFile, err := os.Open(wal.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer readFile.Close()

	entries := make([]*LogEntry, 0, limit)
	for {
		entry, err := DecodeEntry(readFile)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("error reading entry from WAL: %w", err)
		}

		if entry.Offset >= fromOffset {
			entries = append(entries, entry)
			if len(entries) >= limit {
				break
			}
		}
	}

	return entries, nil
}

// Close closes the underlying WAL file.
func (wal *WAL) Close() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.closed {
		return nil
	}
	wal.closed = true
	return wal.file.Close()
}

// ReadWatermark reads an applied offset watermark from disk.
func ReadWatermark(metaPath string) (uint64, error) {
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	str := strings.TrimSpace(string(data))
	if str == "" {
		return 0, nil
	}
	val, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid watermark format in %s: %w", metaPath, err)
	}
	return val, nil
}

// WriteWatermark safely persists the applied offset watermark.
func WriteWatermark(metaPath string, offset uint64) error {
	if err := os.MkdirAll(filepath.Dir(metaPath), 0755); err != nil {
		return err
	}
	tmpFile := fmt.Sprintf("%s.tmp", metaPath)
	if err := os.WriteFile(tmpFile, []byte(strconv.FormatUint(offset, 10)), 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, metaPath)
}
