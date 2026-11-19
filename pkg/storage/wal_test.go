package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	if wal.LastOffset() != 0 {
		t.Fatalf("expected initial offset 0, got %d", wal.LastOffset())
	}

	// Append 5 entries
	for i := 1; i <= 5; i++ {
		entry := NewLogEntry(uint64(i), 1, "PUT", "tbl", "pk", "sk", []byte("val"))
		offset, err := wal.Append(entry)
		if err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
		if offset != uint64(i) {
			t.Fatalf("expected offset %d, got %d", i, offset)
		}
	}

	if wal.LastOffset() != 5 {
		t.Fatalf("expected last offset 5, got %d", wal.LastOffset())
	}

	// Read range [3, 5]
	entries, err := wal.ReadFrom(3, 10)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	if entries[0].Offset != 3 || entries[2].Offset != 5 {
		t.Fatalf("unexpected offsets in range: %d, %d", entries[0].Offset, entries[2].Offset)
	}
}

func TestWAL_RejectNonMonotonic(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}
	defer wal.Close()

	e1 := NewLogEntry(1, 1, "PUT", "tbl", "pk", "sk", []byte("val"))
	if _, err := wal.Append(e1); err != nil {
		t.Fatalf("append e1 failed: %v", err)
	}

	// Attempt to append with unexpected offset 5
	e5 := NewLogEntry(5, 1, "PUT", "tbl", "pk", "sk", []byte("val"))
	if _, err := wal.Append(e5); err == nil {
		t.Fatal("expected error on non-monotonic offset, got nil")
	}
}

func TestWAL_CrashRecovery_TruncatesTornWrite(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL failed: %v", err)
	}

	// Append 3 valid entries
	for i := 1; i <= 3; i++ {
		entry := NewLogEntry(uint64(i), 1, "PUT", "tbl", "pk", "sk", []byte("val"))
		if _, err := wal.Append(entry); err != nil {
			t.Fatalf("append %d failed: %v", i, err)
		}
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Append garbage / torn write to end of file
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("open file failed: %v", err)
	}
	// Write magic bytes and incomplete length
	_, _ = f.Write([]byte{MagicHeader[0], MagicHeader[1], 0x00, 0x00, 0x01})
	_ = f.Close()

	// Reopen WAL; recovery should truncate the torn write and keep the 3 valid entries
	recoveredWAL, err := OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL after crash failed: %v", err)
	}
	defer recoveredWAL.Close()

	if recoveredWAL.LastOffset() != 3 {
		t.Fatalf("expected last offset 3 after recovery, got %d", recoveredWAL.LastOffset())
	}

	// Appending entry 4 should succeed cleanly
	e4 := NewLogEntry(4, 1, "PUT", "tbl", "pk", "sk", []byte("val4"))
	offset, err := recoveredWAL.Append(e4)
	if err != nil {
		t.Fatalf("append after recovery failed: %v", err)
	}
	if offset != 4 {
		t.Fatalf("expected offset 4, got %d", offset)
	}
}

func TestWatermark_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	metaPath := filepath.Join(tmpDir, "offset.meta")

	// Read non-existent
	wm, err := ReadWatermark(metaPath)
	if err != nil {
		t.Fatalf("read non-existent watermark failed: %v", err)
	}
	if wm != 0 {
		t.Fatalf("expected 0, got %d", wm)
	}

	// Write and read
	if err := WriteWatermark(metaPath, 42); err != nil {
		t.Fatalf("write watermark failed: %v", err)
	}

	wm, err = ReadWatermark(metaPath)
	if err != nil {
		t.Fatalf("read watermark failed: %v", err)
	}
	if wm != 42 {
		t.Fatalf("expected 42, got %d", wm)
	}
}
