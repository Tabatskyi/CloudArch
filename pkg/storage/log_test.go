package storage

import (
	"bytes"
	"io"
	"testing"
)

func TestLogEntry_EncodeDecode(t *testing.T) {
	entry := NewLogEntry(1, 1, "PUT", "users", "user#123", "profile", []byte(`{"name":"Alice"}`))

	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("unexpected encode error: %v", err)
	}

	if len(encoded) <= 10 {
		t.Fatalf("encoded length too short: %d", len(encoded))
	}

	decoded, err := DecodeEntry(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}

	if decoded.Offset != entry.Offset {
		t.Errorf("expected offset %d, got %d", entry.Offset, decoded.Offset)
	}
	if decoded.Term != entry.Term {
		t.Errorf("expected term %d, got %d", entry.Term, decoded.Term)
	}
	if decoded.OpType != entry.OpType {
		t.Errorf("expected op %s, got %s", entry.OpType, decoded.OpType)
	}
	if decoded.Table != entry.Table {
		t.Errorf("expected table %s, got %s", entry.Table, decoded.Table)
	}
	if decoded.PartitionKey != entry.PartitionKey {
		t.Errorf("expected partition key %s, got %s", entry.PartitionKey, decoded.PartitionKey)
	}
	if decoded.SortKey != entry.SortKey {
		t.Errorf("expected sort key %s, got %s", entry.SortKey, decoded.SortKey)
	}
	if !bytes.Equal(decoded.Value, entry.Value) {
		t.Errorf("expected value %s, got %s", string(entry.Value), string(decoded.Value))
	}
	if decoded.Checksum != entry.Checksum {
		t.Errorf("expected checksum %d, got %d", entry.Checksum, decoded.Checksum)
	}
}

func TestLogEntry_DecodeFromBytes(t *testing.T) {
	entry := NewLogEntry(42, 2, "DELETE", "orders", "order#99", "item#1", nil)

	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	decoded, consumed, err := DecodeEntryFromBytes(encoded)
	if err != nil {
		t.Fatalf("decode bytes error: %v", err)
	}

	if consumed != len(encoded) {
		t.Errorf("expected consumed %d, got %d", len(encoded), consumed)
	}
	if decoded.Offset != 42 {
		t.Errorf("expected offset 42, got %d", decoded.Offset)
	}
	if decoded.OpType != "DELETE" {
		t.Errorf("expected op DELETE, got %s", decoded.OpType)
	}
}

func TestLogEntry_ChecksumMismatch(t *testing.T) {
	entry := NewLogEntry(5, 1, "PUT", "tbl", "pk", "sk", []byte("val"))
	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Corrupt payload byte
	encoded[len(encoded)-1] ^= 0xFF

	_, err = DecodeEntry(bytes.NewReader(encoded))
	if err != ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}

	_, _, err = DecodeEntryFromBytes(encoded)
	if err != ErrChecksumMismatch {
		t.Fatalf("expected ErrChecksumMismatch from bytes, got %v", err)
	}
}

func TestLogEntry_InvalidMagic(t *testing.T) {
	entry := NewLogEntry(1, 1, "PUT", "tbl", "pk", "sk", []byte("val"))
	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	// Corrupt magic header
	encoded[0] = 0x00

	_, err = DecodeEntry(bytes.NewReader(encoded))
	if err != ErrInvalidMagic {
		t.Fatalf("expected ErrInvalidMagic, got %v", err)
	}

	_, _, err = DecodeEntryFromBytes(encoded)
	if err != ErrInvalidMagic {
		t.Fatalf("expected ErrInvalidMagic from bytes, got %v", err)
	}
}

func TestLogEntry_Truncated(t *testing.T) {
	entry := NewLogEntry(1, 1, "PUT", "tbl", "pk", "sk", []byte("long value for testing truncation"))
	encoded, err := EncodeEntry(entry)
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	truncated := encoded[:len(encoded)-5]
	_, err = DecodeEntry(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected error on truncated input, got nil")
	}

	_, _, err = DecodeEntryFromBytes(truncated)
	if err != io.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}
