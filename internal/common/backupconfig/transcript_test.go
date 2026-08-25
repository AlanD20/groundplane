package backupconfig

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"testing"
)

// L0 - pure byte-contract tests. See docs/standards.md, section 13.

// Rationale: the empty capture and restore digests lock the transcript domain,
// direction tags, authority field widths, and field order.
func TestEmptyTransferTranscriptGoldenVectors(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	_, authority, _, err := BuildManifest(ctx, TransferCapture, nil, nil)
	if err != nil {
		t.Fatalf("BuildManifest(): %v", err)
	}
	capture, err := TransferTranscriptSHA256(ctx, TransferCapture, authority, nil, nil)
	if err != nil {
		t.Fatalf("capture transcript: %v", err)
	}
	restore, err := TransferTranscriptSHA256(ctx, TransferRestore, authority, nil, nil)
	if err != nil {
		t.Fatalf("restore transcript: %v", err)
	}
	if got := hex.EncodeToString(capture[:]); got != "c5595baebe3953162a534d981e7ed5f241d2a6358accbd7e3f9a1ad6449c1ee5" {
		t.Fatalf("capture digest = %s", got)
	}
	if got := hex.EncodeToString(restore[:]); got != "a1652dfd0833e8ed6bd8d18657b4d014e1cac11edad358a2f58c320d2bdf33b0" {
		t.Fatalf("restore digest = %s", got)
	}
}

// Rationale: schema 1 requires every canonical metadata frame before the
// first value frame; ordinal and size fields make that grammar self-delimiting.
func TestTransferTranscriptFramesAllMetadataBeforeValues(t *testing.T) {
	t.Parallel()

	authority := ContentAuthority{
		ManifestSHA256:          sha256.Sum256([]byte("manifest")),
		EntryCount:              2,
		TotalSelectedValueBytes: 2,
		ManifestSizeBytes:       47,
		SourceSizeBytes:         4096,
	}
	metadata := []MetadataFrame{
		{Ordinal: 1, EntryID: "ev_00000000000000000000000000", CanonicalEntry: []byte{0xaa}},
		{Ordinal: 2, EntryID: "ev_00000000000000000000000001", CanonicalEntry: []byte{0xbb, 0xcc}},
	}
	values := []ValueFrame{
		{Ordinal: 1, EntryID: metadata[0].EntryID, SizeBytes: 1, Reader: bytes.NewReader([]byte{'x'})},
		{Ordinal: 2, EntryID: metadata[1].EntryID, SizeBytes: 1, Reader: bytes.NewReader([]byte{'y'})},
	}
	var got bytes.Buffer
	if err := WriteTransferTranscript(context.Background(), &got, TransferCapture, authority, metadata, values); err != nil {
		t.Fatalf("WriteTransferTranscript(): %v", err)
	}

	var want bytes.Buffer
	want.WriteString(transcriptDomain)
	want.WriteByte(0)
	writeTestUint32(&want, 1)
	want.Write(authority.ManifestSHA256[:])
	writeTestUint64(&want, 47)
	writeTestUint64(&want, 4096)
	writeTestUint32(&want, 2)
	writeTestUint64(&want, 2)
	writeTestUint32(&want, 1)
	writeTestUint32(&want, 1)
	want.WriteByte(0xaa)
	writeTestUint32(&want, 2)
	writeTestUint32(&want, 2)
	want.Write([]byte{0xbb, 0xcc})
	writeTestUint32(&want, 1)
	writeTestUint32(&want, 1)
	writeTestUint64(&want, 1)
	want.WriteByte('x')
	writeTestUint32(&want, 2)
	writeTestUint32(&want, 1)
	writeTestUint64(&want, 1)
	want.WriteByte('y')
	if !bytes.Equal(got.Bytes(), want.Bytes()) {
		t.Fatalf("transcript bytes = %x, want %x", got.Bytes(), want.Bytes())
	}
}

// Rationale: protocol metadata is caller-serialized, but the archive leaf
// must still enforce the accepted per-entry and aggregate byte ceilings.
func TestTransferTranscriptRejectsOversizedCanonicalEntry(t *testing.T) {
	t.Parallel()

	authority := ContentAuthority{EntryCount: 1, ManifestSizeBytes: 47, SourceSizeBytes: 2560}
	entryID := "ev_00000000000000000000000000"
	metadata := []MetadataFrame{{Ordinal: 1, EntryID: entryID, CanonicalEntry: make([]byte, MaxCanonicalEntryBytes+1)}}
	values := []ValueFrame{{Ordinal: 1, EntryID: entryID, Reader: bytes.NewReader(nil)}}
	if err := WriteTransferTranscript(context.Background(), &bytes.Buffer{}, TransferCapture, authority, metadata, values); err == nil {
		t.Fatal("WriteTransferTranscript() accepted oversized deterministic metadata")
	}
}

// Rationale: S is recomputed from M, N, and every Vi before transcript bytes
// are emitted, so an authority cannot claim impossible archive geometry.
func TestTransferTranscriptRejectsImpossibleSourceGeometryAndCancellation(t *testing.T) {
	t.Parallel()
	entryID := "ev_00000000000000000000000000"
	metadata := []MetadataFrame{{Ordinal: 1, EntryID: entryID, CanonicalEntry: []byte{1}}}
	values := []ValueFrame{{Ordinal: 1, EntryID: entryID, SizeBytes: 1, Reader: bytes.NewReader([]byte{'x'})}}
	authority := ContentAuthority{
		EntryCount: 1, TotalSelectedValueBytes: 1, ManifestSizeBytes: 47, SourceSizeBytes: 2048,
	}
	var destination bytes.Buffer
	if err := WriteTransferTranscript(context.Background(), &destination, TransferCapture, authority, metadata, values); err == nil {
		t.Fatal("WriteTransferTranscript() accepted impossible S")
	}
	if destination.Len() != 0 {
		t.Fatal("invalid geometry emitted a partial transcript")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WriteTransferTranscript(canceled, &destination, TransferCapture, authority, metadata, values); err == nil {
		t.Fatal("WriteTransferTranscript() ignored cancellation")
	}
}

func writeTestUint32(destination *bytes.Buffer, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	destination.Write(encoded[:])
}

func writeTestUint64(destination *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	destination.Write(encoded[:])
}
