package backupvolume

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
)

// Rationale: archive bytes and semantic digests are durable authority, so
// identical logical input must round-trip to byte-identical output.
func TestArchiveDeterministicRoundTrip(t *testing.T) {
	t.Parallel()
	fileContent := []byte("hello\n")
	entries := []Entry{
		{Path: []byte("."), Kind: EntryDirectory, Mode: 0o755},
		{Path: []byte("!data"), Kind: EntryDirectory, Mode: 0o750, UID: 1000, GID: 1000},
		{
			Path: []byte("!data/a.txt"), Kind: EntryRegular, Mode: 0o640, UID: 1000, GID: 1000,
			SizeBytes: uint64(len(fileContent)), ContentSHA256: sha256.Sum256(fileContent),
		},
	}
	first, evidence := writeArchive(t, entries, [][]byte{nil, nil, fileContent})
	second, secondEvidence := writeArchive(t, entries, [][]byte{nil, nil, fileContent})
	if !bytes.Equal(first, second) || evidence != secondEvidence {
		t.Fatal("canonical writes differ")
	}
	validated, err := Validate(context.Background(), bytes.NewReader(first), fixedManifest(len(entries)), evidence)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(validated.Entries, entries) || validated.Evidence != evidence {
		t.Fatalf("validated = %#v", validated)
	}
	third, _ := writeArchive(t, validated.Entries, [][]byte{nil, nil, fileContent})
	if !bytes.Equal(first, third) {
		t.Fatal("round-tripped bytes differ")
	}
	if string(first[0:1]) != "." || string(first[100:108]) != "0000755\x00" || first[156] != '5' ||
		!bytes.Equal(first[257:265], []byte{'u', 's', 't', 'a', 'r', 0, '0', '0'}) {
		t.Fatal("root USTAR header is not canonical")
	}
}

// Rationale: long paths and overflowing numeric fields have one local PAX
// representation, including fixed-point lengths, key order, and placeholders.
func TestArchiveUsesExactLocalPAXFraming(t *testing.T) {
	t.Parallel()
	path := []byte(strings.Repeat("a", 101))
	content := []byte("x")
	entries := []Entry{
		{Path: []byte("."), Kind: EntryDirectory, Mode: 0o755},
		{
			Path: path, Kind: EntryRegular, Mode: 0o600, UID: math.MaxUint32,
			SizeBytes: 1, ContentSHA256: sha256.Sum256(content),
		},
	}
	encoded, evidence := writeArchive(t, entries, [][]byte{nil, content})
	if got := string(bytes.TrimRight(encoded[512:612], "\x00")); got != "PaxHeaders/000000000001" {
		t.Fatalf("PAX header name = %q", got)
	}
	wantPAX := appendPAXRecord(nil, "path", path)
	wantPAX = appendPAXRecord(wantPAX, "uid", []byte("4294967295"))
	if !bytes.Equal(encoded[1024:1024+len(wantPAX)], wantPAX) {
		t.Fatalf("PAX payload = %q; want %q", encoded[1024:1024+len(wantPAX)], wantPAX)
	}
	if got := string(bytes.TrimRight(encoded[1536:1636], "\x00")); got != "PaxPayload/000000000001" {
		t.Fatalf("payload header name = %q", got)
	}
	if _, err := Validate(
		context.Background(), bytes.NewReader(encoded), fixedManifest(len(entries)), evidence,
	); err != nil {
		t.Fatal(err)
	}
}

// Rationale: manifest hashing consumes schema-owner-provided canonical bytes
// exactly once, with one length prefix and the accepted domain separator.
func TestManifestEntryAndDigestExactPreimage(t *testing.T) {
	t.Parallel()
	fixture := []byte{0x08, 0x01, 0x12, 0x01, '.', 0x18, 0x01, 0x20, 0xed, 0x03}
	manifest := []ManifestEntryBytes{{Ordinal: 1, Bytes: fixture}}
	preimage := bytes.NewBufferString(volumeManifestDomain)
	preimage.WriteByte(0)
	var number [4]byte
	binary.BigEndian.PutUint32(number[:], 1)
	preimage.Write(number[:])
	binary.BigEndian.PutUint32(number[:], uint32(len(fixture)))
	preimage.Write(number[:])
	preimage.Write(fixture)
	got, err := ContentManifestSHA256(manifest)
	if err != nil || got != sha256.Sum256(preimage.Bytes()) {
		t.Fatalf("content manifest digest = %x, %v", got, err)
	}
}

// Rationale: opaque canonical manifest entries still require an exact,
// complete ordinal sequence before their bytes can become digest authority.
func TestManifestRejectsInvalidSequence(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		manifest []ManifestEntryBytes
		count    int
	}{
		{name: "missing", count: 1},
		{name: "count", manifest: fixedManifest(1), count: 2},
		{name: "ordinal", manifest: []ManifestEntryBytes{{Ordinal: 2, Bytes: []byte{1}}}, count: 1},
		{name: "empty bytes", manifest: []ManifestEntryBytes{{Ordinal: 1}}, count: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateManifest(test.manifest, test.count); err == nil {
				t.Fatal("ValidateManifest accepted an invalid sequence")
			}
		})
	}
}

// Rationale: untrusted archives reject truncation, trailing data,
// checksum/padding corruption, duplicate paths, and malformed footers.
func TestArchiveRejectsMalformedFraming(t *testing.T) {
	t.Parallel()
	content := []byte("x")
	entries := []Entry{
		{Path: []byte("."), Kind: EntryDirectory, Mode: 0o755},
		{Path: []byte("x"), Kind: EntryRegular, Mode: 0o600, SizeBytes: 1, ContentSHA256: sha256.Sum256(content)},
	}
	valid, evidence := writeArchive(t, entries, [][]byte{nil, content})
	member := append([]byte(nil), valid[512:1536]...)
	duplicate := append([]byte(nil), valid[:len(valid)-1024]...)
	duplicate = append(duplicate, member...)
	duplicate = append(duplicate, make([]byte, 1024)...)
	checksum := append([]byte(nil), valid...)
	checksum[0] = 'z'
	padding := append([]byte(nil), valid...)
	padding[1025] = 1
	footer := append([]byte(nil), valid...)
	footer[len(footer)-1] = 1
	tests := []struct {
		name    string
		content []byte
	}{
		{"truncated", valid[:len(valid)-1]},
		{"trailing", append(append([]byte(nil), valid...), 0)},
		{"checksum", checksum},
		{"padding", padding},
		{"footer", footer},
		{"duplicate", duplicate},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			testEvidence := evidenceForBytes(evidence, test.content)
			if _, err := Validate(
				context.Background(), bytes.NewReader(test.content), fixedManifest(len(entries)), testEvidence,
			); err == nil {
				t.Fatal("Validate accepted malformed framing")
			}
		})
	}
}

// Rationale: traversal, ordering, unsupported metadata, and resource-limit
// violations fail before the first output byte.
func TestWriteRejectsInvalidModelsAndLimits(t *testing.T) {
	t.Parallel()
	root := Entry{Path: []byte("."), Kind: EntryDirectory, Mode: 0o755}
	invalidCases := []struct {
		name    string
		entries []Entry
	}{
		{"empty", nil},
		{"root regular", []Entry{{Path: []byte("."), Kind: EntryRegular}}},
		{
			"duplicate",
			[]Entry{root, {Path: []byte("a"), Kind: EntryDirectory}, {Path: []byte("a"), Kind: EntryDirectory}},
		},
		{
			"unsorted",
			[]Entry{root, {Path: []byte("b"), Kind: EntryDirectory}, {Path: []byte("a"), Kind: EntryDirectory}},
		},
		{"parent", []Entry{root, {Path: []byte("a/../b"), Kind: EntryDirectory}}},
		{"absolute", []Entry{root, {Path: []byte("/a"), Kind: EntryDirectory}}},
		{"trailing slash", []Entry{root, {Path: []byte("a/"), Kind: EntryDirectory}}},
		{"path limit", []Entry{root, {Path: []byte(strings.Repeat("a", MaxPathBytes+1)), Kind: EntryDirectory}}},
		{"mode bits", []Entry{root, {Path: []byte("a"), Kind: EntryDirectory, Mode: maximumMode + 1}}},
		{"directory size", []Entry{root, {Path: []byte("a"), Kind: EntryDirectory, SizeBytes: 1}}},
		{"non UTF-8 PAX", []Entry{root, {Path: []byte{0xff}, Kind: EntryDirectory, UID: math.MaxUint32}}},
	}
	for _, test := range invalidCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := Write(
				context.Background(), io.Discard, test.entries, make([]io.Reader, len(test.entries)),
				nil,
			); err == nil {
				t.Fatal("Write accepted an invalid model")
			}
		})
	}
	tooMany := make([]Entry, MaxEntries+1)
	tooMany[0] = root
	for index := 1; index < len(tooMany); index++ {
		tooMany[index] = Entry{Path: []byte("a" + fixedDecimal(index)), Kind: EntryDirectory}
	}
	if _, err := Write(
		context.Background(), io.Discard, tooMany, make([]io.Reader, len(tooMany)),
		nil,
	); err == nil {
		t.Fatal("Write accepted too many entries")
	}
}

// Rationale: content, source, and manifest hashes are independent authorities
// and substitution of any one must be detected.
func TestArchiveRejectsContentAndEvidenceDigestMismatch(t *testing.T) {
	t.Parallel()
	content := []byte("content")
	entry := Entry{
		Path: []byte("a"), Kind: EntryRegular, Mode: 0o600,
		SizeBytes: uint64(len(content)), ContentSHA256: sha256.Sum256(content),
	}
	entries := []Entry{{Path: []byte("."), Kind: EntryDirectory, Mode: 0o755}, entry}
	badEntry := entry
	badEntry.ContentSHA256[0]++
	if _, err := Write(
		context.Background(), io.Discard,
		[]Entry{entries[0], badEntry}, []io.Reader{nil, bytes.NewReader(content)},
		fixedManifest(len(entries)),
	); err == nil {
		t.Fatal("Write accepted a content digest mismatch")
	}
	encoded, evidence := writeArchive(t, entries, [][]byte{nil, content})
	badSource := evidence
	badSource.Source.SHA256[0]++
	if _, err := Validate(
		context.Background(), bytes.NewReader(encoded), fixedManifest(len(entries)), badSource,
	); err == nil {
		t.Fatal("Validate accepted a source digest mismatch")
	}
	badManifest := evidence
	badManifest.Archive.ContentManifestSHA256[0]++
	if _, err := Validate(
		context.Background(), bytes.NewReader(encoded), fixedManifest(len(entries)), badManifest,
	); err == nil {
		t.Fatal("Validate accepted a manifest digest mismatch")
	}
}

func writeArchive(t *testing.T, entries []Entry, contents [][]byte) ([]byte, ArtifactEvidence) {
	t.Helper()
	readers := make([]io.Reader, len(contents))
	for index, content := range contents {
		if content != nil {
			readers[index] = bytes.NewReader(content)
		}
	}
	var destination bytes.Buffer
	evidence, err := Write(context.Background(), &destination, entries, readers, fixedManifest(len(entries)))
	if err != nil {
		t.Fatal(err)
	}
	return destination.Bytes(), evidence
}

func fixedManifest(count int) []ManifestEntryBytes {
	fixtures := [][]byte{
		{0x08, 0x01},
		{0x08, 0x02},
		{0x08, 0x03},
	}
	manifest := make([]ManifestEntryBytes, count)
	for index := range manifest {
		manifest[index] = ManifestEntryBytes{
			Ordinal: uint64(index + 1),
			Bytes:   append([]byte(nil), fixtures[index]...),
		}
	}
	return manifest
}

func evidenceForBytes(base ArtifactEvidence, content []byte) ArtifactEvidence {
	base.Source = backupformat.Evidence{SizeBytes: uint64(len(content)), SHA256: sha256.Sum256(content)}
	base.Archive.SourceSizeBytes = uint64(len(content))
	return base
}

func fixedDecimal(value int) string {
	return strconv.FormatInt(int64(value+10_000), 10)[1:]
}
