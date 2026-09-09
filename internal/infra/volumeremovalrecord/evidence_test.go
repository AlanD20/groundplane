package volumeremovalrecord_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: removal evidence must retain the exact accepted mount intent and
// advance only through its complete, manifest-bound ordinal prefix.
func TestEvidenceRecordsPreserveAcceptedPrefix(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	encoded, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil || len(encoded) > 16*1024 {
		t.Fatal("encode manifest", err)
	}
	decoded, err := removal.DecodeEvidenceManifest(encoded)
	if err != nil || decoded != manifest {
		t.Fatal("manifest changed", err)
	}
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for index, row := range rows {
		value, err := removal.EncodeEvidenceRow(row)
		if err != nil || len(value) > 16*1024 {
			t.Fatal("encode row", err)
		}
		decoded, err := removal.DecodeEvidenceRow(value)
		if err != nil || decoded != row {
			t.Fatal("mount row changed", err)
		}
		cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, decoded)
		if err != nil || cursor.CompletedRows != uint64(index+1) || cursor.NextOrdinal != uint64(index+2) {
			t.Fatal("cursor did not advance exactly one row", err)
		}
		value, err = removal.EncodeEvidenceCursor(cursor, manifest)
		if err != nil || len(value) > 16*1024 {
			t.Fatal("encode cursor", err)
		}
		cursor, err = removal.DecodeEvidenceCursor(value, manifest)
		if err != nil {
			t.Fatal("resume cursor", err)
		}
	}
	if cursor.RollingSHA256 != manifest.OrderedSHA256 {
		t.Fatal("completed prefix differs from the accepted ordered set")
	}
}

// Rationale: a cursor cannot skip, repeat, substitute or overrun accepted
// rows, including after a restart under a different manifest.
func TestEvidenceCursorRejectsChangedAuthority(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.AdvanceEvidenceCursor(cursor, manifest, rows[1]); err == nil {
		t.Fatal("skipped the first row")
	}
	first, err := removal.AdvanceEvidenceCursor(cursor, manifest, rows[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.AdvanceEvidenceCursor(first, manifest, rows[0]); err == nil {
		t.Fatal("repeated the first row")
	}
	changed := rows[1]
	changed.ReadOnly = !changed.ReadOnly
	changed.SHA256, err = removal.EvidenceRowDigest(changed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.AdvanceEvidenceCursor(first, manifest, changed); err == nil {
		t.Fatal("completed a different ordered set")
	}
	value, err := removal.EncodeEvidenceCursor(first, manifest)
	if err != nil {
		t.Fatal(err)
	}
	other := manifest
	other.ImpactSHA256 = sha256.Sum256([]byte("different accepted impact"))
	if _, err := removal.DecodeEvidenceCursor(value, other); err == nil {
		t.Fatal("resumed under a different manifest")
	}
	finished, err := removal.AdvanceEvidenceCursor(first, manifest, rows[1])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.AdvanceEvidenceCursor(finished, manifest, rows[1]); err == nil {
		t.Fatal("advanced beyond manifest maximum")
	}
}

// Rationale: an ordered-set digest cannot legitimize rows supplied in a
// noncanonical consumer order, including across a resumed staging cursor.
func TestEvidenceRowsRequireStableConsumerOrder(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	rows[0], rows[1] = rows[1], rows[0]
	manifest.OrderedSHA256 = removal.EmptyEvidenceDigest()
	for index := range rows {
		rows[index].Ordinal = uint64(index + 1)
		var err error
		rows[index].SHA256, err = removal.EvidenceRowDigest(rows[index])
		if err != nil {
			t.Fatal(err)
		}
		manifest.OrderedSHA256 = removal.AppendEvidenceDigest(manifest.OrderedSHA256, rows[index].SHA256)
	}
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, rows[0])
	if err != nil {
		t.Fatal(err)
	}
	value, err := removal.EncodeEvidenceCursor(cursor, manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = removal.DecodeEvidenceCursor(value, manifest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := removal.AdvanceEvidenceCursor(cursor, manifest, rows[1]); err == nil {
		t.Fatal("resumed an out-of-order consumer prefix")
	}
}

// Rationale: every complete encoded evidence value is bounded, self-checking,
// and unambiguous; truncation and trailing data cannot become cleanup intent.
func TestEvidenceEncodingRejectsCorruptionAndOversize(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	row := rows[0]
	row.MountTarget = "/" + strings.Repeat("x", 16*1024)
	if _, err := removal.EvidenceRowDigest(row); err == nil {
		t.Fatal("accepted an oversized complete row")
	}
	value, err := removal.EncodeEvidenceRow(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	row = rows[0]
	row.MountTarget = "/" + strings.Repeat("x", 16*1024-len(value)+len(row.MountTarget)-1)
	row.SHA256, err = removal.EvidenceRowDigest(row)
	if err != nil {
		t.Fatal("maximum legal row", err)
	}
	maximum, err := removal.EncodeEvidenceRow(row)
	if err != nil || len(maximum) != 16*1024 {
		t.Fatal("maximum complete row size", len(maximum), err)
	}
	if decoded, err := removal.DecodeEvidenceRow(maximum); err != nil || decoded != row {
		t.Fatal("maximum row changed", err)
	}
	row.MountTarget += "x"
	if _, err := removal.EvidenceRowDigest(row); err == nil {
		t.Fatal("accepted one byte over the row maximum")
	}
	for index := range value {
		if _, err := removal.DecodeEvidenceRow(value[:index]); err == nil {
			t.Fatalf("accepted row truncated at %d", index)
		}
	}
	corrupt := bytes.Clone(value)
	corrupt[len(corrupt)-1] ^= 1
	if _, err := removal.DecodeEvidenceRow(corrupt); err == nil {
		t.Fatal("accepted changed row digest")
	}
	if _, err := removal.DecodeEvidenceRow(append(value, 0)); err == nil {
		t.Fatal("accepted trailing row bytes")
	}
	manifest.TotalRows, manifest.MaximumOrdinal = 0, 0
	manifest.OrderedSHA256 = removal.EmptyEvidenceDigest()
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil || cursor.CompletedRows != 0 || cursor.NextOrdinal != 1 {
		t.Fatal("unmounted Volume needs a valid empty manifest", err)
	}
	if _, err := removal.AdvanceEvidenceCursor(cursor, manifest, rows[0]); err == nil {
		t.Fatal("appended to the empty accepted set")
	}
}

// Rationale: manifest and cursor boundaries must reject malformed identities,
// overflow, and ambiguous encodings before a staging owner can consume them.
func TestEvidenceManifestAndCursorBoundaries(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	for _, invalid := range []removal.EvidenceManifest{
		{},
		func() removal.EvidenceManifest { v := manifest; v.ReadRevision = 0; return v }(),
		func() removal.EvidenceManifest { v := manifest; v.MaximumOrdinal++; return v }(),
		func() removal.EvidenceManifest {
			v := manifest
			v.TotalRows = math.MaxUint64
			v.MaximumOrdinal = math.MaxUint64
			return v
		}(),
		func() removal.EvidenceManifest { v := manifest; v.DesiredRevisionID = v.SourceRevisionID; return v }(),
	} {
		if _, err := removal.EncodeEvidenceManifest(invalid); err == nil {
			t.Fatal("accepted invalid manifest")
		}
	}
	value, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for index := range value {
		if _, err := removal.DecodeEvidenceManifest(value[:index]); err == nil {
			t.Fatalf("accepted truncated manifest at %d", index)
		}
	}
	if _, err := removal.DecodeEvidenceManifest(append(bytes.Clone(value), 0)); err == nil {
		t.Fatal("accepted trailing manifest data")
	}
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, rows[0])
	if err != nil {
		t.Fatal(err)
	}
	value, err = removal.EncodeEvidenceCursor(cursor, manifest)
	if err != nil {
		t.Fatal(err)
	}
	for index := range value {
		if _, err := removal.DecodeEvidenceCursor(value[:index], manifest); err == nil {
			t.Fatalf("accepted truncated cursor at %d", index)
		}
	}
	if _, err := removal.DecodeEvidenceCursor(append(value, 0), manifest); err == nil {
		t.Fatal("accepted trailing cursor data")
	}
	cursor.NextOrdinal++
	if _, err := removal.EncodeEvidenceCursor(cursor, manifest); err == nil {
		t.Fatal("accepted skipped cursor ordinal")
	}
	manifest.Key = strings.Repeat("k", 255)
	manifest.ReadRevision = math.MaxInt64
	manifest.TotalRows, manifest.MaximumOrdinal = math.MaxUint64-1, math.MaxUint64-1
	value, err = removal.EncodeEvidenceManifest(manifest)
	if err != nil || len(value) != 524 {
		t.Fatal("maximum manifest encoding changed", len(value), err)
	}
	if decoded, err := removal.DecodeEvidenceManifest(value); err != nil || decoded != manifest {
		t.Fatal("maximum manifest round trip", err)
	}
	cursor, err = removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor.CompletedRows, cursor.NextOrdinal = manifest.TotalRows, math.MaxUint64
	cursor.RollingSHA256, cursor.LastConsumerID = manifest.OrderedSHA256, rows[0].ConsumerID
	value, err = removal.EncodeEvidenceCursor(cursor, manifest)
	if err != nil || len(value) != 153 {
		t.Fatal("maximum cursor encoding changed", len(value), err)
	}
	if decoded, err := removal.DecodeEvidenceCursor(value, manifest); err != nil || decoded != cursor {
		t.Fatal("maximum cursor round trip", err)
	}
	if _, err := removal.AdvanceEvidenceCursor(cursor, manifest, rows[0]); err == nil {
		t.Fatal("overflowed a completed cursor")
	}
}

// Rationale: the binary format and digest chain are persisted contracts;
// changing both encoder and decoder must not silently change their bytes.
func TestEvidenceCanonicalEncoding(t *testing.T) {
	manifest, rows := evidenceFixture(t)
	manifestValue, err := removal.EncodeEvidenceManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	rowValue, err := removal.EncodeEvidenceRow(rows[0])
	if err != nil {
		t.Fatal(err)
	}
	cursor, err := removal.InitialEvidenceCursor(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = removal.AdvanceEvidenceCursor(cursor, manifest, rows[0])
	if err != nil {
		t.Fatal(err)
	}
	cursorValue, err := removal.EncodeEvidenceCursor(cursor, manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []struct {
		name   string
		value  []byte
		size   int
		digest string
	}{
		{"manifest", manifestValue, 273, "3e8fc40aadc110f23f1a2dd404cf5051f48823883d77d7c3e42ce669954430b4"},
		{"row", rowValue, 196, "27138d34c0c3d20b44cd2b29c1a451ac8ed6fcfd0191798afcd82a325925d2d4"},
		{"cursor", cursorValue, 153, "969e59610075400c53b347298139e8ca3f9b6e4dd24849f9adf2582bf7d5ed1e"},
	} {
		digest := sha256.Sum256(record.value)
		if len(record.value) != record.size || hex.EncodeToString(digest[:]) != record.digest {
			t.Fatalf("%s encoding changed: %d bytes, SHA256 %x", record.name, len(record.value), digest)
		}
		t.Logf("%s: %d bytes, SHA256 %x", record.name, len(record.value), sha256.Sum256(record.value))
	}
}

func evidenceFixture(t *testing.T) (removal.EvidenceManifest, []removal.EvidenceRow) {
	t.Helper()
	at := time.Date(2026, 9, 9, 0, 0, 0, 0, time.UTC)
	manifest := removal.EvidenceManifest{
		OperationID: ids.NewAt(ids.KindOperation, at, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, at, 2),
		VolumeID: ids.NewAt(ids.KindVolume, at, 3), Key: "data", ReadRevision: 20,
		SourceRevisionID: ids.NewAt(ids.KindTask, at, 4), DesiredRevisionID: ids.NewAt(ids.KindTask, at, 5),
		ImpactSHA256: sha256.Sum256([]byte("accepted impact")), TotalRows: 2, MaximumOrdinal: 2,
	}
	rows := make([]removal.EvidenceRow, 2)
	rolling := removal.EmptyEvidenceDigest()
	for index := range rows {
		rows[index] = removal.EvidenceRow{OperationID: manifest.OperationID, VolumeID: manifest.VolumeID,
			ConsumerID: ids.NewAt(ids.KindService, at, int64(index+6)), SourceRevisionID: manifest.SourceRevisionID,
			Ordinal: uint64(index + 1), MountTarget: "/var/data", ReadOnly: index == 1}
		digest, err := removal.EvidenceRowDigest(rows[index])
		if err != nil {
			t.Fatal(err)
		}
		rows[index].SHA256 = digest
		rolling = removal.AppendEvidenceDigest(rolling, digest)
	}
	manifest.OrderedSHA256 = rolling
	return manifest, rows
}
