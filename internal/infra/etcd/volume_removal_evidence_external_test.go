package etcd_test

import (
	"context"
	"crypto/sha256"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: row staging must survive process reconstruction and lost responses
// without making desired state public or replacing the accepted manifest.
func TestVolumeEvidenceStagingResumesExactPrefix(t *testing.T) {
	ctx := context.Background()
	fixture, audit, manifest, rows := volumeEvidenceFixture(t, 45)
	repository, err := volumeremoval.NewEvidenceRepository(audit)
	if err != nil {
		t.Fatal(err)
	}
	state, err := repository.Begin(ctx, manifest)
	if err != nil || state.Cursor.Record.CompletedRows != 0 {
		t.Fatal("begin staging", err)
	}
	manifestRevision := state.Manifest.Revision
	audit.LoseResponse = true
	if _, err := repository.Stage(ctx, manifest, rows[:44]); err == nil {
		t.Fatal("staging response was not lost")
	}
	if audit.Comparisons != 46 || audit.Mutations != 45 || audit.WireBytes != 737847 || audit.WireBytes > 900*1024 {
		t.Fatalf("44-row request: %d/%d/%d", audit.Comparisons, audit.Mutations, audit.WireBytes)
	}
	t.Logf("44-row staging: %d/%d/%d", audit.Comparisons, audit.Mutations, audit.WireBytes)
	batchRevision := fixture.Revision()
	for _, key := range audit.WrittenKeys {
		if !strings.HasPrefix(key, removal.EvidenceRoot(manifest.OperationID)) ||
			strings.HasPrefix(key, removal.Root(manifest.OperationID)) {
			t.Fatal("staging wrote outside its private evidence namespace")
		}
	}
	repository, err = volumeremoval.NewEvidenceRepository(audit)
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.Revision()
	state, err = repository.Begin(ctx, manifest)
	if err != nil || state.Cursor.Record.CompletedRows != 44 || state.Manifest.Revision != manifestRevision ||
		fixture.Revision() != before {
		t.Fatal("restart changed accepted staging", err)
	}
	replay, err := repository.Stage(ctx, manifest, rows[:44])
	if err != nil || !replay.Replayed || replay.AcceptedRows != 44 || fixture.Revision() != before {
		t.Fatal("lost-response replay changed rows or cursor", err)
	}
	result, err := repository.Stage(ctx, manifest, rows[44:])
	if err != nil || result.Replayed || result.AcceptedRows != 1 || result.State.Cursor.Record.CompletedRows != 45 ||
		result.State.Cursor.Record.RollingSHA256 != manifest.OrderedSHA256 {
		t.Fatal("tail did not complete the accepted set", err)
	}
	keys := make([]string, len(rows))
	for index, row := range rows {
		keys[index] = removal.EvidenceRowKey(manifest.OperationID, row.Ordinal)
	}
	stored, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: keys})
	if err != nil || len(stored.Values) != len(rows) {
		t.Fatal("read staged rows", err)
	}
	for index, value := range stored.Values {
		wantRevision := batchRevision
		if index == 44 {
			wantRevision = result.State.Cursor.Revision
		}
		if value == nil || value.ModRevision != wantRevision {
			t.Fatal("row was not atomic with its batch cursor")
		}
		row, err := removal.DecodeEvidenceRow(value.Value)
		if err != nil || row != rows[index] {
			t.Fatal("stored row differs from accepted input", err)
		}
	}
	changed := manifest
	changed.ImpactSHA256 = sha256.Sum256([]byte("different impact"))
	before = fixture.Revision()
	if _, err := repository.Begin(ctx, changed); err == nil || fixture.Revision() != before {
		t.Fatal("replaced immutable staging manifest", err)
	}
}

// Rationale: exact encoded size chooses the largest fitting prefix; retries
// consume only the committed prefix and never fall back to an oversized row.
func TestVolumeEvidenceStagingHonorsMeasuredPrefix(t *testing.T) {
	ctx := context.Background()
	fixture, audit, manifest, rows := volumeEvidenceFixture(t, 90)
	repository, err := volumeremoval.NewEvidenceRepository(audit)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Begin(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	audit.ReportedRowOverhead = 20 * 1024
	result, err := repository.Stage(ctx, manifest, rows[:44])
	if err != nil || result.AcceptedRows < 1 || result.AcceptedRows >= 44 ||
		audit.SizedRows[result.AcceptedRows] > 900*1024 || audit.SizedRows[result.AcceptedRows+1] <= 900*1024 {
		t.Fatal("did not choose the largest measured prefix", result.AcceptedRows, err)
	}
	before := fixture.Revision()
	replay, err := repository.Stage(ctx, manifest, rows[:44])
	if err != nil || !replay.Replayed || replay.AcceptedRows != result.AcceptedRows || fixture.Revision() != before {
		t.Fatal("partial response replay wrote rows", err)
	}
	audit.ReportedRowOverhead = 900 * 1024
	if _, err := repository.Stage(ctx, manifest, rows[result.AcceptedRows:result.AcceptedRows+44]); err == nil ||
		fixture.Revision() != before {
		t.Fatal("oversized single-row fallback wrote evidence", err)
	}
}

// Rationale: neither a changed manifest/cursor nor a row created after preflight
// may let a losing transaction publish any part of its selected prefix.
func TestVolumeEvidenceStagingRejectsRacingAuthorities(t *testing.T) {
	for _, family := range []string{"manifest", "cursor", "row"} {
		t.Run(family, func(t *testing.T) {
			ctx := context.Background()
			fixture, audit, manifest, rows := volumeEvidenceFixture(t, 44)
			repository, err := volumeremoval.NewEvidenceRepository(audit)
			if err != nil {
				t.Fatal(err)
			}
			state, err := repository.Begin(ctx, manifest)
			if err != nil {
				t.Fatal(err)
			}
			key := removal.EvidenceManifestKey(manifest.OperationID)
			value, err := removal.EncodeEvidenceManifest(manifest)
			if family == "cursor" {
				key = removal.EvidenceCursorKey(manifest.OperationID)
				value, err = removal.EncodeEvidenceCursor(state.Cursor.Record, manifest)
			} else if family == "row" {
				key = removal.EvidenceRowKey(manifest.OperationID, 1)
				value, err = removal.EncodeEvidenceRow(rows[0])
			}
			if err != nil {
				t.Fatal(err)
			}
			audit.BeforeCommit = func() {
				if _, err := fixture.Store.Put(ctx, key, value); err != nil {
					t.Fatal(err)
				}
			}
			before := fixture.Revision()
			if _, err := repository.Stage(ctx, manifest, rows); err == nil || fixture.Revision() != before+1 {
				t.Fatal("losing staging transaction wrote evidence", err)
			}
			read, err := fixture.Store.GetMany(
				ctx,
				etcd.GetManyRequest{Keys: []string{removal.EvidenceCursorKey(manifest.OperationID)}},
			)
			if err != nil || read.Values[0] == nil {
				t.Fatal("read cursor", err)
			}
			cursor, err := removal.DecodeEvidenceCursor(read.Values[0].Value, manifest)
			if err != nil || cursor.CompletedRows != 0 {
				t.Fatal("losing staging advanced cursor", err)
			}
		})
	}
}

// Rationale: immutable identical rows can be reused, but corruption, orphaned
// initialization and missing already-committed rows must never be repaired by replay.
func TestVolumeEvidenceStagingValidatesExistingRows(t *testing.T) {
	for _, mode := range []string{"identical", "changed", "missing committed", "orphan"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			fixture, audit, manifest, rows := volumeEvidenceFixture(t, 44)
			repository, err := volumeremoval.NewEvidenceRepository(audit)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "orphan" {
				if _, err := repository.Begin(ctx, manifest); err != nil {
					t.Fatal(err)
				}
			}
			key := removal.EvidenceRowKey(manifest.OperationID, 1)
			row := rows[0]
			if mode == "changed" {
				row.ReadOnly = !row.ReadOnly
				row.SHA256, err = removal.EvidenceRowDigest(row)
				if err != nil {
					t.Fatal(err)
				}
			}
			value, err := removal.EncodeEvidenceRow(row)
			if err != nil {
				t.Fatal(err)
			}
			revision, err := fixture.Store.Put(ctx, key, value)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "orphan" {
				if _, err := repository.Begin(ctx, manifest); err == nil || fixture.Revision() != revision {
					t.Fatal("adopted orphan evidence", err)
				}
				return
			}
			result, err := repository.Stage(ctx, manifest, rows)
			if mode == "changed" {
				if err == nil || fixture.Revision() != revision {
					t.Fatal("replaced changed evidence", err)
				}
				return
			}
			if err != nil || result.AcceptedRows != 44 || audit.Mutations != 44 {
				t.Fatal("did not reuse identical row", err)
			}
			read, err := fixture.Store.GetMany(ctx, etcd.GetManyRequest{Keys: []string{key}})
			if err != nil || read.Values[0] == nil || read.Values[0].ModRevision != revision {
				t.Fatal("rewrote immutable row", err)
			}
			if mode == "missing committed" {
				if _, err := fixture.Store.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
				before := fixture.Revision()
				if _, err := repository.Stage(ctx, manifest, rows); err == nil || fixture.Revision() != before {
					t.Fatal("replay repaired a missing committed row", err)
				}
			}
		})
	}
}

// Rationale: an unmounted Volume has a valid empty staged set, while non-empty
// callers must supply a full bounded window and may not skip the durable cursor.
func TestVolumeEvidenceStagingEmptyAndWindowBounds(t *testing.T) {
	for _, count := range []int{0, 45} {
		ctx := context.Background()
		fixture, audit, manifest, rows := volumeEvidenceFixture(t, count)
		repository, err := volumeremoval.NewEvidenceRepository(audit)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repository.Begin(ctx, manifest); err != nil {
			t.Fatal(err)
		}
		before := fixture.Revision()
		if count == 0 {
			result, err := repository.Stage(ctx, manifest, nil)
			if err != nil || !result.Replayed || result.AcceptedRows != 0 || fixture.Revision() != before {
				t.Fatal("empty evidence changed state", err)
			}
			continue
		}
		for _, window := range [][]removal.EvidenceRow{nil, rows, rows[:1], rows[1:45]} {
			if _, err := repository.Stage(ctx, manifest, window); err == nil || fixture.Revision() != before {
				t.Fatal("invalid evidence window changed state", err)
			}
		}
	}
}

func volumeEvidenceFixture(t *testing.T, count int) (*etcd.VolumePolicyDesiredFixture,
	*etcd.VolumeEvidenceStageAudit, removal.EvidenceManifest, []removal.EvidenceRow) {
	t.Helper()
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	audit := etcd.NewVolumeEvidenceStageAudit(fixture.Store)
	at := fixture.Task.CreatedAt
	manifest := removal.EvidenceManifest{
		OperationID: ids.NewAt(ids.KindOperation, at, 601), EnvironmentID: fixture.Task.Owner.EnvironmentID,
		VolumeID: ids.NewAt(ids.KindVolume, at, 602), Key: "data", ReadRevision: fixture.Revision(),
		SourceRevisionID: ids.NewAt(ids.KindTask, at, 603), DesiredRevisionID: ids.NewAt(ids.KindTask, at, 604),
		ImpactSHA256: sha256.Sum256([]byte("accepted fixed-revision impact")),
		TotalRows:    uint64(count), MaximumOrdinal: uint64(count), OrderedSHA256: removal.EmptyEvidenceDigest(),
	}
	rows := make([]removal.EvidenceRow, count)
	for index := range rows {
		row := removal.EvidenceRow{OperationID: manifest.OperationID, VolumeID: manifest.VolumeID,
			ConsumerID: ids.NewAt(ids.KindService, at, 605), SourceRevisionID: manifest.SourceRevisionID,
			Ordinal: uint64(index + 1), MountTarget: "/mount-" + strconv.Itoa(index), ReadOnly: index%2 == 0}
		var err error
		row.SHA256, err = removal.EvidenceRowDigest(row)
		if err != nil {
			t.Fatal(err)
		}
		value, err := removal.EncodeEvidenceRow(row)
		if err != nil {
			t.Fatal(err)
		}
		row.MountTarget += strings.Repeat("x", 16*1024-len(value))
		row.SHA256, err = removal.EvidenceRowDigest(row)
		if err != nil {
			t.Fatal(err)
		}
		rows[index] = row
		manifest.OrderedSHA256 = removal.AppendEvidenceDigest(manifest.OrderedSHA256, row.SHA256)
	}
	return fixture, audit, manifest, rows
}
