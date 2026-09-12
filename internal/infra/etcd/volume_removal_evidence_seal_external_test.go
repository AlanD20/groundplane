package etcd_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a complete crash cursor cannot substitute for exact stored-row
// verification; the seal must survive a lost response without rewriting rows.
func TestVolumeEvidenceSealVerifiesStoredSetAndReplays(t *testing.T) {
	ctx := context.Background()
	for _, count := range []int{0, 45, 90} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			fixture, audit, manifest, rows := volumeEvidenceFixture(t, count)
			backend := &volumeEvidenceSealStore{VolumeEvidenceStageAudit: audit}
			repository, err := volumeremoval.NewEvidenceRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Begin(ctx, manifest); err != nil {
				t.Fatal(err)
			}
			for start := 0; start < count; start += removal.EvidenceBatchRows {
				if _, err := repository.Stage(ctx, manifest, rows[start:min(start+removal.EvidenceBatchRows, count)]); err != nil {
					t.Fatal(err)
				}
			}
			before := fixture.Revision()
			audit.LoseResponse = true
			if _, err := repository.Seal(ctx, manifest); err == nil {
				t.Fatal("seal response was not lost")
			}
			if fixture.Revision() != before+1 || audit.Comparisons != 3 || audit.Mutations != 1 ||
				audit.WireBytes != 777 {
				t.Fatal("seal was not one bounded commit")
			}
			t.Logf("seal transaction: %d/%d/%d", audit.Comparisons, audit.Mutations, audit.WireBytes)
			for _, request := range backend.ranges {
				if request.Limit != 44 || request.Revision != before ||
					request.Prefix != removal.EvidenceRoot(manifest.OperationID) {
					t.Fatal("verification did not use bounded fixed-revision pages")
				}
			}
			if len(backend.ranges) != (count+2+43)/44 {
				t.Fatal("verification did not visit the entire namespace")
			}
			repository, err = volumeremoval.NewEvidenceRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			result, err := repository.Seal(ctx, manifest)
			if err != nil || !result.Replayed || result.Seal.Revision != before+1 ||
				result.Seal.Record.VerifiedRevision != before ||
				fixture.Revision() != before+1 {
				t.Fatal("seal replay changed authority", err)
			}
			if result.Seal.Record.ManifestRevision != result.State.Manifest.Revision ||
				result.Seal.Record.CursorRevision != result.State.Cursor.Revision {
				t.Fatal("seal lost its exact metadata fences")
			}
			value, err := removal.EncodeEvidenceSeal(result.Seal.Record, manifest)
			if err != nil || len(value) != 62 {
				t.Fatal("seal encoding is not bounded", err)
			}
			decoded, err := removal.DecodeEvidenceSeal(value, manifest)
			if err != nil || decoded != result.Seal.Record {
				t.Fatal("seal round trip", err)
			}
			if _, err := removal.DecodeEvidenceSeal(append(value, 0), manifest); err == nil {
				t.Fatal("seal accepted trailing bytes")
			}
			for _, mutate := range []func(*removal.EvidenceSeal){
				func(seal *removal.EvidenceSeal) { seal.ManifestSHA256[0] ^= 1 },
				func(seal *removal.EvidenceSeal) { seal.ManifestRevision = 0 },
				func(seal *removal.EvidenceSeal) { seal.CursorRevision = seal.ManifestRevision - 1 },
				func(seal *removal.EvidenceSeal) { seal.VerifiedRevision = seal.CursorRevision - 1 },
			} {
				invalid := result.Seal.Record
				mutate(&invalid)
				if _, err := removal.EncodeEvidenceSeal(invalid, manifest); err == nil {
					t.Fatal("invalid seal binding encoded")
				}
			}
			for length := 0; length < len(value); length++ {
				if _, err := removal.DecodeEvidenceSeal(value[:length], manifest); err == nil {
					t.Fatal("truncated seal decoded")
				}
			}
			for _, offset := range []int{0, 4, 38, 46, 54} {
				corrupt := append([]byte(nil), value...)
				corrupt[offset] = 255
				if _, err := removal.DecodeEvidenceSeal(corrupt, manifest); err == nil {
					t.Fatal("invalid seal header or overflowing revision decoded")
				}
			}
			stage, err := repository.Stage(ctx, manifest, rows[:min(count, removal.EvidenceBatchRows)])
			if err != nil || !stage.Replayed || fixture.Revision() != before+1 {
				t.Fatal("staging modified sealed evidence", err)
			}
		})
	}
}

// Rationale: cursor completion and even an existing seal must not conceal
// missing, substituted, or extra evidence, and failed scans never repair it.
func TestVolumeEvidenceSealRejectsIncompleteOrChangedRows(t *testing.T) {
	ctx := context.Background()
	for _, fault := range []string{"not-staged", "missing-first", "missing-middle", "missing-last", "changed-row", "extra-row", "unknown-key", "sealed-missing-row"} {
		t.Run(fault, func(t *testing.T) {
			fixture, audit, manifest, rows := volumeEvidenceFixture(t, 45)
			repository, err := volumeremoval.NewEvidenceRepository(audit)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Begin(ctx, manifest); err != nil {
				t.Fatal(err)
			}
			if fault != "not-staged" {
				if _, err := repository.Stage(ctx, manifest, rows[:44]); err != nil {
					t.Fatal(err)
				}
				if _, err := repository.Stage(ctx, manifest, rows[44:]); err != nil {
					t.Fatal(err)
				}
			}
			if fault == "sealed-missing-row" {
				if _, err := repository.Seal(ctx, manifest); err != nil {
					t.Fatal(err)
				}
			}
			switch fault {
			case "missing-first", "sealed-missing-row":
				if _, err := fixture.Store.Delete(ctx, removal.EvidenceRowKey(manifest.OperationID, 1)); err != nil {
					t.Fatal(err)
				}
			case "missing-middle":
				if _, err := fixture.Store.Delete(ctx, removal.EvidenceRowKey(manifest.OperationID, 23)); err != nil {
					t.Fatal(err)
				}
			case "missing-last":
				if _, err := fixture.Store.Delete(ctx, removal.EvidenceRowKey(manifest.OperationID, 45)); err != nil {
					t.Fatal(err)
				}
			case "changed-row":
				row := rows[22]
				row.ReadOnly = !row.ReadOnly
				row.SHA256, err = removal.EvidenceRowDigest(row)
				if err != nil {
					t.Fatal(err)
				}
				value, err := removal.EncodeEvidenceRow(row)
				if err != nil {
					t.Fatal(err)
				}
				stored, err := fixture.Store.Get(ctx, removal.EvidenceCursorKey(manifest.OperationID))
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.Store.Transact(ctx, nil, []etcd.Mutation{
					{Type: etcd.MutationPut, Key: removal.EvidenceRowKey(manifest.OperationID, 23), Value: value},
					{Type: etcd.MutationPut, Key: removal.EvidenceCursorKey(manifest.OperationID), Value: stored.Entry.Value},
				}); err != nil {
					t.Fatal(err)
				}
			case "extra-row", "unknown-key":
				key := removal.EvidenceRowKey(manifest.OperationID, 46)
				if fault == "unknown-key" {
					key = removal.EvidenceRoot(manifest.OperationID) + "unknown"
				}
				if _, err := fixture.Store.Put(ctx, key, []byte("orphan")); err != nil {
					t.Fatal(err)
				}
			}
			before := fixture.Revision()
			if _, err := repository.Seal(ctx, manifest); err == nil || fixture.Revision() != before {
				t.Fatal("invalid evidence was sealed or repaired", err)
			}
		})
	}
}

// Rationale: a metadata or cleanup change after the fixed snapshot must win
// over sealing, while a concurrent equal seal is recoverable by read-only replay.
func TestVolumeEvidenceSealFencesMetadataAndRecoversWinner(t *testing.T) {
	ctx := context.Background()
	for _, fault := range []string{"manifest", "cursor", "cleanup-between-pages", "equal-seal", "compacted"} {
		t.Run(fault, func(t *testing.T) {
			fixture, audit, manifest, rows := volumeEvidenceFixture(t, 45)
			backend := &volumeEvidenceSealStore{VolumeEvidenceStageAudit: audit}
			repository, err := volumeremoval.NewEvidenceRepository(backend)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Begin(ctx, manifest); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Stage(ctx, manifest, rows[:44]); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.Stage(ctx, manifest, rows[44:]); err != nil {
				t.Fatal(err)
			}
			before := fixture.Revision()
			switch fault {
			case "manifest", "cursor":
				audit.BeforeCommit = func() {
					key := removal.EvidenceRoot(manifest.OperationID) + fault
					stored, err := fixture.Store.Get(ctx, key)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := fixture.Store.Put(ctx, key, stored.Entry.Value); err != nil {
						t.Fatal(err)
					}
				}
			case "cleanup-between-pages":
				backend.afterRange = func() {
					if _, err := fixture.Store.Transact(ctx, nil, []etcd.Mutation{
						{Type: etcd.MutationDelete, Key: removal.EvidenceCursorKey(manifest.OperationID)},
						{Type: etcd.MutationDelete, Key: removal.EvidenceRowKey(manifest.OperationID, 45)},
					}); err != nil {
						t.Fatal(err)
					}
				}
			case "equal-seal":
				audit.BeforeCommit = func() {
					if _, err := repository.Seal(ctx, manifest); err != nil {
						t.Fatal(err)
					}
				}
			case "compacted":
				backend.rangeError = errs.New(errs.KindCursorExpired, "compacted evidence snapshot")
			}
			_, err = repository.Seal(ctx, manifest)
			if err == nil {
				t.Fatal("seal did not reject changed snapshot authority")
			}
			wantRevision := before + 1
			if fault == "compacted" {
				wantRevision = before
				if kind, ok := errs.KindOf(err); !ok || kind != errs.KindCursorExpired {
					t.Fatal("compaction was hidden", err)
				}
			}
			if fixture.Revision() != wantRevision {
				t.Fatal("losing seal wrote state")
			}
			if fault == "equal-seal" {
				result, err := repository.Seal(ctx, manifest)
				if err != nil || !result.Replayed || fixture.Revision() != wantRevision {
					t.Fatal("winner replay changed state", err)
				}
			} else {
				stored, err := fixture.Store.Get(ctx, removal.EvidenceSealKey(manifest.OperationID))
				if err != nil || stored.Entry != nil {
					t.Fatal("losing seal became visible", err)
				}
			}
		})
	}
}

type volumeEvidenceSealStore struct {
	*etcd.VolumeEvidenceStageAudit
	ranges     []etcd.RangeRequest
	afterRange func()
	rangeError error
}

func (store *volumeEvidenceSealStore) Range(ctx context.Context, request etcd.RangeRequest) (*etcd.RangeResult, error) {
	store.ranges = append(store.ranges, request)
	if store.rangeError != nil {
		return nil, store.rangeError
	}
	result, err := store.Store.Range(ctx, request)
	if store.afterRange != nil {
		after := store.afterRange
		store.afterRange = nil
		after()
	}
	return result, err
}
