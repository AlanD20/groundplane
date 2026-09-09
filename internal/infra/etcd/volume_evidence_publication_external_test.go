package etcd_test

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/volumeremoval"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
)

// Rationale: a runtime's caller-supplied digest is not proof of sealed stored
// evidence. Desired state, policy, Task and removal ownership must stay private.
func TestVolumeEvidencePublicationRequiresStoredSeal(t *testing.T) {
	ctx := context.Background()
	fixture := etcd.NewVolumePolicyDesiredFixture(t)
	runtime := fixture.PrepareRemovalRecords(t)
	if _, err := fixture.Store.Delete(ctx, removal.EvidenceSealKey(runtime.OperationID)); err != nil {
		t.Fatal(err)
	}
	stageVolumePolicyDesired(t, fixture)
	before := fixture.Revision()
	result, err := fixture.Publish(ctx)
	if err == nil {
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || conflict == nil || outcome != etcd.IdempotencyKnownConflict {
			t.Fatalf("unsealed evidence authorized publication: %v/%v/%v", outcome, conflict, classifyErr)
		}
	}
	fixture.AssertUnpublished(t, before)
}

// Rationale: the real staging/sealing producer and sole desired publisher must
// agree on the durable evidence, including the largest policy publication.
func TestVolumeEvidencePublicationConsumesRealSeal(t *testing.T) {
	ctx := context.Background()
	for _, maximum := range []bool{false, true} {
		name := "last-source"
		if maximum {
			name = "maximum-policy"
		}
		t.Run(name, func(t *testing.T) {
			fixture := etcd.NewVolumePolicyDesiredFixture(t)
			if maximum {
				fixture.UseMaximumSelection(t)
			}
			runtime := fixture.PrepareRemovalRecords(t)
			manifest := stageRealVolumePublicationEvidence(t, fixture, runtime, "")
			stageVolumePolicyDesired(t, fixture)
			before := fixture.Revision()
			earliest := time.Now().UTC()
			result, err := fixture.Publish(ctx)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err := result.Classify()
			if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownApplied {
				t.Fatal("real seal did not publish", outcome, conflict, err)
			}
			if maximum {
				fixture.AssertMaximumSelectionPublished(t)
			} else {
				fixture.AssertAtomicPolicy(t, earliest)
			}
			for _, key := range []string{removal.EvidenceManifestKey(runtime.OperationID), removal.EvidenceCursorKey(runtime.OperationID), removal.EvidenceSealKey(runtime.OperationID)} {
				stored, err := fixture.Store.Get(ctx, key)
				if err != nil || stored.Entry == nil || stored.Entry.ModRevision > before {
					t.Fatal("publication rewrote immutable evidence", err)
				}
			}
			stored, err := fixture.Store.Get(ctx, removal.RuntimeKey(runtime.OperationID))
			if err != nil || stored.Entry == nil {
				t.Fatal("missing published runtime", err)
			}
			published, err := removal.DecodeRuntime(stored.Entry.Value)
			value, encodeErr := removal.EncodeEvidenceManifest(manifest)
			if err != nil || encodeErr != nil || published.EvidenceManifestSHA256 != sha256.Sum256(value) {
				t.Fatal("runtime did not bind actual sealed evidence", err, encodeErr)
			}
			before = fixture.Revision()
			result, err = fixture.Publish(ctx)
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, err = result.Classify()
			if err != nil || conflict != nil || outcome != etcd.IdempotencyKnownExisting ||
				fixture.Revision() != before {
				t.Fatal("publication replay was not read-only", outcome, conflict, err)
			}
		})
	}
}

// Rationale: a syntactically valid sealed set for a different accepted impact,
// scope, baseline, immutable key or desired revision must never authorize DELETE.
func TestVolumeEvidencePublicationRejectsDifferentSealedBinding(t *testing.T) {
	for _, changed := range []string{"environment", "volume", "key", "source", "desired", "impact", "digest"} {
		t.Run(changed, func(t *testing.T) {
			fixture := etcd.NewVolumePolicyDesiredFixture(t)
			runtime := fixture.PrepareRemovalRecords(t)
			stageRealVolumePublicationEvidence(t, fixture, runtime, changed)
			stageVolumePolicyDesired(t, fixture)
			before := fixture.Revision()
			if _, err := fixture.Publish(context.Background()); err == nil {
				t.Fatal("different sealed evidence authorized removal")
			}
			fixture.AssertUnpublished(t, before)
		})
	}
}

// Rationale: changing any member of the evidence witness at final commit must
// defeat desired/policy/Task publication and preserve the winning record.
func TestVolumeEvidencePublicationFencesEveryEvidenceRecord(t *testing.T) {
	ctx := context.Background()
	for _, suffix := range []string{"manifest", "cursor", "seal"} {
		for _, late := range []bool{false, true} {
			name := suffix + "-missing"
			if late {
				name = suffix + "-late"
			}
			t.Run(name, func(t *testing.T) {
				fixture := etcd.NewVolumePolicyDesiredFixture(t)
				runtime := fixture.PrepareRemovalRecords(t)
				stageRealVolumePublicationEvidence(t, fixture, runtime, "")
				stageVolumePolicyDesired(t, fixture)
				key := removal.EvidenceRoot(runtime.OperationID) + suffix
				if late {
					fixture.EvidenceBeforePublication = func() {
						stored, err := fixture.Store.Get(ctx, key)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := fixture.Store.Put(ctx, key, stored.Entry.Value); err != nil {
							t.Fatal(err)
						}
					}
				} else if _, err := fixture.Store.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
				before := fixture.Revision()
				result, err := fixture.Publish(ctx)
				if late {
					if err != nil {
						t.Fatal(err)
					}
					outcome, _, conflict, err := result.Classify()
					if err != nil || conflict == nil || outcome != etcd.IdempotencyKnownConflict {
						t.Fatal("late evidence change authorized publication", outcome, conflict, err)
					}
					before++
				} else if err == nil {
					t.Fatal("missing evidence authorized publication")
				}
				fixture.AssertUnpublished(t, before)
			})
		}
	}
}

func stageRealVolumePublicationEvidence(t *testing.T, fixture *etcd.VolumePolicyDesiredFixture,
	runtime removal.Runtime, changed string) removal.EvidenceManifest {
	t.Helper()
	ctx := context.Background()
	stored, err := fixture.Store.Get(ctx, removal.EvidenceManifestKey(runtime.OperationID))
	if err != nil || stored.Entry == nil {
		t.Fatal("missing fixture manifest", err)
	}
	manifest, err := removal.DecodeEvidenceManifest(stored.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	// Remove only this hermetic fixture's preseeded snapshot, then exercise the
	// actual bounded producer. This is not a production cleanup implementation.
	if _, err := fixture.Store.Transact(ctx, nil, []etcd.Mutation{{Type: etcd.MutationDelete, Key: removal.EvidenceRoot(runtime.OperationID), Prefix: true}}); err != nil {
		t.Fatal(err)
	}
	switch changed {
	case "environment":
		manifest.EnvironmentID = ids.New(ids.KindEnvironment)
	case "volume":
		manifest.VolumeID = ids.New(ids.KindVolume)
	case "key":
		manifest.Key = "different-key"
	case "source":
		manifest.SourceRevisionID = ids.New(ids.KindTask)
	case "desired":
		manifest.DesiredRevisionID = ids.New(ids.KindTask)
	case "impact":
		manifest.ImpactSHA256 = sha256.Sum256([]byte("different accepted impact"))
	}
	repository, err := volumeremoval.NewEvidenceRepository(etcd.NewVolumeEvidenceStageAudit(fixture.Store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Begin(ctx, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Stage(ctx, manifest, nil); err != nil {
		t.Fatal(err)
	}
	seal, err := repository.Seal(ctx, manifest)
	if err != nil {
		t.Fatal(err)
	}
	runtime.EvidenceManifestSHA256 = seal.Seal.Record.ManifestSHA256
	if changed == "digest" {
		runtime.EvidenceManifestSHA256 = sha256.Sum256([]byte("unrelated manifest"))
	}
	fixture.Task.Params = etcd.EnvironmentVolumeRemovalTaskParams(runtime, 1)
	initial, err := removal.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatal(err)
	}
	fixture.Initial = &initial
	return manifest
}
