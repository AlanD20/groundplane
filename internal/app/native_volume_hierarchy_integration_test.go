package app

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/volume"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-160: a native queued Volume removal, not an old removal record, must
// block parent deletion and defeat the final deletion CAS if published late.
// The idle control closes the creation Task so it cannot mask missing guards.
func TestNativeVolumeRemovalExcludesParentDeletion(t *testing.T) {
	for _, kind := range []hierarchydeletion.HierarchyDeletionTargetKind{
		hierarchydeletion.HierarchyDeletionTargetEnvironment, hierarchydeletion.HierarchyDeletionTargetProject, hierarchydeletion.HierarchyDeletionTargetTenant} {
		for _, mode := range []string{"idle", "held", "late"} {
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				f := newNativeVolumeFixture(t)
				finishNativeVolumeFixture(t, f, f.created.TaskID)
				finishNativeVolumeTask(t, f, f.created.TaskID)
				accepted, _, err := f.queue.ReadTask(t.Context(), f.created.TaskID, 0)
				if err != nil {
					t.Fatal(err)
				}
				begin := nativeVolumeParentDeletion(t, accepted.Task.Scope, kind)
				publish := func() {
					impact, err := f.reads.GetVolumeDeletionImpact(t.Context(), f.created.Volume.ID, "", 40)
					if err != nil || !impact.Complete {
						t.Fatalf("Volume impact: %v", err)
					}
					response, err := f.mutations.RemoveVolume(
						t.Context(),
						f.created.Volume.ID,
						impact.ImpactToken,
						"data",
						"native-parent-racing-volume",
					)
					if err != nil {
						t.Fatal(err)
					}
					var task api.TaskAccepted
					if err := json.Unmarshal(response.Body, &task); err != nil || task.TaskID == "" {
						t.Fatalf("Volume response: %v", err)
					}
				}
				injected := false
				if mode == "held" {
					publish()
				}
				if mode == "late" {
					f.store.beforeCommit = func(mutations []keyvalue.Mutation) {
						for _, mutation := range mutations {
							if mutation.Key != workqueue.TaskKey(begin.TaskID) {
								continue
							}
							f.store.beforeCommit = nil
							publish()
							injected = true
							return
						}
					}
				}
				journal, err := etcd.NewHierarchyDeletionRepository(f.store)
				if err != nil {
					t.Fatal(err)
				}
				_, err = journal.Begin(t.Context(), begin)
				if mode == "idle" {
					if err != nil {
						t.Fatalf("idle parent deletion: %v", err)
					}
					return
				}
				want := errs.KindResourceInUse
				if mode == "late" {
					want = errs.KindStateConflict
					if !injected {
						t.Fatal("late Volume publication was not reached")
					}
				}
				if got, _ := errs.KindOf(err); got != want {
					t.Fatalf("parent ignored native Volume removal: %v", err)
				}
				marker, err := idempotency.IdempotencyMarkerKey(begin.Marker.Locator)
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range []string{workqueue.TaskKey(begin.TaskID), marker,
					hierarchydeletion.HierarchyDeletionTombstoneKey(string(kind), begin.TargetID)} {
					read, err := f.store.Get(t.Context(), key)
					if err != nil || read.Entry != nil {
						t.Fatalf("losing parent publication wrote %s: %v", key, err)
					}
				}
			})
		}
	}
}

func finishNativeVolumeTask(t *testing.T, f *nativeVolumeFixture, taskID string) {
	t.Helper()
	finished, err := f.queue.ReadFinishedWork(t.Context(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := volume.NewAgentWork(f.inputs, f.queue)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	proof, err := owner.PrepareFinish(t.Context(), finished, at)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := f.queue.PrepareTaskFinish(
		t.Context(),
		finished.Task,
		at,
		at.Add(workexecution.TaskHistoryRetention),
	)
	if err != nil {
		t.Fatal(err)
	}
	f.commit(t, proof, terminal)
}

// Protected request bytes are supplied here; this checks storage admission,
// not the parent-removal public request canonicalizer or physical child cleanup.
func nativeVolumeParentDeletion(
	t *testing.T,
	scope workexecution.Scope,
	kind hierarchydeletion.HierarchyDeletionTargetKind,
) etcd.HierarchyDeletionBegin {
	t.Helper()
	id, operation := scope.EnvironmentID, hierarchydeletion.HierarchyDeletionOperationEnvironment
	if kind == hierarchydeletion.HierarchyDeletionTargetProject {
		id, operation = scope.ProjectID, hierarchydeletion.HierarchyDeletionOperationProject
	}
	if kind == hierarchydeletion.HierarchyDeletionTargetTenant {
		id, operation = scope.TenantID, hierarchydeletion.HierarchyDeletionOperationTenant
	}
	at := time.Now().UTC().Truncate(time.Millisecond)
	taskID := ids.New(ids.KindTask)
	key := "native-parent-removal-" + taskID
	ciphertext := []byte("supplied-protected-parent-intent")
	digest := sha256.Sum256(ciphertext)
	keyDigest := sha256.Sum256([]byte(key))
	marker := idempotency.IdempotencyMarker{
		Kind: idempotency.IdempotencyMarkerTask, State: idempotency.IdempotencyMarkerPending, TaskID: taskID, CreatedAt: at, UpdatedAt: at,
		Locator: idempotency.IdempotencyLocator{
			ScopeKind: idempotency.IdempotencyScopeKind(kind),
			ScopeID:   id,
			Method:    "DELETE",
			Route:     "/" + string(kind) + "/{id}",
			Key:       key,
		},
		Intent: idempotency.ProtectedIntentRecord{
			EnvelopeVersion:  1,
			Cipher:           "age-x25519",
			DigestAlgorithm:  "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]),
			Ciphertext:       ciphertext,
		},
		Response: idempotency.IdempotencyResponse{
			Status:      202,
			ContentKind: "application/json",
			Body:        []byte(`{"task_id":"` + taskID + `"}`),
		},
	}
	return etcd.HierarchyDeletionBegin{
		OperationID:     "del_" + strings.Repeat("a", 32),
		OperationKind:   operation,
		TargetKind:      kind,
		TargetID:        id,
		TaskID:          taskID,
		IdempotencyHash: hex.EncodeToString(keyDigest[:]),
		Marker:          marker,
		CreatedAt:       at,
		DeadlineAt:      at.Add(hierarchydeletion.AttemptTimeout),
	}
}
