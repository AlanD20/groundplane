package volumeremoval

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an exact running Task cannot authorize cleanup after the stable
// Volume owner or Environment lock disappears or belongs to another operation,
// including at commit.
func TestVolumeRemovalExecutionRequiresExactOwner(t *testing.T) {
	for _, family := range []string{"volume", "environment"} {
		t.Run(family, func(t *testing.T) {
			for _, action := range []string{"detach", "begin", "redeliver", "complete"} {
				changes := []string{"none", "missing", "operation", "environment", "volume", "corrupt"}
				if action != "redeliver" { // Redelivery only reads; there is no commit race to inject.
					changes = append(changes, "late")
				}
				for _, change := range changes {
					t.Run(action+"/"+change, func(t *testing.T) {
						ctx := context.Background()
						store, repository, runtime, task, marker := environmentVolumeRemovalRuntimeFixture(t)
						persistEnvironmentVolumeRemovalTaskAndMarker(t, store, task, marker)
						if _, err := repository.Create(ctx, runtime, task); err != nil {
							t.Fatal(err)
						}
						for index, checkpoint := range []removalrecord.Checkpoint{
							removalrecord.RevisionStaged, removalrecord.DesiredPublished,
						} {
							if _, err := repository.AdvanceCheckpoint(ctx, runtime.OperationID, checkpoint,
								runtime.CreatedAt.Add(time.Duration(index+1)*time.Second)); err != nil {
								t.Fatal(err)
							}
						}
						assignment := assignEnvironmentVolumeRemovalTask(
							t, store, task.ID, runtime.OperationID, runtime.CreatedAt.Add(3*time.Second),
						)
						if action != "detach" {
							if _, err := repository.MarkConsumersDetached(ctx, assignment, runtime.CreatedAt.Add(4*time.Second)); err != nil {
								t.Fatal(err)
							}
						}
						var pathResult EnvironmentVolumeRemovalPathResult
						if action == "redeliver" || action == "complete" {
							pending, _, err := repository.BeginPathCall(
								ctx,
								assignment,
								runtime.CreatedAt.Add(5*time.Second),
							)
							if err != nil {
								t.Fatal(err)
							}
							completion := removalrecord.Completion{
								OperationID: runtime.OperationID, RequestOrdinal: pending.Record.RequestOrdinal,
								RequestSHA256: pending.Record.RequestSHA256, MutationCount: 1, DirectoryAbsent: true,
								CompletedAt: runtime.CreatedAt.Add(6 * time.Second),
							}
							completion.ResponseBytes = removalrecord.PathResponseBytes(completion)
							completion.ResponseSHA256 = removalrecord.PathResponseDigest(completion)
							pathResult = environmentVolumeRemovalPathResult(assignment, completion)
						}
						owner := removalrecord.Owner{
							VolumeID:      runtime.VolumeID,
							EnvironmentID: runtime.EnvironmentID,
							OperationID:   runtime.OperationID,
						}
						switch change {
						case "operation", "late":
							owner.OperationID = ids.NewAt(ids.KindOperation, runtime.CreatedAt, 99)
						case "environment":
							owner.EnvironmentID = ids.NewAt(ids.KindEnvironment, runtime.CreatedAt, 99)
						case "volume":
							owner.VolumeID = ids.NewAt(ids.KindVolume, runtime.CreatedAt, 99)
						}
						value, err := removalrecord.EncodeOwner(owner)
						if err != nil {
							t.Fatal(err)
						}
						mutation := testkeyvalue.Mutation{
							Type:  testkeyvalue.MutationPut,
							Key:   removalrecord.OwnerKey(runtime.VolumeID),
							Value: value,
						}
						if family == "environment" {
							mutation.Key = removalrecord.EnvironmentLockKey(runtime.EnvironmentID)
						}
						if change == "missing" {
							mutation.Type, mutation.Value = testkeyvalue.MutationDelete, nil
						} else if change == "corrupt" {
							mutation.Value = []byte("corrupt")
						}
						if change == "late" {
							repository.store = &ownerRaceStore{memoryHierarchyStore: store, mutation: mutation}
						} else if change != "none" {
							if _, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{mutation}); err != nil {
								t.Fatal(err)
							}
						}
						before := store.revision
						switch action {
						case "detach":
							_, err = repository.MarkConsumersDetached(
								ctx,
								assignment,
								runtime.CreatedAt.Add(6*time.Second),
							)
						case "begin", "redeliver":
							_, _, err = repository.BeginPathCall(ctx, assignment, runtime.CreatedAt.Add(6*time.Second))
						case "complete":
							_, _, err = repository.CompletePathCall(ctx, pathResult)
						}
						if change == "none" {
							if err != nil {
								t.Fatalf("valid owner rejected: %v", err)
							}
							return
						}
						if !isKind(err, errs.KindStateConflict) {
							t.Fatalf("changed owner authorized %s: %v", action, err)
						}
						if change == "late" {
							before++ // Only the winning replacement may write.
						}
						if store.revision != before {
							t.Fatal("rejected cleanup wrote state")
						}
						origin, found, err := repository.ReplayRootResponse(
							ctx, runtime.OperationID, volumeRemovalRootLocator(runtime), runtime.IntentSHA256,
						)
						if err != nil || !found || origin != task.ID || store.revision != before {
							t.Fatalf("owner loss broke read-only root replay: %q/%v/%v", origin, found, err)
						}
					})
				}
			}
		})
	}
}

type ownerRaceStore struct {
	*memoryHierarchyStore
	mutation testkeyvalue.Mutation
}

func (store *ownerRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{store.mutation}); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}
