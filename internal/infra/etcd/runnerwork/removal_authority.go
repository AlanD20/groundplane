package runnerwork

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemovalAuthority fences the resource captured at admission, independently of
// Task history. The same comparisons protect execution and cancellation.
func (repository *Repository) RemovalAuthority(
	ctx context.Context,
	input Input,
	revision int64,
) ([]keyvalue.Condition, error) {
	if input.Validate() != nil || input.Operation != runnerallocation.RuntimeOperationRemove || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Runner removal authority is invalid")
	}
	id := input.Desired.ID
	keys := []string{runners.RunnerKey(id), runners.RunnerLifecycleKey(id),
		runners.RunnerRuntimeOwnershipKey(id), AppliedKey(id), AllocationPlanKey(id),
		deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, errs.New(errs.KindInternal, "Runner removal authority read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[1] == nil || read.Values[4] == nil || read.Values[5] == nil {
		return nil, errs.New(errs.KindStateConflict, "Runner captured removal authority is unavailable")
	}
	source := input.Removal
	desired, err := runners.EncodeRunnerDesiredRecord(input.Desired)
	if err != nil {
		return nil, err
	}
	lifecycle, err := runners.EncodeRunnerLifecycleRecord(source.Lifecycle)
	if err != nil {
		return nil, err
	}
	if read.Values[0].ModRevision != source.DesiredRevision || read.Values[1].ModRevision != source.LifecycleRevision ||
		!bytes.Equal(desired, read.Values[0].Value) || !bytes.Equal(lifecycle, read.Values[1].Value) {
		return nil, errs.New(errs.KindStateConflict, "Runner changed after removal was accepted")
	}
	plan, err := DecodeAllocationPlan(read.Values[4].Value)
	if err != nil {
		return nil, err
	}
	if plan.Digest() != input.Plan.Digest() {
		return nil, errs.New(errs.KindStateConflict, "Runner allocation plan changed after removal was accepted")
	}
	tombstone, err := runners.DecodeRunnerDeletionTombstone(read.Values[5].Value)
	if err != nil {
		return nil, err
	}
	if tombstone.TargetID != id || tombstone.TaskID != input.TaskID ||
		tombstone.TargetRevision != source.DesiredRevision ||
		tombstone.Phase != deletions.DeletionPhaseFinalizing {
		return nil, errs.New(errs.KindStateConflict, "Runner removal no longer owns the deletion fence")
	}
	if source.Ownership == nil {
		if read.Values[2] != nil || read.Values[3] != nil {
			return nil, errs.New(errs.KindStateConflict, "Runner acquired runtime ownership after removal was accepted")
		}
	} else {
		ownership, err := runners.EncodeRunnerRuntimeOwnership(*source.Ownership)
		if err != nil {
			return nil, err
		}
		if read.Values[2] == nil || !bytes.Equal(ownership, read.Values[2].Value) || read.Values[3] == nil {
			return nil, errs.New(errs.KindStateConflict, "Runner runtime ownership changed after removal was accepted")
		}
		applied, err := DecodeApplied(read.Values[3].Value)
		if err != nil {
			return nil, err
		}
		if applied.Plan.Digest() != plan.Digest() || applied.Evidence.ContainerID != source.Lifecycle.ContainerID ||
			applied.Evidence.Software != source.Lifecycle.Software {
			return nil, errs.New(errs.KindStateConflict, "Runner applied runtime differs from the captured removal")
		}
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	return conditions, nil
}
