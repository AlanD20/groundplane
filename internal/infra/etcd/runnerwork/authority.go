package runnerwork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func GrantKey(attemptID string, step runnerallocation.RunnerRuntimeStep) string {
	return "/v1/execution/runner/grants/" + attemptID + "/" + string(step)
}

// Grant persists one permission before a host effect. A repeated permission is
// not another execution grant: reconciliation must observe the issued effect.
func (repository *Repository) Grant(ctx context.Context, attempt workexecution.Attempt,
	step runnerallocation.RunnerRuntimeStep, at time.Time,
) error {
	input, conditions, err := repository.attemptAuthority(ctx, attempt, true)
	if err != nil {
		return err
	}
	steps := runnerallocation.CreationRuntimeSteps()
	if input.Operation == runnerallocation.RuntimeOperationRemove || attempt.Direction == workexecution.Recovery {
		steps = runnerallocation.RemovalRuntimeSteps()
	}
	found := false
	for _, candidate := range steps {
		if candidate == step {
			found = true
			break
		}
	}
	if !found || !at.Before(attempt.Deadline) {
		return errs.New(errs.KindStateConflict, "Runner step is outside this attempt's operation or deadline")
	}
	key := GrantKey(attempt.ID, step)
	conditions = append(conditions, keyvalue.Condition{Key: key})
	result, err := repository.store.Transact(ctx, conditions, []keyvalue.Mutation{{Type: keyvalue.MutationPut,
		Key: key, Value: []byte(at.UTC().Format(time.RFC3339Nano))}})
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Runner effect permission changed or was already issued; inspect its outcome")
	}
	return nil
}

// attemptAuthority reads every binding at one revision. Result recording may
// account for a previous executor's effects; issuing effects requires the current
// registered generation and an unrevoked forward action (or captured cleanup).
func (repository *Repository) attemptAuthority(ctx context.Context, expected workexecution.Attempt,
	forwardEffect bool,
) (Input, []keyvalue.Condition, error) {
	if expected.Validate() != nil || expected.Executor.Executor != workexecution.Controller || expected.FinishedAt != nil {
		return Input{}, nil, errs.New(errs.KindValidationFailed, "Runner execution attempt is invalid")
	}
	keys := []string{InputKey(expected.WorkItemID), workqueue.ItemKey(expected.WorkItemID),
		workqueue.AttemptKey(expected.ID), workqueue.TaskKey(expected.TaskID)}
	if forwardEffect {
		keys = append(keys, workqueue.ExecutorKey(expected.Executor))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return Input{}, nil, err
	}
	if read == nil {
		return Input{}, nil, errs.New(errs.KindInternal, "Runner execution authority read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return Input{}, nil, errs.New(errs.KindInternal, "Runner execution authority read is incomplete")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil {
			return Input{}, nil, errs.New(errs.KindStateConflict, "Runner execution authority is unavailable")
		}
		conditions[index] = keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision}
	}
	input, err := Decode(read.Values[0].Value)
	if err != nil {
		return Input{}, nil, err
	}
	item, err := workqueue.DecodeItem(read.Values[1].Value)
	if err != nil || input.MatchWork(item) != nil || item.ActiveAttemptID != expected.ID ||
		(item.Phase != workexecution.Running && item.Phase != workexecution.Assigned) {
		return Input{}, nil, errs.New(errs.KindStateConflict, "Runner work no longer owns this execution")
	}
	actual, err := workqueue.DecodeAttempt(read.Values[2].Value)
	if err != nil || actual.ID != expected.ID || actual.TaskID != expected.TaskID ||
		actual.WorkItemID != expected.WorkItemID || actual.Number != expected.Number ||
		actual.Executor != expected.Executor || actual.Direction != expected.Direction ||
		actual.Input != expected.Input || item.Input == nil || actual.Input != *item.Input || actual.FinishedAt != nil ||
		!actual.Deadline.Equal(expected.Deadline) ||
		(actual.StartedAt == nil) != (expected.StartedAt == nil) {
		return Input{}, nil, errs.New(errs.KindStateConflict, "Runner attempt assignment changed")
	}
	task, err := workqueue.DecodeTask(read.Values[3].Value)
	if err != nil || task.ID != expected.TaskID || task.CollectionStartedAt != nil {
		return Input{}, nil, errs.New(errs.KindStateConflict, "Runner Task authority changed")
	}
	if forwardEffect {
		executor, err := workqueue.DecodeExecutor(read.Values[4].Value)
		if err != nil || executor.Identity != expected.Executor || actual.StartedAt == nil || item.Phase != workexecution.Running ||
			!time.Now().UTC().Before(actual.Deadline) ||
			actual.Direction == workexecution.Forward && (task.Stop != nil || item.ForwardRevoked) {
			return Input{}, nil, errs.New(errs.KindStateConflict, "Runner attempt no longer permits host effects")
		}
	}
	claimKeys := make([]string, len(item.Claims))
	for index, claim := range item.Claims {
		claimKeys[index] = workqueue.ClaimKey(claim)
	}
	claims, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: claimKeys, Revision: read.ReadRevision})
	if err != nil {
		return Input{}, nil, err
	}
	if claims == nil {
		return Input{}, nil, errs.New(errs.KindInternal, "Runner resource claims read is missing")
	}
	defer keyvalue.ClearValues(claims.Values)
	if claims.ReadRevision != read.ReadRevision || len(claims.Values) != len(claimKeys) {
		return Input{}, nil, errs.New(errs.KindInternal, "Runner resource claims read is incomplete")
	}
	for index, value := range claims.Values {
		if value == nil {
			return Input{}, nil, errs.New(errs.KindStateConflict, "Runner execution lost its resource claim")
		}
		owner, err := recordcodec.Decode[workqueue.ClaimOwner](value.Value, "execution_owner")
		if err != nil || owner.TaskID != item.TaskID || owner.WorkItemID != item.ID {
			return Input{}, nil, errs.New(errs.KindStateConflict, "Runner resource claim changed owner")
		}
		conditions = append(conditions, keyvalue.Condition{Key: claimKeys[index], ModRevision: value.ModRevision})
	}
	var resourceConditions []keyvalue.Condition
	if input.Operation == runnerallocation.RuntimeOperationRemove {
		resourceConditions, err = repository.RemovalAuthority(ctx, input, read.ReadRevision)
	} else {
		resourceConditions, err = repository.creationAuthority(ctx, input, read.ReadRevision)
	}
	if err != nil {
		return Input{}, nil, err
	}
	conditions = append(conditions, resourceConditions...)
	return input, conditions, nil
}

func (repository *Repository) creationAuthority(ctx context.Context, input Input, revision int64) ([]keyvalue.Condition, error) {
	id := input.Desired.ID
	keys := []string{runners.RunnerKey(id), runners.RunnerLifecycleKey(id), runners.RunnerRuntimeOwnershipKey(id),
		AppliedKey(id), deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, errs.New(errs.KindInternal, "Runner resource authority read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil {
		return nil, errs.New(errs.KindStateConflict, "Runner resource authority is unavailable")
	}
	current, err := runners.DecodeRunnerAggregate(read.Values[0], read.Values[1])
	if err != nil {
		return nil, err
	}
	desired, err := runners.EncodeRunnerDesiredRecord(input.Desired)
	if err != nil {
		return nil, err
	}
	if input.Operation != runnerallocation.RuntimeOperationCreate || !bytes.Equal(desired, read.Values[0].Value) ||
		current.CreateTaskID != input.TaskID || current.Allocation != input.Plan.Allocation ||
		current.RuntimeEpoch > input.Plan.RuntimeEpoch || current.ProvisioningState == runners.RunnerProvisioningReady ||
		read.Values[2] != nil || read.Values[3] != nil || read.Values[4] != nil {
		return nil, errs.New(errs.KindStateConflict, "Runner captured creation no longer owns the host allocation")
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	return conditions, nil
}
