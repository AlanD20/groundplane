package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRunnerCreationResult must join native work completion in one CAS.
// Runtime ownership and applied configuration never require retained Task data.
func (repository *RunnerRepository) PrepareRunnerCreationResult(ctx context.Context,
	input keyvalue.Versioned[runnerwork.Input], result keyvalue.Versioned[runnerwork.Result],
) (workqueue.Change, error) {
	capture, outcome := input.Record, result.Record
	if input.Revision <= 0 || result.Revision <= 0 || outcome.Validate(capture) != nil ||
		capture.Operation != runnerallocation.RuntimeOperationCreate {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner result does not own a captured creation")
	}
	id := capture.Desired.ID
	keys := []string{runners.RunnerKey(id), runners.RunnerLifecycleKey(id), runners.RunnerRuntimeOwnershipKey(id),
		runnerwork.AppliedKey(id), deletions.TombstoneKey(string(deletions.DeletionTargetRunner), id)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Runner settlement read is missing")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision <= 0 || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner settlement resource is unavailable")
	}
	record, err := runners.DecodeRunnerAggregate(read.Values[0], read.Values[1])
	if err != nil {
		return workqueue.Change{}, err
	}
	if record.CreateTaskID != capture.TaskID || record.Allocation != capture.Plan.Allocation ||
		!runnerCreationDesiredEqual(record.Desired, capture.Desired) || record.RuntimeEpoch > capture.Plan.RuntimeEpoch || read.Values[4] != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner settlement allocation or ownership changed")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: runnerwork.InputKey(capture.WorkItemID), ModRevision: input.Revision},
		{Key: runnerwork.ResultKey(outcome.Attempt.ID), ModRevision: result.Revision},
	}}
	for index, key := range keys {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])})
	}
	record.ProvisioningState = runners.RunnerProvisioningFailed
	switch outcome.Effects {
	case runnerwork.RuntimeReady:
		if read.Values[2] != nil || read.Values[3] != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner already has a published runtime")
		}
		evidence := *outcome.Evidence
		record.RuntimeEpoch, record.ContainerID, record.Software = capture.Plan.RuntimeEpoch, evidence.ContainerID, evidence.Software
		record.ProvisioningState = runners.RunnerProvisioningReady
		ownership := runners.RunnerRuntimeOwnershipRecord{RunnerID: id, RuntimeEpoch: capture.Plan.RuntimeEpoch,
			DaemonSocketEndpoint: "unix://" + capture.Plan.Paths.RawSocket, DaemonInstanceNonce: evidence.DaemonNonce,
			SocketDevice: evidence.SocketDevice, SocketInode: evidence.SocketInode, CreatedAt: outcome.RecordedAt}
		value, err := runners.EncodeRunnerRuntimeOwnership(ownership)
		if err != nil {
			return workqueue.Change{}, err
		}
		applied, err := runnerwork.EncodeApplied(runnerwork.AppliedRuntime{Plan: capture.Plan.Clone(), Evidence: evidence})
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Mutations = append(change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[2], Value: value},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[3], Value: applied})
	case runnerwork.NoEffects, runnerwork.RuntimeRemoved:
		// A creation never replaces an applied Runner. Existing ownership must
		// not be erased merely because this attempt reports no remaining effects.
		if read.Values[2] != nil || read.Values[3] != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner cleanup conflicts with an applied runtime")
		}
		record.ContainerID, record.Software = "", buildversion.ContainerIdentity{}
	case runnerwork.EffectsUnknown:
		if read.Values[2] != nil || read.Values[3] != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner failed attempt conflicts with an applied runtime")
		}
	}
	value, err := runners.EncodeRunnerLifecycleRecord(record.RunnerLifecycleRecord)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[1], Value: value})
	return change, nil
}
