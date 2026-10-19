package workqueue

import (
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadExecutor returns a revision-zero snapshot for an unregistered placement.
// Authentication and host ownership are checked by the channel owner, not inferred
// from a worker-supplied generation or host label.
func (repository *Repository) ReadExecutor(
	ctx context.Context,
	target workexecution.Placement,
) (ExecutorSnapshot, error) {
	if err := target.Validate(); err != nil {
		return ExecutorSnapshot{}, err
	}
	identity := workexecution.Identity{Executor: target.Executor, HostID: target.HostID}
	read, err := repository.read(ctx, []string{ExecutorKey(identity)}, 0)
	if err != nil {
		return ExecutorSnapshot{}, err
	}
	if read.Values[0] == nil {
		return ExecutorSnapshot{}, nil
	}
	record, err := DecodeExecutor(read.Values[0].Value)
	if err != nil {
		return ExecutorSnapshot{}, err
	}
	if record.Identity.Executor != target.Executor || record.Identity.HostID != target.HostID {
		return ExecutorSnapshot{}, corrupt()
	}
	return ExecutorSnapshot{Record: record, Revision: read.Values[0].ModRevision}, nil
}

// PrepareRegistration advances the persisted generation only for a new executor
// process. A reconnect resumes the same identity instead. The channel owner joins
// its authenticated host binding to this CAS. Existing slots and claims are never
// deleted here: replacing an executor is not proof its effects stopped.
func PrepareRegistration(previous ExecutorSnapshot, requested ExecutorRecord) (Change, ExecutorRecord, error) {
	if previous.Revision < 0 || requested.Identity.Generation != 0 {
		return Change{}, ExecutorRecord{}, invalid("executor registration requires a server-assigned generation")
	}
	next := requested
	next.Operations = append([]workexecution.Operation(nil), requested.Operations...)
	next.Identity.Generation = 1
	if previous.Revision > 0 {
		if err := previous.Record.Validate(); err != nil {
			return Change{}, ExecutorRecord{}, err
		}
		old := previous.Record.Identity
		if old.Executor == workexecution.Agent && previous.Record.ProcessGeneration == requested.ProcessGeneration {
			return Change{}, ExecutorRecord{}, invalid(
				"reconnecting Agent must resume its registered executor instead of creating a generation",
			)
		}
		if old.Executor != next.Identity.Executor || old.HostID != next.Identity.HostID ||
			old.Generation == math.MaxUint64 {
			return Change{}, ExecutorRecord{}, invalid(
				"executor registration does not match its host or generation is exhausted",
			)
		}
		next.Identity.Generation = old.Generation + 1
	}
	// Reconciliation must explicitly enable dispatch after a new registration.
	next.Ready = false
	change, err := executorChange(previous.Revision, next)
	return change, next, err
}

// PrepareAvailability requires the exact registered process identity, preventing
// an old disconnect callback from disabling its replacement. Capacity changes
// retain all occupied slots, including slots beyond a newly reduced limit.
func PrepareAvailability(
	previous ExecutorSnapshot,
	identity workexecution.Identity,
	ready bool,
	capacity uint16,
	unavailable []workexecution.Operation,
) (Change, error) {
	if previous.Revision <= 0 || previous.Record.Identity != identity {
		return Change{}, errs.New(errs.KindStateConflict, "executor registration has changed")
	}
	next := previous.Record
	next.Ready, next.Capacity = ready, capacity
	next.UnavailableOperations = slices.Clone(unavailable)
	return executorChange(previous.Revision, next)
}

func executorChange(revision int64, next ExecutorRecord) (Change, error) {
	value, err := EncodeExecutor(next)
	if err != nil {
		return Change{}, err
	}
	key := ExecutorKey(next.Identity)
	return Change{
		Conditions: []keyvalue.Condition{{Key: key, ModRevision: revision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}},
	}, nil
}
