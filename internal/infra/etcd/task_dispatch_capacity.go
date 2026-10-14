package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The MVP has one Controller and one Task owner. Claim and operator recovery
// authorization share this gate; their durable assignment roster is reread
// after acquiring it, including after a Controller restart.
func (repository *TaskRepository) lockAgentDispatch(ctx context.Context) (context.Context, func(), error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, nil, err
	}
	if repository.dispatchClaims == nil {
		return nil, nil, errs.New(errs.KindInternal, "agent dispatch coordination is unavailable")
	}
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second)
	select {
	case repository.dispatchClaims <- struct{}{}:
	case <-bounded.Done():
		cancel()
		return nil, nil, bounded.Err()
	}
	return bounded, func() { cancel(); <-repository.dispatchClaims }, nil
}

func (repository *TaskRepository) ensureAgentDispatchCapacity(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) error {
	active, err := repository.ListAgentAssignments(ctx, agentID, generation, maximum)
	if err != nil {
		return err
	}
	if len(active) >= int(maximum) {
		return errs.New(errs.KindResourceInUse, "Agent execution slots are busy; retry after a running Task finishes")
	}
	return nil
}

func (repository *TaskRepository) ensureRecoveryDispatchCapacity(ctx context.Context, current TaskAssignment) error {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{localagents.LocalAgentConfigKey(current.Assignment.Record.AgentID)}},
	)
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	configuration, err := localagents.DecodeLocalAgentConfig(read.Values[0].Value)
	if err != nil || configuration.AgentID != current.Assignment.Record.AgentID ||
		configuration.Generation != current.Assignment.Record.AgentGeneration {
		return taskassignments.CorruptTaskAssignment()
	}
	return repository.ensureAgentDispatchCapacity(
		ctx,
		configuration.AgentID,
		configuration.Generation,
		configuration.Config.MaxConcurrentTasks,
	)
}

// Stopped and expired proof-required recoveries keep ownership, but not an
// execution slot. Read pages at one revision instead of treating the accumulated
// stopped roster as active concurrency.
func (repository *TaskRepository) resumableAgentAssignmentRange(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
) (*keyvalue.RangeResult, error) {
	result := &keyvalue.RangeResult{}
	start := ""
	for {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: taskjournal.TaskAssignmentScopePrefix(
				agentID,
			), StartExclusive: start, Revision: result.ReadRevision, Limit: 64,
		})
		if err != nil {
			keyvalue.ClearRangeValues(result.Values)
			return nil, err
		}
		if page == nil || page.More && len(page.Values) == 0 ||
			result.ReadRevision != 0 && page.ReadRevision != result.ReadRevision {
			keyvalue.ClearRangeValues(result.Values)
			return nil, taskassignments.CorruptTaskAssignment()
		}
		result.ReadRevision = page.ReadRevision
		for index, value := range page.Values {
			start = value.Key
			record, err := taskassignments.DecodeTaskAssignment(value.Value)
			if err != nil || record.AgentID != agentID || record.AgentGeneration != generation {
				keyvalue.ClearRangeValues(page.Values[index:])
				keyvalue.ClearRangeValues(result.Values)
				return nil, taskassignments.CorruptTaskAssignment()
			}
			read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
				taskassignments.RecoveryRetryControlKey(
					record.TaskID,
				), taskjournal.TaskRecoveryProofRequiredKey(record.TaskID),
			}, Revision: result.ReadRevision})
			if err != nil {
				keyvalue.ClearRangeValues(page.Values[index:])
				keyvalue.ClearRangeValues(result.Values)
				return nil, err
			}
			if read == nil || read.ReadRevision != result.ReadRevision || len(read.Values) != 2 {
				keyvalue.ClearRangeValues(page.Values[index:])
				keyvalue.ClearRangeValues(result.Values)
				return nil, taskassignments.CorruptTaskAssignment()
			}
			stopped := false
			if read.Values[0] != nil {
				control, err := taskassignments.DecodeRecoveryRetryControl(read.Values[0].Value)
				if err != nil || control.TaskID != record.TaskID || control.AssignmentID != record.AssignmentID ||
					control.ExecutionEpoch > record.ExecutionEpoch || record.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
					keyvalue.ClearValues(read.Values)
					keyvalue.ClearRangeValues(page.Values[index:])
					keyvalue.ClearRangeValues(result.Values)
					return nil, taskassignments.CorruptTaskAssignment()
				}
				stopped = control.Stopped
			}
			if read.Values[1] != nil && record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly &&
				!record.RecoveryExecutionDeadline.After(time.Now().UTC()) {
				stopped = true
			}
			keyvalue.ClearValues(read.Values)
			if stopped {
				clear(value.Value)
				continue
			}
			result.Values = append(result.Values, value)
			if len(result.Values) > int(maximum) {
				keyvalue.ClearRangeValues(page.Values[index+1:])
				keyvalue.ClearRangeValues(result.Values)
				return nil, errs.New(errs.KindInternal, "active Agent assignments exceed configured concurrency")
			}
		}
		if !page.More {
			return result, nil
		}
	}
}
