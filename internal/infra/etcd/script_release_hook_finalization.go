package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

type releaseHookExecutionStep struct {
	stepID      string
	executionID string
}

type releaseHookExecutionRetryTransfer struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func (transfer *releaseHookExecutionRetryTransfer) clear() {
	if transfer == nil {
		return
	}
	etcdstore.ZeroMutationBytes(transfer.mutations)
	transfer.conditions = nil
	transfer.mutations = nil
}

// finalizeReleaseHookExecutionBatch releases up to eight hook execution
// references after a non-recoverable parent release terminal result. A skipped
// hook retains its not-started checkpoint as audit evidence but no longer pins
// its Script generation. A run hook is released only after cleanup is proven.
func (repository *TaskRepository) finalizeReleaseHookExecutionBatch(
	ctx context.Context,
	task TaskRecord,
	terminalAt time.Time,
	revision int64,
) (bool, error) {
	steps, err := releaseHookExecutionSteps(task)
	if err != nil || len(steps) == 0 {
		return false, err
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptexecutions.ScriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return false, releases.CorruptReleaseRecord()
	}
	type activeHook struct {
		step   releaseHookExecutionStep
		record scriptexecutions.ScriptExecutionRecord
		value  *etcdstore.KeyValue
	}
	active := make([]activeHook, 0, scriptexecutions.MaximumReleaseHookTerminalBatch)
	seenScripts := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			return false, releases.CorruptReleaseRecord()
		}
		record, decodeErr := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || scriptexecutions.ValidateScriptExecutionRecord(record) != nil || record.ID != step.executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != step.stepID ||
			record.PlanHash != task.PlanHash {
			return false, releases.CorruptReleaseRecord()
		}
		if _, duplicate := seenScripts[record.ScriptID]; duplicate {
			return false, releases.CorruptReleaseRecord()
		}
		seenScripts[record.ScriptID] = struct{}{}
		if !record.ActiveReference {
			continue
		}
		if record.State != scriptexecutions.ScriptExecutionNotStarted &&
			record.State != scriptexecutions.ScriptExecutionCleanupProven {
			return false, errs.New(
				errs.KindStateConflict,
				"release hook execution has not reached a releasable checkpoint",
			)
		}
		if !terminalAt.After(record.UpdatedAt) {
			return false, errs.New(errs.KindStateConflict, "release hook terminal timestamp is not monotonic")
		}
		if len(active) < scriptexecutions.MaximumReleaseHookTerminalBatch {
			active = append(active, activeHook{step: step, record: record, value: value})
		}
	}
	if len(active) == 0 {
		return false, nil
	}
	detailKeys := make([]string, 0, len(active)*3)
	for _, hook := range active {
		detailKeys = append(
			detailKeys,
			scriptrecord.ScriptSetScriptKey(
				hook.record.EnvironmentID,
				hook.record.ScriptSetGeneration,
				hook.record.ScriptID,
			),
			scriptexecutions.ScriptSetBodyForwardReferenceKey(
				hook.record.EnvironmentID,
				hook.record.ScriptSetGeneration,
				hook.record.ScriptID,
				hook.record.ScriptGeneration,
				hook.record.ID,
			),
			scriptexecutions.ScriptBodyReverseReferenceKey(hook.record.ID),
		)
	}
	details, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: detailKeys, Revision: revision})
	if err != nil {
		return false, err
	}
	if details == nil || details.ReadRevision != revision || len(details.Values) != len(detailKeys) {
		return false, releases.CorruptReleaseRecord()
	}
	conditions := make([]etcdstore.Condition, 0, len(active)*4)
	mutations := make([]etcdstore.Mutation, 0, len(active)*4)
	defer etcdstore.ZeroMutationBytes(mutations)
	for index, hook := range active {
		values := details.Values[index*3 : index*3+3]
		if values[0] == nil || values[1] == nil || values[2] == nil {
			return false, releases.CorruptReleaseRecord()
		}
		script, decodeErr := scriptrecord.DecodeRecord(values[0].Value)
		if decodeErr != nil || script.Desired.ID != hook.record.ScriptID ||
			script.EnvironmentID != hook.record.EnvironmentID ||
			script.ScriptSetGeneration != hook.record.ScriptSetGeneration || script.ActiveReferences == 0 {
			return false, releases.CorruptReleaseRecord()
		}
		if err := validateScriptBodyReference(values[1].Value, hook.record); err != nil ||
			validateScriptBodyReference(values[2].Value, hook.record) != nil {
			return false, releases.CorruptReleaseRecord()
		}
		next := hook.record
		next.ActiveReference = false
		next.UpdatedAt = terminalAt.UTC()
		if scriptexecutions.ValidateScriptExecutionRecord(next) != nil {
			return false, releases.CorruptReleaseRecord()
		}
		executionValue, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			return false, encodeErr
		}
		scriptValue, encodeErr := decrementStoredScriptActiveReferences(values[0].Value, hook.record.ScriptID)
		if encodeErr != nil {
			clear(executionValue)
			return false, encodeErr
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: hook.value.Key, ModRevision: hook.value.ModRevision},
			etcdstore.Condition{Key: values[0].Key, ModRevision: values[0].ModRevision},
			etcdstore.Condition{Key: values[1].Key, ModRevision: values[1].ModRevision},
			etcdstore.Condition{Key: values[2].Key, ModRevision: values[2].ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hook.value.Key, Value: executionValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: values[0].Key, Value: scriptValue},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: values[1].Key},
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: values[2].Key},
		)
	}
	if len(conditions)+len(mutations) > etcdstore.MaximumOperations {
		return false, errs.New(errs.KindInternal, "release hook terminal batch exceeds the transaction ceiling")
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "release hook terminal evidence changed")
	}
	return true, nil
}
