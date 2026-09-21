package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareReleaseHookExecutionRetryTransfer(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (releaseHookExecutionRetryTransfer, error) {
	steps, err := releaseHookExecutionSteps(source)
	if err != nil {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier Task authority is invalid")
	}
	if len(steps) == 0 {
		return releaseHookExecutionRetryTransfer{}, nil
	}
	if retry.PlanID != source.PlanID || retry.PlanHash != source.PlanHash || len(retry.Steps) != len(source.Steps) {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier plan lineage changed")
	}
	for index, step := range source.Steps {
		if retry.Steps[index].ID != step.ID ||
			retry.Params[ReleaseHookStepExecutionParam(step.ID)] != source.Params[ReleaseHookStepExecutionParam(step.ID)] {
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier step lineage changed")
		}
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return releaseHookExecutionRetryTransfer{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(steps) {
		return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier read is incomplete")
	}
	transfer := releaseHookExecutionRetryTransfer{
		conditions: make([]etcdstore.Condition, 0, len(steps)), mutations: make([]etcdstore.Mutation, 0, len(steps)),
	}
	seenExecutions := make(map[string]struct{}, len(steps))
	for index, step := range steps {
		value := read.Values[index]
		if value == nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution state is unknown")
		}
		record, decodeErr := recordcodec.Decode[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution state is unknown")
		}
		if record.ID != step.executionID ||
			record.CurrentTaskID != source.ID || record.OperationID != source.OperationID ||
			record.StepID != step.stepID || record.PlanHash != source.PlanHash || !record.ActiveReference ||
			!retry.CreatedAt.After(record.UpdatedAt) {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier lineage changed")
		}
		if record.State != ScriptExecutionNotStarted || record.StartAuthorized {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script execution may already have started")
		}
		if _, duplicate := seenExecutions[record.ID]; duplicate {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, scriptRetryUnsafe("Script retry barrier lineage changed")
		}
		seenExecutions[record.ID] = struct{}{}
		next := record
		next.CurrentTaskID = retry.ID
		next.AssignmentID = ""
		next.UpdatedAt = retry.CreatedAt.UTC()
		encoded, encodeErr := recordcodec.Encode("script-execution", next)
		if encodeErr != nil {
			transfer.clear()
			return releaseHookExecutionRetryTransfer{}, encodeErr
		}
		transfer.conditions = append(transfer.conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
		transfer.mutations = append(transfer.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: value.Key, Value: encoded})
	}
	return transfer, nil
}

func releaseHookExecutionSteps(task TaskRecord) ([]releaseHookExecutionStep, error) {
	if task.Type != TaskDeploy && task.Type != TaskRollback &&
		(task.Type != TaskUpdate || !blueprintScriptTaskShape(task)) {
		return nil, errs.New(errs.KindValidationFailed, "release hook execution Task is invalid")
	}
	steps := make([]releaseHookExecutionStep, 0)
	seen := make(map[string]struct{})
	for _, step := range task.Steps {
		executionID := task.Params[ReleaseHookStepExecutionParam(step.ID)]
		if executionID == "" {
			continue
		}
		if !validRawScriptExecutionID(executionID) {
			return nil, corruptReleaseRecord()
		}
		if _, duplicate := seen[executionID]; duplicate {
			return nil, corruptReleaseRecord()
		}
		seen[executionID] = struct{}{}
		steps = append(steps, releaseHookExecutionStep{stepID: step.ID, executionID: executionID})
	}
	return steps, nil
}

// releaseScriptEffectEvidenceAtRevision returns only durable checkpoint
// evidence. A host observation is never used to choose recovery mode.
func (repository *TaskRepository) releaseScriptEffectEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	revision int64,
) (bool, []etcdstore.Condition, error) {
	steps, err := releaseHookExecutionSteps(task)
	if err != nil {
		return false, nil, err
	}
	if len(steps) == 0 {
		return false, nil, nil
	}
	keys := make([]string, len(steps))
	for index, step := range steps {
		keys[index] = scriptExecutionKey(step.executionID)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return false, nil, corruptReleaseRecord()
	}
	effect := false
	conditions := make([]etcdstore.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil {
			return false, nil, corruptReleaseRecord()
		}
		record, decodeErr := recordcodec.Decode[ScriptExecutionRecord](value.Value, "script-execution")
		if decodeErr != nil || validateScriptExecutionRecord(record) != nil || record.ID != steps[index].executionID ||
			record.CurrentTaskID != task.ID || record.OperationID != task.OperationID ||
			record.StepID != steps[index].stepID || record.PlanHash != task.PlanHash ||
			record.State == ScriptExecutionNotStarted && record.AssignmentID != "" ||
			record.State != ScriptExecutionNotStarted && record.AssignmentID != assignment.AssignmentID {
			return false, nil, corruptReleaseRecord()
		}
		conditions[index] = etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision}
		effect = effect || record.State != ScriptExecutionNotStarted
	}
	return effect, conditions, nil
}

func scriptRetryUnsafe(detail string) error {
	return errs.New(errs.KindScriptRetryUnsafe, detail)
}
