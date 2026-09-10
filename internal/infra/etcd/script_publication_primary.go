package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// preparedScriptPrimary fences authored metadata after source preparation has
// incremented body references. Only that bookkeeping may differ from capture.
func preparedScriptPrimary(
	ctx context.Context,
	store hierarchyStore,
	sources ScriptExecutionSources,
) (Condition, error) {
	expected := sources.Script.Record
	key := scriptSetScriptKey(expected.EnvironmentID, expected.ScriptSetGeneration, expected.Desired.ID)
	read, err := store.Get(ctx, key)
	if err != nil {
		return Condition{}, err
	}
	if read == nil || read.Entry == nil {
		return Condition{}, errs.New(errs.KindStateConflict, "Script disappeared during source preparation")
	}
	defer clear(read.Entry.Value)
	current, err := decodeScriptRecord(read.Entry.Value)
	if err != nil {
		return Condition{}, err
	}
	expected.ActiveReferences = current.ActiveReferences
	// Blueprint's fixed-read primary is bodyless. The immutable body is needed
	// only for write-boundary validation; encodeScriptRecord never stores it.
	expected.Desired.Body = sources.BodyGeneration.Record.Body
	value, err := encodeScriptRecord(expected)
	if err != nil {
		return Condition{}, err
	}
	defer clear(value)
	if current.ActiveReferences == 0 || !bytes.Equal(value, read.Entry.Value) {
		return Condition{}, errs.New(errs.KindStateConflict, "Script changed during source preparation")
	}
	return Condition{Key: key, ModRevision: read.Entry.ModRevision}, nil
}

func (ledger *ReleaseLedger) blueprintScriptPrimaryConditions(
	ctx context.Context, hooks []ReleaseHookExecutionPublication,
) ([]Condition, error) {
	conditions := make([]Condition, 0, len(hooks))
	byKey := make(map[string]Condition, len(hooks))
	for _, hook := range hooks {
		script, execution := hook.Sources.Script.Record, hook.Execution
		if script.Desired.ID != execution.ScriptID || script.EnvironmentID != execution.EnvironmentID ||
			script.ServiceID != execution.ServiceID || script.ActiveGeneration != execution.ScriptGeneration ||
			script.ScriptSetGeneration == "" ||
			(execution.ScriptSetGeneration != "" && script.ScriptSetGeneration != execution.ScriptSetGeneration) {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint Script primary differs from its execution")
		}
		if err := validateStoredScriptContext(hook.Sources, execution); err != nil {
			return nil, err
		}
		condition, err := preparedScriptPrimary(ctx, ledger.store, hook.Sources)
		if err != nil {
			return nil, err
		}
		if previous, found := byKey[condition.Key]; found {
			if previous != condition {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Script primary revisions disagree")
			}
			continue
		}
		byKey[condition.Key] = condition
		conditions = append(conditions, condition)
	}
	return conditions, nil
}
