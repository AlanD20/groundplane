package etcd

import (
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseHookExecutionPublication struct {
	Sources   ScriptExecutionSources
	Execution ScriptExecutionRecord
}

type releaseHookPublicationFragment struct {
	conditions []Condition
	mutations  []Mutation
}

func prepareReleaseHookPublicationFragment(evidence ReleasePublicationEvidence) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	updatedScripts := make(map[string]ScriptRecord)
	scriptRevisions := make(map[string]int64)
	conditionKeys := make(map[string]struct{})
	appendRevision := func(key string, revision int64) {
		if _, exists := conditionKeys[key]; exists {
			return
		}
		conditionKeys[key] = struct{}{}
		fragment.conditions = append(fragment.conditions, Condition{Key: key, ModRevision: revision})
	}
	for _, hook := range evidence.Hooks {
		sources, execution := hook.Sources, hook.Execution
		generation := sources.Script.Record.ScriptSetGeneration
		if execution.ScriptSetGeneration != "" && execution.ScriptSetGeneration != generation {
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed, "release hook execution evidence is invalid",
			)
		}
		// RunScript plans do not carry Script-set storage ownership. Bind it
		// from the frozen source snapshot at the atomic publication boundary,
		// as manual Script publication does before validating the record.
		execution.ScriptSetGeneration = generation
		if err := validateScriptExecutionSources(sources, execution); err != nil ||
			validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != evidence.Task.ID ||
			execution.OperationID != evidence.Task.OperationID || execution.PlanHash != evidence.Task.PlanHash ||
			evidence.Task.Params[ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID ||
			sources.Revision != evidence.Manifest.ReadRevision {
			return releaseHookPublicationFragment{}, errs.New(errs.KindValidationFailed, "release hook execution evidence is invalid")
		}
		if _, exists := updatedScripts[execution.ScriptID]; exists {
			return releaseHookPublicationFragment{}, errs.New(errs.KindValidationFailed, "release hook Script selection is duplicated")
		}
		if sources.Script.Record.ActiveReferences == math.MaxUint64 {
			return releaseHookPublicationFragment{}, errs.New(errs.KindStateConflict, "Script active reference count is exhausted")
		}
		updated := sources.Script.Record
		updated.ActiveReferences++
		updatedScripts[execution.ScriptID] = updated
		scriptRevisions[execution.ScriptID] = sources.Script.Revision

		executionValue, err := encodeEnvelope("script-execution", execution)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		snapshotValue, err := encodeEnvelope("script-runner-snapshot", struct {
			ExecutionID string `json:"script_execution_id"`
			SnapshotID  string `json:"snapshot_id"`
			SHA256      string `json:"sha256"`
			Payload     []byte `json:"payload"`
		}{execution.ID, execution.SnapshotID, execution.SnapshotSHA256, execution.Snapshot})
		if err != nil {
			clear(executionValue)
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		bodyReference, err := encodeEnvelope("script-body-reference", struct {
			ExecutionID         string `json:"script_execution_id"`
			ScriptID            string `json:"script_id"`
			Generation          uint64 `json:"generation"`
			ScriptSetGeneration string `json:"script_set_generation"`
		}{execution.ID, execution.ScriptID, execution.ScriptGeneration, execution.ScriptSetGeneration})
		if err != nil {
			clear(executionValue)
			clear(snapshotValue)
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		fragment.conditions = append(fragment.conditions, Condition{Key: scriptExecutionKey(execution.ID)})
		appendRevision(scriptSetActiveKey(execution.EnvironmentID), sources.ScriptSet.Revision)
		appendRevision(scriptSetBodyGenerationKey(
			execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration,
		), sources.BodyGeneration.Revision)
		fragment.conditions = append(fragment.conditions, serviceDesiredCondition(sources.Service))
		for _, condition := range scriptExecutionProjectionConditions(sources) {
			appendRevision(condition.Key, condition.ModRevision)
		}
		fragment.mutations = append(fragment.mutations,
			Mutation{Type: MutationPut, Key: scriptExecutionKey(execution.ID), Value: executionValue},
			Mutation{Type: MutationPut, Key: scriptRunnerSnapshotKey(execution.SnapshotID), Value: snapshotValue},
			Mutation{Type: MutationPut, Key: scriptSetBodyForwardReferenceKey(
				execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration, execution.ID,
			), Value: bodyReference},
			Mutation{Type: MutationPut, Key: scriptBodyReverseReferenceKey(execution.ID), Value: append([]byte(nil), bodyReference...)},
		)
	}
	for scriptID, updated := range updatedScripts {
		value, err := encodeScriptRecord(updated)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		appendRevision(scriptSetScriptKey(
			updated.EnvironmentID, updated.ScriptSetGeneration, scriptID,
		), scriptRevisions[scriptID])
		fragment.mutations = append(fragment.mutations, Mutation{Type: MutationPut, Key: scriptSetScriptKey(
			updated.EnvironmentID, updated.ScriptSetGeneration, scriptID,
		), Value: value})
	}
	return fragment, nil
}

func clearReleaseHookPublicationFragment(fragment releaseHookPublicationFragment) {
	clearMutations(fragment.mutations)
}
