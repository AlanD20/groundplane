package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseHookExecutionPublication struct {
	Sources          scriptsourcequeries.ScriptExecutionSources
	Execution        scriptexecutions.ScriptExecutionRecord
	SnapshotRevision int64
}

type releaseHookPublicationFragment struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func prepareReleaseHookPublicationFragment(
	evidence ReleasePublicationEvidence,
) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	updatedScripts := make(map[string]scriptrecord.Record)
	scriptRevisions := make(map[string]int64)
	conditionKeys := make(map[string]struct{})
	appendRevision := func(key string, revision int64) {
		if _, exists := conditionKeys[key]; exists {
			return
		}
		conditionKeys[key] = struct{}{}
		fragment.conditions = append(fragment.conditions, etcdstore.Condition{Key: key, ModRevision: revision})
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
			scriptexecutions.ValidateScriptExecutionRecord(
				execution,
			) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != evidence.Task.ID ||
			execution.OperationID != evidence.Task.OperationID || execution.PlanHash != evidence.Task.PlanHash ||
			evidence.Task.Params[releaserender.ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID ||
			sources.Revision != evidence.Manifest.ReadRevision {
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed,
				"release hook execution evidence is invalid",
			)
		}
		if _, exists := updatedScripts[execution.ScriptID]; exists {
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed,
				"release hook Script selection is duplicated",
			)
		}
		if sources.Script.Record.ActiveReferences == math.MaxUint64 {
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindStateConflict,
				"Script active reference count is exhausted",
			)
		}
		updated := sources.Script.Record
		updated.ActiveReferences++
		updatedScripts[execution.ScriptID] = updated
		scriptRevisions[execution.ScriptID] = sources.Script.Revision

		executionValue, err := recordcodec.Encode("script-execution", execution)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		snapshotValue, err := recordcodec.Encode("script-runner-snapshot", struct {
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
		bodyReference, err := recordcodec.Encode("script-body-reference", struct {
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
		fragment.conditions = append(
			fragment.conditions,
			etcdstore.Condition{Key: scriptexecutions.ScriptExecutionKey(execution.ID)},
		)
		appendRevision(scriptrecord.ScriptSetActiveKey(execution.EnvironmentID), sources.ScriptSet.Revision)
		appendRevision(scriptrecord.ScriptSetBodyGenerationKey(
			execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID, execution.ScriptGeneration,
		), sources.BodyGeneration.Revision)
		fragment.conditions = append(fragment.conditions, servicerecord.ServiceDesiredCondition(sources.Service))
		for _, condition := range scriptExecutionProjectionConditions(sources) {
			appendRevision(condition.Key, condition.ModRevision)
		}
		fragment.mutations = append(
			fragment.mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptexecutions.ScriptExecutionKey(execution.ID),
				Value: executionValue,
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID),
				Value: snapshotValue,
			},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptexecutions.ScriptSetBodyForwardReferenceKey(
				execution.EnvironmentID,
				execution.ScriptSetGeneration,
				execution.ScriptID,
				execution.ScriptGeneration,
				execution.ID,
			), Value: bodyReference},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptexecutions.ScriptBodyReverseReferenceKey(execution.ID),
				Value: append([]byte(nil), bodyReference...),
			},
		)
	}
	for scriptID, updated := range updatedScripts {
		value, err := scriptrecord.EncodeRecord(updated)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		appendRevision(scriptrecord.ScriptSetScriptKey(
			updated.EnvironmentID, updated.ScriptSetGeneration, scriptID,
		), scriptRevisions[scriptID])
		fragment.mutations = append(
			fragment.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetScriptKey(
				updated.EnvironmentID, updated.ScriptSetGeneration, scriptID,
			), Value: value},
		)
	}
	return fragment, nil
}

func clearReleaseHookPublicationFragment(fragment releaseHookPublicationFragment) {
	etcdstore.ZeroMutationBytes(fragment.mutations)
}
