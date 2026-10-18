package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseHookExecutionPublication struct {
	Sources          scriptsourcequeries.ScriptExecutionSources
	Execution        scriptexecutions.ScriptExecutionRecord
	SnapshotRevision int64
}

type ReleaseHookWorkPublication struct {
	Task         workexecution.Task
	Item         workexecution.Item
	OperationID  string
	ReadRevision int64
	Hooks        []ReleaseHookExecutionPublication
}

type releaseHookPublicationFragment struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func prepareReleaseHookPublicationFragment(
	evidence ReleaseHookWorkPublication,
) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	if evidence.Task.Validate() != nil || evidence.Item.Validate() != nil ||
		evidence.Item.TaskID != evidence.Task.ID || evidence.Item.Operation != workexecution.Release ||
		evidence.Item.Placement.Executor != workexecution.Agent || evidence.Item.Input == nil ||
		evidence.Item.AttemptNumber != 0 || evidence.Item.ForwardRevoked || evidence.Task.Stop != nil ||
		evidence.Task.FinishedAt != nil || evidence.ReadRevision <= 0 ||
		ids.Validate(ids.KindOperation, evidence.OperationID) != nil || len(evidence.Hooks) > 16 {
		return fragment, errs.New(errs.KindValidationFailed, "release hook publication requires unstarted native work")
	}
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
		if execution.ScriptSetGeneration != generation {
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed, "release hook execution evidence is invalid",
			)
		}
		if err := validateScriptExecutionSources(sources, execution); err != nil ||
			scriptexecutions.ValidateScriptExecutionRecord(
				execution,
			) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != evidence.Task.ID ||
			execution.OperationID != evidence.OperationID || execution.WorkItemID != evidence.Item.ID ||
			execution.WorkInput == nil || *execution.WorkInput != *evidence.Item.Input ||
			execution.ServiceID != evidence.Item.TargetID ||
			execution.EnvironmentID != evidence.Task.Scope.EnvironmentID ||
			sources.Project.Record.ID != evidence.Task.Scope.ProjectID ||
			sources.Project.Record.TenantID != evidence.Task.Scope.TenantID ||
			execution.SourceMembershipCount == 0 || sources.Revision != evidence.ReadRevision {
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

// PrepareReleaseHookWork returns writes for the native Release publication
// transaction. Its caller joins source-set publication, captured Release input,
// work readiness and all resource comparisons; it must not commit this alone.
func PrepareReleaseHookWork(evidence ReleaseHookWorkPublication) (workqueue.Change, error) {
	fragment, err := prepareReleaseHookPublicationFragment(evidence)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: fragment.conditions, Mutations: fragment.mutations}, nil
}

func clearReleaseHookPublicationFragment(fragment releaseHookPublicationFragment) {
	etcdstore.ZeroMutationBytes(fragment.mutations)
}
