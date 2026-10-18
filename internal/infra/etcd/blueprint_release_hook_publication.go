package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type PreparedReleaseWorkHooks struct {
	key               string
	revision          int64
	snapshotRevisions map[string]int64
	stage             blueprintReleaseHookStage
}

func blueprintReleaseHookStageKey(workItemID string) string {
	return "/v1/preparations/blueprint-release-hooks/" + workItemID
}

type blueprintReleaseHookStage struct {
	Schema      int
	OperationID string
	TaskID      string
	WorkItemID  string
	Input       workexecution.Input
	Digest      string
	Count       int
	Snapshots   []blueprintHookSnapshot
}

type blueprintHookSnapshot struct {
	ExecutionID string
	SnapshotID  string
	SHA256      string
}

func (ledger *ReleaseLedger) PrepareReleaseWorkHooks(
	ctx context.Context,
	work workqueue.View,
	evidence ReleaseHookWorkPublication,
) (PreparedReleaseWorkHooks, error) {
	if ctx == nil || ledger == nil || ledger.store == nil {
		return PreparedReleaseWorkHooks{}, errs.New(errs.KindInternal, "Blueprint hook storage is unavailable")
	}
	if work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.Task.Task.ID != evidence.Task.ID ||
		work.Item.Item.ID != evidence.Item.ID || work.Item.Item.TaskID != evidence.Task.ID ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil || work.Task.Task.CollectionStartedAt != nil ||
		work.Item.Item.Phase == workexecution.Settled || work.Item.Item.AttemptNumber != 0 {
		return PreparedReleaseWorkHooks{}, errs.New(
			errs.KindStateConflict,
			"Blueprint hook preparation no longer owns unstarted work",
		)
	}
	for _, hook := range evidence.Hooks {
		if hook.Execution.SourceMembershipCount != 0 || hook.Execution.SourceMembershipSHA256 != "" ||
			hook.SnapshotRevision != 0 {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint hook preparation requires unpublished source inputs",
			)
		}
	}
	fragment, err := prepareReleaseWorkHookPublicationFragment(evidence)
	if err != nil {
		return PreparedReleaseWorkHooks{}, err
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(evidence.Hooks) == 0 {
		return PreparedReleaseWorkHooks{}, nil
	}
	hash := sha256.New()
	for _, mutation := range fragment.mutations {
		hash.Write([]byte(mutation.Key))
		hash.Write([]byte{0})
		hash.Write(mutation.Value)
	}
	stage := blueprintReleaseHookStage{
		Schema:      1,
		OperationID: evidence.OperationID,
		TaskID:      evidence.Task.ID,
		WorkItemID:  evidence.Item.ID,
		Input:       *evidence.Item.Input,
		Digest:      hex.EncodeToString(hash.Sum(nil)),
		Count:       len(evidence.Hooks),
	}
	for index, hook := range evidence.Hooks {
		digest := sha256.Sum256(fragment.mutations[index*2+1].Value)
		stage.Snapshots = append(stage.Snapshots, blueprintHookSnapshot{
			ExecutionID: hook.Execution.ID, SnapshotID: hook.Execution.SnapshotID, SHA256: hex.EncodeToString(digest[:]),
		})
	}
	value, err := json.Marshal(stage)
	if err != nil {
		return PreparedReleaseWorkHooks{}, errs.Wrap(errs.KindInternal, err)
	}
	key := blueprintReleaseHookStageKey(evidence.Item.ID)
	read, err := ledger.store.Get(ctx, key)
	if err != nil {
		return PreparedReleaseWorkHooks{}, err
	}
	var revision int64
	if read == nil || read.Entry == nil {
		result, txErr := ledger.store.Transact(
			ctx,
			[]etcdstore.Condition{{Key: key},
				{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
				{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision}},
			[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		)
		if txErr != nil {
			return PreparedReleaseWorkHooks{}, txErr
		}
		if result.Succeeded {
			revision = result.Revision
		} else {
			read, err = ledger.store.Get(ctx, key)
			if err != nil {
				return PreparedReleaseWorkHooks{}, err
			}
		}
	}
	if revision == 0 {
		if read == nil || read.Entry == nil || !bytes.Equal(read.Entry.Value, value) {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation is occupied",
			)
		}
		revision = read.Entry.ModRevision
	}
	prepared := PreparedReleaseWorkHooks{
		key:               key,
		revision:          revision,
		snapshotRevisions: make(map[string]int64, len(evidence.Hooks)),
		stage:             stage,
	}
	// Only immutable snapshots are staged. Executions become visible together
	// with the native work input and retained source root in final publication.
	for i := 1; i < len(fragment.mutations); i += 2 {
		snapshot := fragment.mutations[i]
		loaded, loadErr := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{snapshot.Key}})
		if loadErr != nil {
			return PreparedReleaseWorkHooks{}, loadErr
		}
		if loaded == nil || len(loaded.Values) != 1 {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindInternal,
				"Blueprint hook preparation evidence is incomplete",
			)
		}
		if loaded.Values[0] == nil {
			result, txErr := ledger.store.Transact(
				ctx,
				[]etcdstore.Condition{{Key: key, ModRevision: revision}, {Key: snapshot.Key},
					{Key: workqueue.TaskKey(work.Task.Task.ID), ModRevision: work.Task.Revision},
					{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision}},
				[]etcdstore.Mutation{snapshot},
			)
			if txErr != nil {
				return PreparedReleaseWorkHooks{}, txErr
			}
			if !result.Succeeded {
				loaded, loadErr = ledger.store.GetMany(
					ctx,
					etcdstore.GetManyRequest{Keys: []string{snapshot.Key}},
				)
				if loadErr != nil {
					return PreparedReleaseWorkHooks{}, loadErr
				}
			} else {
				prepared.snapshotRevisions[evidence.Hooks[i/2].Execution.SnapshotID] = result.Revision
				continue
			}
		}
		if loaded == nil || len(loaded.Values) != 1 || loaded.Values[0] == nil ||
			loaded.Values[0].Key != snapshot.Key || loaded.Values[0].ModRevision <= 0 ||
			!bytes.Equal(loaded.Values[0].Value, snapshot.Value) {
			return PreparedReleaseWorkHooks{}, errs.New(
				errs.KindStateConflict,
				"Blueprint hook preparation changed",
			)
		}
		prepared.snapshotRevisions[evidence.Hooks[i/2].Execution.SnapshotID] = loaded.Values[0].ModRevision
	}
	return prepared, nil
}

func (prepared PreparedReleaseWorkHooks) SnapshotRevision(id string) int64 {
	return prepared.snapshotRevisions[id]
}

type releaseHookSourceMember struct {
	Evidence scriptsourceevidence.ScriptSourceEvidence
}

func (member releaseHookSourceMember) withReference(
	reference sourceref.Reference,
) scriptsourceevidence.ScriptSourcePreparationMember {
	reference.SourceModRevision = 0
	return scriptsourceevidence.ScriptSourcePreparationMember{
		Reference: reference,
		Evidence:  member.Evidence,
	}
}

func releaseHookSourceEvidence(
	authority *agentpb.ScriptSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (releaseHookSourceMember, error) {
	if authority == nil || authority.Existing != nil || authority.Staged == nil {
		return releaseHookSourceMember{}, errs.New(
			errs.KindValidationFailed,
			"Release hook staged source requires its captured candidate authority",
		)
	}
	return releaseHookStagedSourceEvidence(
		authority.Staged,
		key,
		value,
		ownerID,
	)
}

func releaseHookStagedSourceEvidence(
	authority *agentpb.ScriptStagedSourceAuthority,
	key string,
	value []byte,
	ownerID string,
) (releaseHookSourceMember, error) {
	digest := sha256.Sum256(value)
	if authority == nil || authority.EnvironmentId != ownerID ||
		authority.FixedReadRevision > uint64(^uint64(0)>>1) ||
		!bytes.Equal(authority.CanonicalValueSha256, digest[:]) {
		return releaseHookSourceMember{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Script staged source digest changed",
		)
	}
	var canonical [sha256.Size]byte
	copy(canonical[:], authority.CanonicalValueSha256)
	return releaseHookSourceMember{Evidence: scriptsourceevidence.ScriptSourceEvidence{
		Staged: &scriptsourceevidence.ScriptStagedSourceEvidence{
			SourceKey: key,
			Stage: scriptsourceevidence.ScriptCandidateSourceStage{
				EnvironmentID:        authority.EnvironmentId,
				RevisionID:           authority.RevisionId,
				RenderGeneration:     authority.RenderGeneration,
				FixedReadRevision:    int64(authority.FixedReadRevision),
				CanonicalValueSHA256: canonical,
			},
			Value: append([]byte(nil), value...),
		},
	}}, nil
}

func prepareReleaseWorkHookPublicationFragment(
	evidence ReleaseHookWorkPublication,
) (releaseHookPublicationFragment, error) {
	fragment := releaseHookPublicationFragment{}
	if evidence.Task.Validate() != nil || evidence.Item.Validate() != nil ||
		!releaserender.IsReleaseAction(evidence.Task.Action) || evidence.Item.Operation != workexecution.Release ||
		evidence.Item.Placement.Executor != workexecution.Agent || len(evidence.Hooks) > 16 ||
		evidence.Item.TaskID != evidence.Task.ID || evidence.Item.Input == nil ||
		evidence.Item.AttemptNumber != 0 || evidence.Task.Stop != nil || evidence.Item.ForwardRevoked ||
		evidence.Task.FinishedAt != nil || evidence.ReadRevision <= 0 ||
		ids.Validate(ids.KindOperation, evidence.OperationID) != nil {
		return fragment, errs.New(errs.KindValidationFailed, "Blueprint hooks require unstarted native work")
	}
	executionIDs := make(map[string]bool, len(evidence.Hooks))
	snapshotIDs := make(map[string]bool, len(evidence.Hooks))
	for _, hook := range evidence.Hooks {
		execution := hook.Execution
		if scriptsourcequeries.ValidateStoredScriptContext(hook.Sources, execution) != nil ||
			scriptexecutions.ValidateScriptExecutionRecord(
				execution,
			) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
			!execution.ActiveReference || execution.CurrentTaskID != evidence.Task.ID || execution.OperationID != evidence.OperationID ||
			execution.WorkItemID != evidence.Item.ID || execution.WorkInput == nil || *execution.WorkInput != *evidence.Item.Input ||
			execution.ServiceID != evidence.Item.TargetID || execution.EnvironmentID != evidence.Task.Scope.EnvironmentID ||
			hook.Sources.Project.Record.ID != evidence.Task.Scope.ProjectID ||
			hook.Sources.Project.Record.TenantID != evidence.Task.Scope.TenantID ||
			executionIDs[execution.ID] || snapshotIDs[execution.SnapshotID] ||
			execution.ScriptSetGeneration != hook.Sources.Script.Record.ScriptSetGeneration ||
			hook.Sources.Revision != evidence.ReadRevision {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint release hook execution evidence is invalid",
			)
		}
		executionIDs[execution.ID] = true
		snapshotIDs[execution.SnapshotID] = true
		executionValue, err := recordcodec.Encode("script-execution", execution)
		if err != nil {
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		snapshotValue, err := encodeBlueprintReleaseHookSnapshot(execution)
		if err != nil {
			clear(executionValue)
			clearReleaseHookPublicationFragment(fragment)
			return releaseHookPublicationFragment{}, err
		}
		fragment.conditions = append(fragment.conditions,
			etcdstore.Condition{Key: scriptexecutions.ScriptExecutionKey(execution.ID)},
			etcdstore.Condition{Key: scriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID)},
		)
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
		)
	}
	return fragment, nil
}

func encodeBlueprintReleaseHookSnapshot(execution scriptexecutions.ScriptExecutionRecord) ([]byte, error) {
	return recordcodec.Encode("script-runner-snapshot", scriptsourceevidence.StoredScriptRunnerSnapshot{
		ExecutionID: execution.ID, SnapshotID: execution.SnapshotID,
		SHA256: execution.SnapshotSHA256, Payload: execution.Snapshot,
	})
}

func cloneBlueprintReleaseMutations(values []etcdstore.Mutation) []etcdstore.Mutation {
	result := make([]etcdstore.Mutation, len(values))
	for index, value := range values {
		result[index] = value
		result[index].Value = append([]byte(nil), value.Value...)
	}
	return result
}

func cloneScriptSourcePreparationMembers(
	values []scriptsourceevidence.ScriptSourcePreparationMember,
) []scriptsourceevidence.ScriptSourcePreparationMember {
	result := make([]scriptsourceevidence.ScriptSourcePreparationMember, len(values))
	for index, value := range values {
		result[index] = value
		if value.Evidence.Existing != nil {
			existing := *value.Evidence.Existing
			result[index].Evidence.Existing = &existing
		}
		if value.Evidence.Staged != nil {
			staged := *value.Evidence.Staged
			staged.Value = append([]byte(nil), staged.Value...)
			result[index].Evidence.Staged = &staged
		}
	}
	return result
}

func clearScriptSourcePreparationMembers(values []scriptsourceevidence.ScriptSourcePreparationMember) {
	for index := range values {
		if values[index].Evidence.Staged != nil {
			clear(values[index].Evidence.Staged.Value)
		}
	}
}

var _ = domain.OperationBlueprintApply
