package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func blueprintScriptTaskShape(task TaskRecord) bool {
	return task.Type == taskjournal.TaskUpdate && task.Executor == taskjournal.TaskExecutorAgent &&
		releases.ValidatePublicationID(task.Params[releaserender.TaskReleasePublicationParam]) == nil &&
		task.Owner.EnvironmentID != "" && task.Target == task.Owner.EnvironmentID &&
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] == task.Owner.EnvironmentID &&
		ids.Validate(ids.KindTask, task.Params[blueprints.EnvironmentDesiredRevisionParam]) == nil
}

func (repository *ScriptRepository) validateBlueprintScriptExecutionAuthority(
	ctx context.Context,
	task TaskRecord,
	execution scriptexecutions.ScriptExecutionRecord,
	revision int64,
) error {
	_, err := repository.blueprintScriptExecutionAuthority(ctx, task, execution, revision)
	return err
}

func (repository *ScriptRepository) blueprintScriptExecutionAuthority(
	ctx context.Context,
	task TaskRecord,
	execution scriptexecutions.ScriptExecutionRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	if repository == nil || repository.store == nil || !blueprintScriptTaskShape(task) ||
		execution.CurrentTaskID != task.ID || execution.OperationID != task.OperationID ||
		execution.EnvironmentID != task.Owner.EnvironmentID ||
		execution.PlanHash != task.PlanHash ||
		task.Params[releaserender.ReleaseHookStepExecutionParam(execution.StepID)] != execution.ID {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script execution does not match its Task")
	}
	publicationID := task.Params[releaserender.TaskReleasePublicationParam]
	keys := []string{releases.ReleasePublicationKey(publicationID), releases.ReleaseManifestStagingKey(publicationID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script publication authority is unavailable")
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
		read.Values[0].Value,
		"release-publication",
	)
	if err != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script publication authority is corrupt")
	}
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
		read.Values[1].Value,
		"release-staged-manifest",
	)
	if err != nil || validateBlueprintCandidateManifest(task, marker, manifest) != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script manifest authority is corrupt")
	}
	var selected releases.ReleaseStagedMemberRef
	memberBound := false
	for _, member := range manifest.Members {
		if member.ReleaseID == execution.ReleaseID && member.ServiceID == execution.ServiceID {
			selected = member
			memberBound = true
			break
		}
	}
	if !memberBound {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script execution is outside the candidate manifest")
	}
	intentKey := releases.ReleaseIntentStagingKey(publicationID, selected.ReleaseID)
	intentRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{intentKey}, Revision: revision},
	)
	if err != nil {
		return nil, err
	}
	if intentRead == nil || intentRead.ReadRevision != revision ||
		len(intentRead.Values) != 1 || intentRead.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script candidate Intent is unavailable")
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](intentRead.Values[0].Value, "release-intent")
	intentDigest, _ := domain.Digest(intent)
	if err != nil || domain.ValidateIntent(intent) != nil || intentDigest != selected.IntentDigest ||
		intent.ID != execution.ReleaseID || intent.ServiceID != execution.ServiceID ||
		intent.EnvironmentID != execution.EnvironmentID ||
		intent.OperationID != task.OperationID || intent.OperationKind != domain.OperationBlueprintApply {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Script candidate Intent changed")
	}
	return []etcdstore.Condition{
		{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: intentKey, ModRevision: intentRead.Values[0].ModRevision},
	}, nil
}

func validateScriptBodyReference(value []byte, execution scriptexecutions.ScriptExecutionRecord) error {
	reference, err := recordcodec.Decode[struct {
		ExecutionID         string `json:"script_execution_id"`
		ScriptID            string `json:"script_id"`
		Generation          uint64 `json:"generation"`
		ScriptSetGeneration string `json:"script_set_generation"`
	}](value, "script-body-reference")
	if err != nil || reference.ExecutionID != execution.ID || reference.ScriptID != execution.ScriptID ||
		reference.Generation != execution.ScriptGeneration ||
		reference.ScriptSetGeneration != execution.ScriptSetGeneration {
		return releases.CorruptReleaseRecord()
	}
	return nil
}

func decrementStoredScriptActiveReferences(value []byte, scriptID string) ([]byte, error) {
	stored, err := recordcodec.Decode[scriptrecord.StoredRecord](value, "script")
	if err != nil || stored.Desired.ID != scriptID || stored.ActiveReferences == 0 {
		return nil, releases.CorruptReleaseRecord()
	}
	stored.ActiveReferences--
	encoded, err := recordcodec.Encode("script", stored)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
