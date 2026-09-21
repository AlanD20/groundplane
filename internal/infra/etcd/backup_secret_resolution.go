package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"time"
)

// BackupSecretValueEvidence is an encrypted Secret value selected by the
// project-first/platform-fallback lookup. Ciphertext is caller-owned and must
// be cleared after the controller has opened it.
type BackupSecretValueEvidence struct {
	Name      backupsecret.CredentialName
	Reference string
	Value     secretrecord.EncryptedValue
}

// BackupSecretResolutionEvidence contains only the durable proof and
// encrypted values required by the Controller resolver. No plaintext,
// Connector object locator, or age identity is returned.
type BackupSecretResolutionEvidence struct {
	ReadRevision        int64
	Task                TaskRecord
	Assignment          taskassignments.TaskAssignmentRecord
	Environment         hierarchyrecord.EnvironmentRecord
	EnvironmentRevision int64
	Project             hierarchyrecord.ProjectRecord
	ProjectRevision     int64
	Connector           connectorrecord.Record
	ConnectorRevision   int64
	Run                 *backupruntime.BackupRunRecord
	Dispatch            *backupruntime.BackupRecoveryPointPruneDispatchRecord
	DispatchRevision    int64
	Point               *backupruntime.BackupRecoveryPointRecord
	Prune               *backupruntime.BackupRecoveryPointPruneRecord
	Source              backuppolicy.BackupSourceRecord
	SourceRevision      int64
	Credentials         connectorrecord.EncryptedCredentials
	HasCredentials      bool
	SecretValues        []BackupSecretValueEvidence
}

// Clear releases every encrypted buffer returned by the reader. It is safe
// to call more than once and is intentionally explicit for callers that take
// ownership before returning an error.
func (e *BackupSecretResolutionEvidence) Clear() {
	if e == nil {
		return
	}
	clear(e.Credentials.Ciphertext)
	e.Credentials.Ciphertext = nil
	for index := range e.SecretValues {
		clear(e.SecretValues[index].Value.Ciphertext)
		e.SecretValues[index].Value.Ciphertext = nil
	}
	e.SecretValues = nil
}

// BackupSecretResolutionReader reads the exact durable evidence needed by a
// backup capture or prune secret delivery. It is deliberately narrower than
// the runtime repositories so channel composition can depend on one read
// contract without acquiring lifecycle authority.
type BackupSecretResolutionReader struct {
	store backupSecretResolutionStore
	now   func() time.Time
}

type backupSecretResolutionStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewBackupSecretResolutionReader(store backupSecretResolutionStore) (*BackupSecretResolutionReader, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "backup secret resolution store is required")
	}
	return &BackupSecretResolutionReader{store: store, now: time.Now}, nil
}

func (reader *BackupSecretResolutionReader) ResolveBackupSecretEvidence(
	ctx context.Context,
	request backupsecret.Request,
) (BackupSecretResolutionEvidence, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	if err := validateBackupSecretResolutionRequest(request); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	plan, err := executionplan.Validate(request.Plan)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	stepIndex, err := backupSecretStepIndex(plan, request.StepID)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}

	anchor, err := reader.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(request.TaskID), taskjournal.TaskAssignmentIndexKey(request.TaskID),
	}})
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != 2 || anchor.Values[1] == nil {
		etcdstore.ClearValues(anchorValues(anchor))
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment evidence is unavailable",
		)
	}
	assignmentForKey, err := taskassignments.DecodeTaskAssignment(anchor.Values[1].Value)
	etcdstore.ClearValues(anchor.Values)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task assignment evidence is corrupt",
		)
	}
	fixedRevision := anchor.ReadRevision

	baseKeys := []string{
		taskjournal.TaskStorageKey(request.TaskID),
		taskjournal.TaskAssignmentIndexKey(request.TaskID),
		taskjournal.TaskExecutionClaimKey(taskjournal.TaskExecutorAgent, request.AgentID, request.TaskID),
		taskjournal.TaskTimeoutIndexKey(request.TaskID, assignmentForKey.Deadline),
		backupruntime.BackupRunKey(request.TaskID),
		backupruntime.BackupRecoveryPointPruneDispatchKey(request.TaskID),
	}
	base, err := reader.readFixed(ctx, baseKeys, fixedRevision)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	defer etcdstore.ClearValues(base.Values)
	if base.Values[0] == nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindTaskNotFound,
			"backup task was not found",
		)
	}
	if base.Values[1] == nil || base.Values[2] == nil || base.Values[3] == nil ||
		base.Values[1].ModRevision != base.Values[2].ModRevision ||
		base.Values[1].ModRevision != base.Values[3].ModRevision ||
		!bytes.Equal(base.Values[1].Value, base.Values[2].Value) ||
		!bytes.Equal(base.Values[1].Value, base.Values[3].Value) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment evidence changed",
		)
	}
	task, err := decodeTaskRecord(base.Values[0].Value)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task evidence is corrupt",
		)
	}
	assignment, err := taskassignments.DecodeTaskAssignment(base.Values[1].Value)
	if err != nil {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindInternal,
			"backup task assignment evidence is corrupt",
		)
	}
	if err := validateBackupSecretTaskAssignment(task, assignment, request); err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	now := reader.now
	if now == nil {
		now = time.Now
	}
	if !now().Before(assignment.Deadline) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task assignment deadline has expired",
		)
	}
	if len(task.Params) != 0 || len(task.Materializations) != 0 {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task contains mutable execution inputs",
		)
	}
	if task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) ||
		task.RenderGeneration != int32(plan.RenderGeneration) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task plan identity changed",
		)
	}
	if !proto.Equal(plan.Steps[stepIndex], request.Step) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task step evidence changed",
		)
	}
	for index, taskStep := range task.Steps {
		if index >= len(plan.Steps) || taskStep.ID != plan.Steps[index].StepId {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup task step evidence changed",
			)
		}
	}
	if len(task.Steps) != len(plan.Steps) {
		return BackupSecretResolutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup task step evidence changed",
		)
	}

	evidence := BackupSecretResolutionEvidence{
		ReadRevision: fixedRevision, Task: task, Assignment: assignment,
	}
	if task.Type == taskjournal.TaskBackup {
		if base.Values[4] == nil || base.Values[5] != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup run evidence is unavailable",
			)
		}
		run, decodeErr := backupruntime.DecodeBackupRunRecord(base.Values[4].Value)
		if decodeErr != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindInternal,
				"backup run evidence is corrupt",
			)
		}
		if err := validateBackupRunTaskBinding(task, run); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		if run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup run is no longer active",
			)
		}
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup task plan operation changed",
			)
		}
		if err := validateBackupRunExecutionPlan(run, plan); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		evidence.Run = &run
	} else {
		if base.Values[4] != nil || base.Values[5] == nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup prune dispatch evidence is unavailable",
			)
		}
		dispatch, decodeErr := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(base.Values[5].Value)
		if decodeErr != nil {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindInternal,
				"backup prune dispatch evidence is corrupt",
			)
		}
		if err := validateBackupPruneTaskBinding(task, dispatch); err != nil {
			return BackupSecretResolutionEvidence{}, err
		}
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
			return BackupSecretResolutionEvidence{}, errs.New(
				errs.KindStateConflict,
				"backup prune task plan operation changed",
			)
		}
		evidence.Dispatch = &dispatch
		evidence.DispatchRevision = base.Values[5].ModRevision
	}

	dynamic := newBackupSecretDynamicRead()
	environmentID, connectorIDs, sourceIDs, err := reader.planDynamicKeys(
		dynamic, evidence, plan, stepIndex,
	)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	firstDynamic, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		return BackupSecretResolutionEvidence{}, err
	}
	defer etcdstore.ClearValues(firstDynamic.Values)
	selectedConnectorID := ""
	if evidence.Run != nil {
		selectedConnectorID = evidence.Run.ConnectorID
	} else if evidence.Dispatch != nil {
		selectedConnectorID = plan.Steps[stepIndex].GetBackupArtifactPrune().ConnectorId
	}
	if err := reader.decodeCommonDynamicEvidence(
		firstDynamic, dynamic, &evidence, environmentID, connectorIDs, selectedConnectorID,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	if err := reader.decodeSourceDynamicEvidence(
		firstDynamic, dynamic, &evidence, plan, stepIndex, sourceIDs,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	if evidence.Dispatch != nil {
		if err := reader.decodePruneDynamicEvidence(
			ctx, firstDynamic, dynamic, &evidence, plan, stepIndex,
		); err != nil {
			evidence.Clear()
			return BackupSecretResolutionEvidence{}, err
		}
	}

	secondDynamic, err := reader.readFixed(ctx, dynamic.keys, fixedRevision)
	if err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	defer etcdstore.ClearValues(secondDynamic.Values)
	if err := reader.resolveEncryptedCredentialValues(
		ctx, fixedRevision, dynamic, &evidence, secondDynamic,
	); err != nil {
		evidence.Clear()
		return BackupSecretResolutionEvidence{}, err
	}
	return evidence, nil
}
