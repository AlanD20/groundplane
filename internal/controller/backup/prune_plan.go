package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// BackupPrunePlanInput contains one fixed publication snapshot. Scope binds
// the exact Project/Environment record bytes. ExecutionIDs are allocated once
// by publication and retained with the sealed plan, never generated here.
type BackupPrunePlanInput struct {
	Task         etcd.TaskRecord
	Scope        *agentpb.BackupPlanScope
	Dispatch     backupruntime.BackupRecoveryPointPruneDispatchRecord
	Evidence     []backupplanning.PruneExecutionEvidence
	ExecutionIDs []string
}

// BuildBackupPrunePlan is pure: it preserves the retained dispatch order and
// emits one exact immutable object per step from real durable evidence.
func BuildBackupPrunePlan(input BackupPrunePlanInput) (*agentpb.ExecutionPlan, error) {
	task := input.Task
	dispatch := input.Dispatch
	if task.Type != taskjournal.TaskBackupPrune || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Actor != taskjournal.TaskActorSystem ||
		(task.Status != taskjournal.TaskStatusPending && task.Status != taskjournal.TaskStatusRunning) ||
		task.RenderGeneration != 0 || task.TimeoutSeconds != int64(executionplan.MaximumBackupPruneStepTimeoutSeconds) ||
		len(task.Params) != 0 || len(task.Materializations) != 0 ||
		ids.Validate(ids.KindTask, task.ID) != nil || ids.Validate(ids.KindOperation, task.OperationID) != nil ||
		ids.Validate(ids.KindPlan, task.PlanID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backup prune Task identity or execution contract is invalid")
	}
	if err := backupruntime.ValidateBackupRecoveryPointPruneDispatchRecord(dispatch); err != nil {
		return nil, err
	}
	if err := etcd.ValidateBackupPruneTaskBinding(task, dispatch); err != nil {
		return nil, err
	}
	if input.Scope == nil || input.Scope.EnvironmentId != dispatch.EnvironmentID ||
		input.Scope.ProjectId != task.Owner.ProjectID ||
		len(input.Evidence) != len(dispatch.RecoveryPointIDs) || len(task.Steps) != len(input.Evidence) ||
		len(input.ExecutionIDs) != len(input.Evidence) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup prune requires its sealed scope, ordered evidence and execution identities",
		)
	}
	if err := taskjournal.ValidateTaskSteps(task.Steps); err != nil {
		return nil, err
	}
	deadline := dispatch.CreatedAt.Add(time.Duration(executionplan.MaximumBackupPruneStepTimeoutSeconds) * time.Second)
	if deadline.UnixNano() <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "backup prune absolute deadline is invalid")
	}
	plan := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: task.PlanID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: dispatch.EnvironmentID,
		BackupScope: proto.CloneOf(input.Scope), Steps: make([]*agentpb.ExecutionStep, 0, len(input.Evidence)),
	}
	for index, evidence := range input.Evidence {
		step := task.Steps[index]
		if step.Kind != taskjournal.TaskStepOperation || evidence.Prune.Point.ID != dispatch.RecoveryPointIDs[index] {
			return nil, errs.New(errs.KindValidationFailed, "backup prune step or retained point order changed")
		}
		object, err := buildBackupPruneObject(evidence, uint32(index+1))
		if err != nil {
			return nil, err
		}
		authority, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
			StepId: step.ID, ExecutionId: input.ExecutionIDs[index], StepDeadlineUnixNano: uint64(deadline.UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
				RetentionPolicy: proto.CloneOf(evidence.RetentionPolicy), Objects: []*agentpb.BackupPruneObject{object},
			}},
		})
		if err != nil {
			return nil, err
		}
		plan.Steps = append(plan.Steps, &agentpb.ExecutionStep{
			StepId: step.ID, TimeoutSeconds: executionplan.MaximumBackupPruneStepTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: authority},
		})
	}
	if err := backupplanning.ValidateBackupPruneExecutionPlan(dispatch, input.Evidence, plan); err != nil {
		return nil, err
	}
	sealed, err := executionplan.Seal(plan)
	if err != nil {
		return nil, err
	}
	if task.PlanHash != "" {
		expected, err := hex.DecodeString(task.PlanHash)
		if err != nil || !bytes.Equal(expected, sealed.PlanHash) {
			return nil, errs.New(errs.KindStateConflict, "backup prune sealed plan changed during reconnect")
		}
	}
	return sealed, nil
}

func buildBackupPruneObject(
	evidence backupplanning.PruneExecutionEvidence,
	ordinal uint32,
) (*agentpb.BackupPruneObject, error) {
	point := evidence.Prune.Point
	if err := backupruntime.ValidateBackupRecoveryPointSnapshot(point); err != nil {
		return nil, err
	}
	policyDigest, err := hex.DecodeString(evidence.Prune.PolicySHA256)
	if err != nil || len(policyDigest) != sha256.Size || evidence.Prune.PolicyRevision <= 0 ||
		evidence.RetentionPolicy == nil || evidence.RetentionPolicy.ModRevision != evidence.Prune.PolicyRevision ||
		!bytes.Equal(evidence.RetentionPolicy.Sha256, policyDigest) || evidence.ConnectorAuthority == nil ||
		evidence.PointRevision != evidence.Prune.PointRevision || evidence.PointRevision <= 0 || len(evidence.PointSHA256) != sha256.Size {
		return nil, errs.New(
			errs.KindValidationFailed,
			"backup prune retained policy or point authority is incomplete or changed",
		)
	}
	sourceDigest, err := hex.DecodeString(point.Evidence.SourceSHA256)
	if err != nil || len(sourceDigest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backup prune source digest is invalid")
	}
	storedDigest, err := hex.DecodeString(point.Evidence.StoredSHA256)
	if err != nil || len(storedDigest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backup prune stored digest is invalid")
	}
	artifact := backupobject.Artifact{
		Key: point.ObjectKey, EnvironmentID: point.EnvironmentID, SourceID: point.SourceID, RecoveryPointID: point.ID,
		SourceFormat: backupobject.SourceFormat(
			point.SourceFormat,
		), Encryption: backupobject.Encryption(point.Encryption),
		Evidence: backupobject.Evidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes,
			StoredSizeBytes: point.Evidence.StoredSizeBytes,
		},
	}
	copy(artifact.Evidence.SourceSHA256[:], sourceDigest)
	copy(artifact.Evidence.StoredSHA256[:], storedDigest)
	if point.Encryption == backupruntime.BackupRuntimeEncryptionAge {
		era := uint64(point.KeyEra)
		artifact.KeyEra = &era
	}
	if err := artifact.Validate(); err != nil {
		return nil, err
	}
	metadataCount, metadataDigest := artifact.MetadataEvidence()
	object := &agentpb.BackupObjectIdentity{
		Connector: proto.CloneOf(evidence.ConnectorAuthority), Bucket: point.Object.Target.ConnectorBucket,
		ObjectKey: point.Object.Target.ObjectKey,
	}
	switch point.Object.Discriminator.Kind {
	case backupobject.DiscriminatorVersionID:
		object.Discriminator = &agentpb.BackupObjectIdentity_VersionId{
			VersionId: &agentpb.BackupS3VersionId{Value: point.Object.Discriminator.Value},
		}
	case backupobject.DiscriminatorETag:
		object.Discriminator = &agentpb.BackupObjectIdentity_Etag{
			Etag: &agentpb.BackupS3ETag{Value: point.Object.Discriminator.Value},
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "backup prune selected immutable discriminator is missing")
	}
	return &agentpb.BackupPruneObject{
		Ordinal: ordinal, PointId: point.ID,
		Point: &agentpb.RevisionDigest{
			ModRevision: evidence.PointRevision,
			Sha256:      append([]byte(nil), evidence.PointSHA256...),
		},
		Evidence: &agentpb.BackupArtifactEvidence{
			SourceSizeBytes: point.Evidence.SourceSizeBytes,
			SourceSha256:    sourceDigest,
			StoredSizeBytes: point.Evidence.StoredSizeBytes,
			StoredSha256:    storedDigest,
		},
		Object: object, MetadataCount: metadataCount, MetadataSha256: append([]byte(nil), metadataDigest[:]...),
	}, nil
}
