package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateBackupPlan(plan *agentpb.ExecutionPlan) error {
	return validateBackupExecutionPlan(plan, agentpb.PlanOperation_PLAN_OPERATION_BACKUP)
}

func validateBackupPrunePlan(plan *agentpb.ExecutionPlan) error {
	return validateBackupExecutionPlan(plan, agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE)
}

func validateBackupExecutionPlan(plan *agentpb.ExecutionPlan, operation agentpb.PlanOperation) error {
	maximum := MaximumBackupSources
	if operation == agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE {
		maximum = MaximumBackupPrunePoints
	}
	if ids.Validate(ids.KindEnvironment, plan.TargetId) != nil || plan.Operation != operation ||
		len(plan.Steps) == 0 || len(plan.Steps) > maximum {
		return errs.New(errs.KindValidationFailed, "backup execution plan shape is invalid")
	}
	if err := validateBackupPlanScope(plan.BackupScope, plan.TargetId); err != nil {
		return err
	}
	steps := make(map[string]struct{}, len(plan.Steps))
	executions := make(map[string]struct{}, len(plan.Steps))
	for _, step := range plan.Steps {
		if step == nil || ids.Validate(ids.KindStep, step.StepId) != nil || step.TimeoutSeconds == 0 ||
			step.GetBackupStep() == nil || step.GetBackupStep().StepId != step.StepId {
			return errs.New(errs.KindValidationFailed, "backup execution step binding is invalid")
		}
		authority := step.GetBackupStep()
		if _, err := ValidateBackupStepAuthority(authority); err != nil {
			return err
		}
		if _, exists := steps[step.StepId]; exists {
			return errs.New(errs.KindValidationFailed, "backup step is duplicated")
		}
		if _, exists := executions[authority.ExecutionId]; exists {
			return errs.New(errs.KindValidationFailed, "backup execution is duplicated")
		}
		steps[step.StepId] = struct{}{}
		executions[authority.ExecutionId] = struct{}{}
		switch operation {
		case agentpb.PlanOperation_PLAN_OPERATION_BACKUP:
			capture := authority.GetCapture()
			if capture == nil ||
				(capture.GetConfig() != nil && capture.GetConfig().EnvironmentId != plan.TargetId) ||
				!backupCheckpointObjectKey(
					capture.GetTarget().GetObjectKey(),
					capture.GetTarget().GetConnector().GetPrefix(),
					capture.PointId,
				) {
				return errs.New(errs.KindValidationFailed, "backup capture does not match its plan")
			}
			if !bytes.HasPrefix(
				[]byte(capture.Target.ObjectKey),
				[]byte(capture.Target.Connector.Prefix+plan.TargetId+"/"),
			) {
				return errs.New(errs.KindValidationFailed, "backup capture belongs to another Environment")
			}
		case agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE:
			if authority.GetPrune() == nil {
				return errs.New(errs.KindValidationFailed, "backup prune does not match its plan")
			}
			for _, object := range authority.GetPrune().Objects {
				if !bytes.HasPrefix(
					[]byte(object.Object.ObjectKey),
					[]byte(object.Object.Connector.Prefix+plan.TargetId+"/"),
				) {
					return errs.New(errs.KindValidationFailed, "backup prune belongs to another Environment")
				}
			}
		case agentpb.PlanOperation_PLAN_OPERATION_RESTORE:
			restore := authority.GetRestore()
			if restore == nil ||
				!bytes.HasPrefix(
					[]byte(restore.SourceObject.ObjectKey),
					[]byte(restore.SourceObject.Connector.Prefix+plan.TargetId+"/"),
				) {
				return errs.New(errs.KindValidationFailed, "backup restore belongs to another Environment")
			}
			if config := restore.GetConfig(); config != nil {
				scope, err := environmentpath.Parse(config.Files.VolumeRoot, config.Files.VolumeDir)
				if err != nil || scope.ProjectID != plan.BackupScope.ProjectId || scope.EnvironmentID != plan.TargetId {
					return errs.New(errs.KindValidationFailed, "Config restore file directory belongs to another scope")
				}
			}
		}
	}
	return validateBackupPlanArtifacts(plan)
}

// SealBackupStepAuthority creates the one owned immutable step input used by
// publication, assignment and checkpoint fences. Digest never includes itself.
func SealBackupStepAuthority(step *agentpb.BackupStepAuthority) (*agentpb.BackupStepAuthority, error) {
	if step == nil || len(step.StepDigest) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "backup step must be unhashed before sealing")
	}
	owned := proto.Clone(step).(*agentpb.BackupStepAuthority)
	if err := validateBackupStepAuthorityShape(owned); err != nil {
		return nil, err
	}
	digest, err := backupStepAuthorityDigest(owned)
	if err != nil {
		return nil, err
	}
	owned.StepDigest = digest
	return owned, nil
}

func ValidateBackupStepAuthority(step *agentpb.BackupStepAuthority) (*agentpb.BackupStepAuthority, error) {
	if step == nil || len(step.StepDigest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "backup step digest is invalid")
	}
	owned := proto.Clone(step).(*agentpb.BackupStepAuthority)
	if err := validateBackupStepAuthorityShape(owned); err != nil {
		return nil, err
	}
	digest, err := backupStepAuthorityDigest(owned)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(digest, owned.StepDigest) {
		return nil, errs.New(errs.KindValidationFailed, "backup step digest changed")
	}
	return owned, nil
}

func backupStepAuthorityDigest(step *agentpb.BackupStepAuthority) ([]byte, error) {
	owned := proto.Clone(step).(*agentpb.BackupStepAuthority)
	owned.StepDigest = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.backup.step.schema-one.v1\x00"))
	_, _ = hash.Write(encoded)
	return hash.Sum(nil), nil
}

func validateBackupStepAuthorityShape(step *agentpb.BackupStepAuthority) error {
	if err := RejectUnknown(step); err != nil {
		return err
	}
	if ids.Validate(ids.KindStep, step.StepId) != nil || !validRawULID(step.ExecutionId) ||
		step.StepDeadlineUnixNano == 0 {
		return errs.New(errs.KindValidationFailed, "backup step identity or deadline is invalid")
	}
	seen := make(map[string]struct{}, len(step.ConsumerServiceIds))
	for _, serviceID := range step.ConsumerServiceIds {
		if ids.Validate(ids.KindService, serviceID) != nil {
			return errs.New(errs.KindValidationFailed, "backup consumer identity is invalid")
		}
		if _, exists := seen[serviceID]; exists {
			return errs.New(errs.KindValidationFailed, "backup consumer is duplicated")
		}
		seen[serviceID] = struct{}{}
	}
	valid := false
	switch operation := step.Operation.(type) {
	case *agentpb.BackupStepAuthority_Capture:
		valid = operation != nil && validBackupCaptureAuthority(operation.Capture)
	case *agentpb.BackupStepAuthority_Prune:
		valid = operation != nil && validBackupPruneAuthority(operation.Prune) && len(step.ConsumerServiceIds) == 0
	case *agentpb.BackupStepAuthority_Restore:
		valid = operation != nil && validBackupRestoreAuthority(operation.Restore)
	}
	if !valid {
		return errs.New(errs.KindValidationFailed, "backup step operation authority is invalid")
	}
	return nil
}
