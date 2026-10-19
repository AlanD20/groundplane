package executionplan

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ValidateBackupWorkInput validates captured resources independently of attempt
// identity. No runtime or desired-state lookup can change this payload on Retry.
func ValidateBackupWorkInput(input *agentpb.BackupWorkInput) error {
	if input == nil || input.Schema != 1 || proto.Size(input) > MaximumPlanBytes {
		return errs.New(errs.KindValidationFailed, "Backup work input is invalid")
	}
	if err := RejectUnknown(input); err != nil {
		return err
	}
	var operation agentpb.PlanOperation
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		operation = agentpb.PlanOperation_PLAN_OPERATION_RESTORE
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE:
		operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	default:
		return errs.New(errs.KindValidationFailed, "Backup work operation is invalid")
	}
	// This resource-only view shares domain validation, not delivery authority.
	scope := &agentpb.BackupPlanScope{ProjectId: input.ProjectId, Project: input.Project,
		EnvironmentId: input.EnvironmentId, Environment: input.Environment, Services: input.Services}
	if err := ValidateBackupInput(BackupInput{Operation: operation, EnvironmentID: input.EnvironmentId,
		Scope: scope, Steps: input.Steps, Artifacts: input.Artifacts}); err != nil {
		return err
	}
	serviceIDs := make([]string, len(input.Services))
	for index, service := range input.Services {
		serviceIDs[index] = service.ServiceId
	}
	for _, step := range input.Steps {
		for _, consumer := range step.ConsumerServiceIds {
			if !slices.Contains(serviceIDs, consumer) {
				return errs.New(errs.KindValidationFailed, "Backup consumer has no captured Service fact")
			}
		}
	}
	return nil
}

func CaptureBackupWorkInput(
	inputID string,
	input *agentpb.BackupWorkInput,
) (*agentpb.BackupWorkInput, []byte, workexecution.Input, error) {
	if err := ValidateBackupWorkInput(input); err != nil {
		return nil, nil, workexecution.Input{}, err
	}
	owned := proto.CloneOf(input)
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	reference := workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(digest[:])}
	if err := reference.Validate(); err != nil {
		clear(raw)
		return nil, nil, workexecution.Input{}, err
	}
	return owned, raw, reference, nil
}

func DecodeBackupWorkInput(raw []byte) (*agentpb.BackupWorkInput, error) {
	if len(raw) == 0 || len(raw) > MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Backup work input size is invalid")
	}
	input := &agentpb.BackupWorkInput{}
	if err := proto.Unmarshal(raw, input); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup work input encoding is invalid")
	}
	if err := ValidateBackupWorkInput(input); err != nil {
		return nil, err
	}
	return input, nil
}

func OpenBackupWorkInput(
	attempt workexecution.Attempt,
	input *agentpb.BackupWorkInput,
) (*agentpb.BackupWorkInput, error) {
	if attempt.Validate() != nil || attempt.Executor.Executor != workexecution.Agent ||
		attempt.Direction != workexecution.Forward || attempt.FinishedAt != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup work attempt is invalid")
	}
	owned, raw, reference, err := CaptureBackupWorkInput(attempt.Input.ID, input)
	defer clear(raw)
	if err != nil {
		return nil, err
	}
	if reference != attempt.Input {
		return nil, errs.New(errs.KindStateConflict, "Backup assignment differs from its captured input")
	}
	for _, step := range owned.Steps {
		if step.StepDeadlineUnixNano < uint64(attempt.Deadline.UnixNano()) {
			return nil, errs.New(errs.KindValidationFailed, "Backup attempt exceeds its captured deadline")
		}
	}
	return owned, nil
}
