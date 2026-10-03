package composeruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ObserveBackupVolumeConsumer checks the live member against the sealed
// first-party Service fact, including every required label and local image ID.
func (runtime *Runtime) ObserveBackupVolumeConsumer(ctx context.Context,
	assignment taskassignment.Assignment, step *agentpb.ExecutionStep, serviceID string,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if runtime == nil || runtime.observer == nil || assignment.Plan == nil ||
		step.GetBackupStep() == nil {
		return nil, zero, invalidBackupVolumeObservation()
	}
	volume := step.GetBackupStep().GetCapture().GetVolume().GetProjection()
	if step.GetBackupStep().GetRestore().GetVolume() != nil {
		volume = step.GetBackupStep().GetRestore().GetVolume().GetProjection()
	}
	if volume == nil {
		return nil, zero, invalidBackupVolumeObservation()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range assignment.Plan.BackupScope.GetServices() {
		if candidate.ServiceId == serviceID {
			fact = candidate
			break
		}
	}
	artifact, service, err := executionplan.BackupVolumeConsumer(assignment.Plan, step.GetBackupStep(), serviceID)
	if err != nil {
		return nil, zero, invalidBackupVolumeObservation()
	}
	if fact == nil || service == nil || uint32(len(service.ExpectedLabels)) != fact.RequiredLabelCount {
		return nil, zero, invalidBackupVolumeObservation()
	}
	labels, err := backupservicefact.LabelsDigest(service.ExpectedLabels)
	if err != nil || !bytes.Equal(labels, fact.RequiredLabelsSha256) {
		return nil, zero, invalidBackupVolumeObservation()
	}
	observed, err := runtime.observer.Observe(ctx, assignment.Plan, artifact.ArtifactId)
	if err != nil || observed == nil || len(observed.Collisions) != 0 {
		return observed, zero, invalidBackupVolumeObservation()
	}
	observed = backupConsumerObservation(
		observed,
		artifact,
		serviceID,
	)
	count := uint32(0)
	for _, container := range observed.Containers {
		if container.GetServiceId() != serviceID {
			continue
		}
		if !labelPairsEqual(container.Labels, service.ExpectedLabels) ||
			!sameBackupVolumeImageID(container.ImageId, fact.LocalImageIdSha256) {
			return observed, zero, invalidBackupVolumeObservation()
		}
		count++
	}
	if count != 0 && count != service.ExpectedReplicas ||
		fact.PriorRuntimeIntent.Kind == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT &&
			count != 0 {
		return observed, zero, invalidBackupVolumeObservation()
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(observed)
	if err != nil {
		return observed, zero, errs.Wrap(errs.KindInternal, err)
	}
	return observed, sha256.Sum256(encoded), nil
}

func (runtime *Runtime) ExecuteBackupVolumeConsumer(ctx context.Context,
	assignment taskassignment.Assignment, step *agentpb.ExecutionStep, serviceID string,
	operation agentpb.BackupVolumeConsumerOperation,
) (StepResult, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if runtime == nil || runtime.helper == nil {
		return StepResult{}, zero, invalidBackupVolumeObservation()
	}
	_, before, err := runtime.ObserveBackupVolumeConsumer(ctx, assignment, step, serviceID)
	if err != nil {
		return StepResult{}, zero, err
	}
	request := &agentpb.ComposeHelperRequest{Schema: composeHelperSchema,
		AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.StepId,
		TimeoutSeconds:       taskassignment.RemainingSeconds(ctx, step.TimeoutSeconds),
		BackupVolumeConsumer: &agentpb.BackupVolumeConsumerAction{ServiceId: serviceID, Operation: operation}}
	result := StepResult{MutationAttempted: true}
	response, helperErr := runtime.helper.Execute(ctx, request)
	observed, after, observeErr := runtime.ObserveBackupVolumeConsumer(ctx, assignment, step, serviceID)
	result.Observed = observed
	if observeErr == nil {
		observeErr = validateBackupVolumeConsumerState(observed, serviceID, operation)
	}
	if helperErr != nil {
		result.ReconciliationRequired = true
		return result, after, helperErr
	}
	if response == nil || response.Schema != composeHelperSchema ||
		response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.ExitCode != 0 || response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, after, errs.New(errs.KindRequestFailed, "Backup Volume consumer action failed")
	}
	if observeErr != nil {
		result.ReconciliationRequired = true
		return result, after, observeErr
	}
	_ = before
	return result, after, nil
}

func validateBackupVolumeConsumerState(observed *agentpb.ObservedProject, serviceID string,
	operation agentpb.BackupVolumeConsumerOperation,
) error {
	count := 0
	for _, container := range observed.GetContainers() {
		if container.GetServiceId() != serviceID {
			continue
		}
		count++
		if operation == agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_STOP &&
			container.State == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
			operation == agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_RECOVER &&
				container.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return invalidBackupVolumeObservation()
		}
	}
	if count == 0 && operation != agentpb.BackupVolumeConsumerOperation_BACKUP_VOLUME_CONSUMER_OPERATION_STOP {
		return invalidBackupVolumeObservation()
	}
	return nil
}

func sameBackupVolumeImageID(observed string, expected []byte) bool {
	if len(expected) != sha256.Size || !strings.HasPrefix(observed, "sha256:") {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(observed, "sha256:"))
	return err == nil && bytes.Equal(decoded, expected)
}

func invalidBackupVolumeObservation() error {
	return errs.New(errs.KindStateConflict, "Backup Volume Service observation differs from sealed authority")
}
