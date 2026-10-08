package composeruntime

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ObserveDatabaseConsumer authenticates a read-only Compose observation against
// the sealed Backup Service fact before a database Restore consumer transition.
func (runtime *Runtime) ObserveDatabaseConsumer(ctx context.Context,
	assignment taskassignment.Assignment, artifactID, serviceID string,
) (*agentpb.ObservedProject, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if runtime == nil || runtime.observer == nil || assignment.Plan == nil ||
		assignment.Plan.BackupScope == nil || artifactID == "" || serviceID == "" {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer authority is unavailable")
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range assignment.Plan.BackupScope.Services {
		if candidate.ServiceId == serviceID {
			if fact != nil {
				return nil, zero, errs.New(errs.KindStateConflict, "database consumer fact is ambiguous")
			}
			fact = candidate
		}
	}
	artifact := taskassignment.ComposeArtifact(assignment.Plan, artifactID)
	if fact == nil || artifact == nil {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer artifact is unavailable")
	}
	var service *agentpb.ComposeService
	for _, candidate := range artifact.Services {
		if candidate.ServiceId == serviceID && candidate.ComposeName == fact.CurrentName {
			if service != nil {
				return nil, zero, errs.New(errs.KindStateConflict, "database consumer is ambiguous")
			}
			service = candidate
		}
	}
	if service == nil || uint32(len(service.ExpectedLabels)) != fact.RequiredLabelCount {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer labels are unavailable")
	}
	labels, err := backupservicefact.LabelsDigest(service.ExpectedLabels)
	if err != nil || !bytes.Equal(labels, fact.RequiredLabelsSha256) {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer labels changed")
	}
	observed, err := runtime.observer.Observe(ctx, assignment.Plan, artifactID)
	if err != nil || observed == nil || len(observed.Collisions) != 0 {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer observation is unavailable")
	}
	observed = backupConsumerObservation(observed, artifact, serviceID)
	count := uint32(0)
	for _, container := range observed.Containers {
		if container.GetServiceId() != serviceID {
			continue
		}
		if !labelPairsEqual(container.Labels, service.ExpectedLabels) ||
			!sameBackupVolumeImageID(container.ImageId, fact.LocalImageIdSha256) {
			return nil, zero, errs.New(errs.KindStateConflict, "database consumer identity changed")
		}
		count++
	}
	if count != 0 && count != service.ExpectedReplicas ||
		fact.PriorRuntimeIntent.GetKind() == agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT &&
			count != 0 {
		return nil, zero, errs.New(errs.KindStateConflict, "database consumer count changed")
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(observed)
	if err != nil {
		return nil, zero, errs.Wrap(errs.KindInternal, err)
	}
	return observed, sha256.Sum256(encoded), nil
}

type DatabaseConsumerOperation uint8

const (
	DatabaseConsumerStop DatabaseConsumerOperation = iota + 1
	DatabaseConsumerRecover
)

func (runtime *Runtime) ExecuteDatabaseConsumer(ctx context.Context,
	assignment taskassignment.Assignment, step *agentpb.ExecutionStep,
	artifactID, serviceID string, operation DatabaseConsumerOperation,
) (StepResult, [sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	restore := step.GetBackupStep().GetRestore()
	if runtime == nil || runtime.helper == nil ||
		restore.GetPostgres() == nil && restore.GetMysql() == nil ||
		operation != DatabaseConsumerStop && operation != DatabaseConsumerRecover {
		return StepResult{}, zero, errs.New(errs.KindStateConflict, "database consumer action is unavailable")
	}
	if _, _, err := runtime.ObserveDatabaseConsumer(ctx, assignment, artifactID, serviceID); err != nil {
		return StepResult{}, zero, err
	}
	request := &agentpb.ComposeHelperRequest{Schema: composeHelperSchema,
		AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
		OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.StepId,
		TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.TimeoutSeconds),
	}
	if restore.GetPostgres() != nil {
		value := agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_STOP
		if operation == DatabaseConsumerRecover {
			value = agentpb.BackupPostgresConsumerOperation_BACKUP_POSTGRES_CONSUMER_OPERATION_RECOVER
		}
		request.BackupPostgresConsumer = &agentpb.BackupPostgresConsumerAction{ServiceId: serviceID, Operation: value}
	} else {
		value := agentpb.BackupMySQLConsumerOperation_BACKUP_MYSQL_CONSUMER_OPERATION_STOP
		if operation == DatabaseConsumerRecover {
			value = agentpb.BackupMySQLConsumerOperation_BACKUP_MYSQL_CONSUMER_OPERATION_RECOVER
		}
		request.BackupMysqlConsumer = &agentpb.BackupMySQLConsumerAction{ServiceId: serviceID, Operation: value}
	}
	result := StepResult{MutationAttempted: true}
	response, helperErr := runtime.helper.Execute(ctx, request)
	observed, digest, observeErr := runtime.ObserveDatabaseConsumer(ctx, assignment, artifactID, serviceID)
	result.Observed = observed
	if observeErr == nil {
		if operation == DatabaseConsumerStop &&
			postgresConsumerRunning(observed, serviceID) ||
			operation == DatabaseConsumerRecover &&
				!postgresConsumerAllRunning(observed, serviceID) {
			observeErr = errs.New(errs.KindStateConflict, "database consumer state did not converge")
		}
	}
	if helperErr != nil || observeErr != nil || response == nil || response.Schema != composeHelperSchema ||
		response.Outcome != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.ExitCode != 0 || response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, digest, errs.New(errs.KindStateConflict, "database consumer action outcome is unknown")
	}
	return result, digest, nil
}

func postgresConsumerRunning(observed *agentpb.ObservedProject, serviceID string) bool {
	for _, container := range observed.GetContainers() {
		if container.ServiceId == serviceID &&
			container.State == agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return true
		}
	}
	return false
}

func postgresConsumerAllRunning(observed *agentpb.ObservedProject, serviceID string) bool {
	count := 0
	for _, container := range observed.GetContainers() {
		if container.ServiceId != serviceID {
			continue
		}
		count++
		if container.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING {
			return false
		}
	}
	return count != 0
}
