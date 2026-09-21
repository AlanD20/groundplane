package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type taskReportStage uint8

const (
	taskReportRejected taskReportStage = iota
	taskReportApplicationAttempted
)

func (s *Server) acknowledge(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	acknowledgement *agentpb.TaskAck,
) (taskReportStage, error) {
	if acknowledgement == nil ||
		ids.Validate(ids.KindAssignment, acknowledgement.AssignmentId) != nil ||
		len(acknowledgement.PlanHash) != 32 || acknowledgement.GetExecutionEpoch() == 0 {
		return taskReportRejected, errs.New(errs.KindValidationFailed, "Agent Task acknowledgement is invalid")
	}
	task, err := s.tasks.GetTask(ctx, acknowledgement.TaskId)
	if err != nil {
		return taskReportRejected, err
	}
	if assignments, ok := s.tasks.(interface {
		GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	}); ok {
		assignment, assignmentErr := assignments.GetTaskAssignment(ctx, acknowledgement.GetTaskId())
		terminalReplay := assignmentErr != nil && task.Record.Result != nil && task.Record.TerminalAssignment != nil
		if assignmentErr != nil && !terminalReplay {
			return taskReportRejected, assignmentErr
		}
		if !terminalReplay && assignment.Assignment.Record.AssignmentID != acknowledgement.GetAssignmentId() {
			return taskReportRejected, errs.New(
				errs.KindStateConflict,
				"Agent Task acknowledgement execution epoch does not match",
			)
		}
		if terminalReplay {
			if task.Record.Result.ExecutionEpoch != acknowledgement.GetExecutionEpoch() ||
				task.Record.Result.ReleaseRecoveryRecordSHA256 != hex.EncodeToString(
					acknowledgement.GetReleaseRecoveryRecordSha256(),
				) {
				return taskReportRejected, errs.New(
					errs.KindStateConflict,
					"Agent terminal acknowledgement replay authority changed",
				)
			}
		} else {
			recoveryDigest, decodeErr := hex.DecodeString(assignment.Assignment.Record.ReleaseRecoveryRecordSHA256)
			oldPrimaryReplay := assignment.Assignment.Record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly &&
				acknowledgement.GetExecutionEpoch() < assignment.Assignment.Record.ExecutionEpoch &&
				len(acknowledgement.GetReleaseRecoveryRecordSha256()) == 0
			if !oldPrimaryReplay && assignment.Assignment.Record.ExecutionEpoch != acknowledgement.GetExecutionEpoch() {
				return taskReportRejected, errs.New(
					errs.KindStateConflict,
					"Agent Task acknowledgement execution epoch does not match",
				)
			}
			if !oldPrimaryReplay && (decodeErr != nil || !bytes.Equal(recoveryDigest, acknowledgement.GetReleaseRecoveryRecordSha256())) {
				return taskReportRejected, errs.New(
					errs.KindStateConflict,
					"Agent Task acknowledgement recovery digest does not match",
				)
			}
		}
	}
	environmentTarget := ids.Validate(ids.KindEnvironment, task.Record.Target) == nil
	environmentCreation := task.Record.Type == taskjournal.TaskCreate && environmentTarget
	if s.plans == nil {
		return taskReportRejected, errs.New(errs.KindInternal, "Agent Task plan resolver is not configured")
	}
	plan, err := s.plans.ResolveExecutionPlan(ctx, task.Record)
	if err != nil {
		return taskReportRejected, err
	}
	environmentDirectory := executionplan.UsesEnvironmentDirectoryResult(plan)
	if environmentDirectory {
		if err := validateEnvironmentDirectoryTaskResult(acknowledgement); err != nil {
			return taskReportRejected, err
		}
	} else if err := validateComposeTaskResult(acknowledgement); err != nil {
		return taskReportRejected, err
	}
	if err := validateDNSResolverResultShape(acknowledgement, plan); err != nil {
		return taskReportRejected, err
	}
	planHash, err := hex.DecodeString(task.Record.PlanHash)
	if err != nil || !bytes.Equal(planHash, acknowledgement.PlanHash) {
		return taskReportRejected, errs.New(
			errs.KindStateConflict,
			"Agent Task acknowledgement plan hash does not match",
		)
	}
	var terminal taskjournal.TaskStatus
	switch acknowledgement.Terminal {
	case agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED:
		terminal = taskjournal.TaskStatusCompleted
	case agentpb.TaskTerminal_TASK_TERMINAL_FAILED:
		terminal = taskjournal.TaskStatusFailed
	case agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT:
		terminal = taskjournal.TaskStatusTimedOut
	case agentpb.TaskTerminal_TASK_TERMINAL_ABORTED:
		terminal = taskjournal.TaskStatusAborted
	default:
		return taskReportRejected, errs.New(
			errs.KindValidationFailed,
			"Agent Task acknowledgement terminal state is invalid",
		)
	}
	if environmentCreation {
		store, ok := s.tasks.(environmentCreationTaskStore)
		if !ok {
			return taskReportRejected, errs.New(
				errs.KindInternal,
				"Environment creation Task store is not configured",
			)
		}
		_, err = store.AcknowledgeEnvironmentCreation(
			ctx,
			agentID,
			agentGeneration,
			acknowledgement.TaskId,
			acknowledgement.AssignmentId,
			task.Record.Target,
			terminal,
			durableEnvironmentDirectoryTaskResult(acknowledgement),
			s.now().UTC(),
		)
	} else {
		result := durableComposeTaskResult(acknowledgement)
		result.ExecutionEpoch = acknowledgement.GetExecutionEpoch()
		result.ReleaseRecoveryRecordSHA256 = hex.EncodeToString(acknowledgement.GetReleaseRecoveryRecordSha256())
		if environmentDirectory {
			result = durableEnvironmentDirectoryTaskResult(acknowledgement)
		}
		_, err = s.tasks.AcknowledgeTask(
			ctx,
			agentID,
			agentGeneration,
			acknowledgement.TaskId,
			acknowledgement.AssignmentId,
			terminal,
			result,
			s.now().UTC(),
		)
	}
	return taskReportApplicationAttempted, err
}

func quarantineTaskReportConflict(
	stage taskReportStage,
	err error,
	acknowledgement *agentpb.TaskAck,
	delivered map[string]string,
	quarantined map[string]string,
) bool {
	kind, ok := errs.KindOf(err)
	if stage != taskReportApplicationAttempted || !ok || kind != errs.KindStateConflict || acknowledgement == nil ||
		delivered[acknowledgement.GetTaskId()] != acknowledgement.GetAssignmentId() {
		return false
	}
	delete(delivered, acknowledgement.GetTaskId())
	quarantined[acknowledgement.GetTaskId()] = acknowledgement.GetAssignmentId()
	return true
}

func validComposeTaskDiagnostic(diagnostic agentpb.ComposeHelperDiagnostic) bool {
	switch diagnostic {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED:
		return true
	default:
		return false
	}
}

func validateDNSResolverResultShape(acknowledgement *agentpb.TaskAck, plan *agentpb.ExecutionPlan) error {
	result := acknowledgement.GetComposeResult()
	if result == nil {
		return nil
	}
	candidate := result.GetDnsResolverCandidateObservation()
	rollback := result.GetDnsResolverRollbackObservation()
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if candidate != nil || rollback != nil {
			return errs.New(errs.KindValidationFailed, "non-Component Task returned DNS resolver observation evidence")
		}
		return nil
	}
	managed, observation, candidateService, rollbackService, planErr := dnsResolverProofPlan(plan)
	mode := plan.GetComponentLifecycleMode()
	switch mode {
	case agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE:
		if planErr != nil {
			return planErr
		}
	case agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE:
		rollbackAction := plan.GetComponentRollbackObservation()
		if candidate != nil || rollbackAction == nil || len(plan.GetArtifacts()) != 1 ||
			len(plan.GetArtifacts()[0].GetServices()) != 1 {
			return errs.New(errs.KindValidationFailed, "disabled Component Task proof plan is invalid")
		}
		if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED {
			if rollback != nil || result.GetReconciliationRequired() {
				return errs.New(errs.KindValidationFailed, "completed Component disable returned rollback evidence")
			}
			return nil
		}
		if result.GetReconciliationRequired() {
			return errs.New(errs.KindValidationFailed, "Component disable compensation is not proven")
		}
		failedStepID := result.GetFailedStepId()
		mutationAttempted := false
		if failedStepID != "" {
			for _, step := range plan.GetSteps() {
				if step.GetStepId() == failedStepID {
					mutationAttempted = true
					break
				}
			}
			if !mutationAttempted {
				return errs.New(errs.KindValidationFailed, "Component disable failed step is not in its sealed plan")
			}
		}
		if mutationAttempted && rollback == nil {
			return errs.New(errs.KindValidationFailed, "Component disable is missing rollback serving evidence")
		}
		if !mutationAttempted && rollback != nil {
			return errs.New(errs.KindValidationFailed, "pre-mutation Component disable returned rollback evidence")
		}
		if rollback != nil && !dnsResolverProofMatches(
			rollback, rollbackAction, plan.GetArtifacts()[0].GetServices()[0], rollbackAction.GetArtifactDigest(),
		) {
			return errs.New(
				errs.KindValidationFailed,
				"Component disable rollback observation does not match its sealed plan",
			)
		}
		return nil
	default:
		return errs.New(errs.KindValidationFailed, "Component Task has an invalid lifecycle mode")
	}
	if candidate != nil && !dnsResolverProofMatches(
		candidate,
		observation,
		candidateService,
		observation.GetArtifactDigest(),
	) {
		return errs.New(
			errs.KindValidationFailed,
			"Component Task candidate observation does not match its sealed plan",
		)
	}
	if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED {
		if rollback != nil {
			return errs.New(
				errs.KindValidationFailed,
				"completed Component Task returned rollback observation evidence",
			)
		}
		if candidate == nil {
			return errs.New(
				errs.KindValidationFailed,
				"completed Component Task is missing candidate observation evidence",
			)
		}
		return nil
	}
	if result.GetReconciliationRequired() {
		return errs.New(
			errs.KindValidationFailed,
			"Component Task compensation is not proven",
		)
	}
	failurePosition, err := managedConfigFailurePosition(plan, result.GetFailedStepId())
	if err != nil {
		return err
	}
	if failurePosition < 0 && (candidate != nil || rollback != nil) {
		return errs.New(
			errs.KindValidationFailed,
			"pre-mutation Component Task returned DNS resolver observation evidence",
		)
	}
	requiresRollback := mode != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE &&
		failurePosition > 0 &&
		len(managed.GetExpectedPreviousArtifactDigest()) == sha256.Size
	if requiresRollback && rollback == nil {
		return errs.New(
			errs.KindValidationFailed,
			"compensated Component Task is missing rollback observation evidence",
		)
	}
	if len(managed.GetExpectedPreviousArtifactDigest()) == 0 && rollback != nil {
		return errs.New(
			errs.KindValidationFailed,
			"Component Task returned rollback evidence without an applied candidate",
		)
	}
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE && rollback != nil {
		return errs.New(errs.KindValidationFailed, "failed Component enable returned serving rollback evidence")
	}
	rollbackAction := proto.CloneOf(observation)
	rollbackAction.ArtifactId = managed.GetExpectedPreviousArtifactId()
	rollbackAction.ArtifactDigest = append([]byte(nil), managed.GetExpectedPreviousArtifactDigest()...)
	rollbackAction.Generation = managed.GetExpectedPreviousGeneration()
	if rollback != nil && !dnsResolverProofMatches(
		rollback,
		rollbackAction,
		rollbackService,
		managed.GetExpectedPreviousArtifactDigest(),
	) {
		return errs.New(errs.KindValidationFailed, "Component Task rollback observation does not match its sealed plan")
	}
	return nil
}

func dnsResolverProofPlan(
	plan *agentpb.ExecutionPlan,
) (*agentpb.ComponentApply, *agentpb.ComponentApply, *agentpb.ComposeService, *agentpb.ComposeService, error) {
	var managed *agentpb.ComponentApply
	var observation *agentpb.ComponentApply
	for _, step := range plan.GetSteps() {
		action := step.GetComponentApply()
		if action == nil {
			continue
		}
		if action.GetManagedConfigContent() {
			if managed != nil {
				return nil, nil, nil, nil, errs.New(
					errs.KindValidationFailed,
					"Component Task has ambiguous managed-config action",
				)
			}
			managed = action
		} else {
			if observation != nil {
				return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "Component Task has ambiguous observation action")
			}
			observation = action
		}
	}
	if managed == nil || observation == nil || len(plan.GetArtifacts()) < 1 || len(plan.GetArtifacts()) > 2 {
		return nil, nil, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Component Task DNS resolver proof plan is invalid",
		)
	}
	candidateArtifact := componentObservationComposeArtifact(plan)
	if candidateArtifact == nil || len(candidateArtifact.GetServices()) != 1 {
		return nil, nil, nil, nil, errs.New(errs.KindValidationFailed, "Component Task candidate artifact is invalid")
	}
	rollbackService := candidateArtifact.GetServices()[0]
	if rollbackArtifact := componentRollbackComposeArtifact(plan, candidateArtifact.GetArtifactId()); rollbackArtifact != nil {
		if len(rollbackArtifact.GetServices()) != 1 {
			return nil, nil, nil, nil, errs.New(
				errs.KindValidationFailed,
				"Component Task rollback artifact is invalid",
			)
		}
		rollbackService = rollbackArtifact.GetServices()[0]
	}
	return managed, observation, candidateArtifact.GetServices()[0], rollbackService, nil
}

func componentObservationComposeArtifact(plan *agentpb.ExecutionPlan) *agentpb.ComposeArtifact {
	if len(plan.GetArtifacts()) == 1 {
		return plan.GetArtifacts()[0]
	}
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			for _, artifact := range plan.GetArtifacts() {
				if artifact.GetArtifactId() == apply.GetArtifactId() {
					return artifact
				}
			}
		}
	}
	return nil
}

func componentRollbackComposeArtifact(
	plan *agentpb.ExecutionPlan,
	candidateArtifactID string,
) *agentpb.ComposeArtifact {
	if len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() != candidateArtifactID {
			return artifact
		}
	}
	return nil
}

func managedConfigFailurePosition(plan *agentpb.ExecutionPlan, failedStepID string) (int, error) {
	managedIndex := -1
	failedIndex := -1
	for index, step := range plan.GetSteps() {
		if step.GetComponentApply().GetManagedConfigContent() {
			managedIndex = index
		}
		if step.GetStepId() == failedStepID {
			failedIndex = index
		}
	}
	if failedStepID == "" {
		return -1, nil
	}
	if managedIndex < 0 || failedIndex < 0 {
		return 0, errs.New(errs.KindValidationFailed, "Component Task failed step does not match its sealed plan")
	}
	switch {
	case failedIndex < managedIndex:
		return -1, nil
	case failedIndex > managedIndex:
		return 1, nil
	default:
		return 0, nil
	}
}

func dnsResolverProofMatches(
	evidence *agentpb.DNSResolverObservationEvidence,
	action *agentpb.ComponentApply,
	service *agentpb.ComposeService,
	digest []byte,
) bool {
	return evidence.GetComponentId() == action.GetComponentId() &&
		evidence.GetServiceId() == service.GetServiceId() &&
		evidence.GetArtifactId() == action.GetArtifactId() &&
		bytes.Equal(evidence.GetArtifactSha256(), digest) &&
		evidence.GetRenderGeneration() == action.GetGeneration() &&
		evidence.GetImageReference() == service.GetImageReference() &&
		evidence.GetImageRepository() == service.GetImageRepository() &&
		bytes.Equal(evidence.GetImageIndexDigest(), service.GetImageIndexDigest()) &&
		bytes.Equal(evidence.GetImageConfigDigest(), service.GetImageConfigDigest()) &&
		bytes.Equal(evidence.GetVerifiedImageDigest(), service.GetImageChildDigest()) &&
		evidence.GetImageOs() == service.GetImageOs() &&
		evidence.GetImageArchitecture() == service.GetImageArchitecture() &&
		evidence.GetImageVariant() == service.GetImageVariant()
}
