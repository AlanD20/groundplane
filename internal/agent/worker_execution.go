package agent

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"time"
)

func (p *WorkerPool) execute(runCtx context.Context, reservation *taskReservation) {
	if isReleaseExecution(reservation.assignment.Plan) {
		p.executeRelease(runCtx, reservation)
		return
	}
	err := reservation.ctx.Err()
	planHash := taskassignment.PlanDigest(reservation.assignment.Plan)
	exitCode := int32(0)
	failedStepID := ""
	diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE
	reconciliationRequired := false
	mutationAttempted := false
	componentCompensated := false
	projects := make(map[string]*agentpb.ObservedProject)
	environmentDirectoryTask := usesEnvironmentDirectory(reservation.assignment.Plan)
	var environmentDirectoryResult *agentpb.EnvironmentDirectoryTaskResult
	var dnsResolverCandidateObservation *agentpb.DNSResolverObservationEvidence
	var dnsResolverRollbackObservation *agentpb.DNSResolverObservationEvidence
	var managedConfigStep *agentpb.ExecutionStep
	attemptedSteps := make([]*agentpb.ExecutionStep, 0, len(reservation.assignment.Plan.Steps))
	for _, step := range reservation.assignment.Plan.Steps {
		if err != nil {
			break
		}
		p.emitProgress(runCtx, TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID,
			TaskID:       reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, ExecutionEpoch: reservation.assignment.ExecutionEpoch,
			Ordinal: 1, State: TaskProgressRunning,
		})
		stepCtx, cancel := context.WithTimeout(
			reservation.ctx,
			time.Duration(step.TimeoutSeconds)*time.Second,
		)
		attemptedSteps = append(attemptedSteps, step)
		if step.GetMaterializeFile() != nil {
			var payload filematerialization.Payload
			payload, err = p.materializations.Take(
				stepCtx,
				reservation.assignment.TaskID,
				step.GetStepId(),
			)
			if err == nil {
				if p.materializer == nil {
					err = filematerialization.CloseSourceWithError(
						payload.Source,
						"agent: materialization runtime is not configured",
					)
				} else {
					err = p.materializer.ExecuteStep(stepCtx, reservation.assignment, step, payload)
				}
			}
		} else if step.GetAdapterProcedure() != nil {
			var stepResult backingadapter.StepResult
			stepResult, err = p.adapter.ExecuteStep(stepCtx, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
		} else if step.GetBackingHookProcedure() != nil {
			err = p.executeBackingHookStep(stepCtx, reservation.assignment, step)
		} else if (step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil ||
			step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetManagedVolumeDirectoryRemove() != nil) &&
			p.environmentDirectories != nil {
			var stepResult directoryruntime.StepResult
			stepResult, err = p.environmentDirectories.ExecuteStep(stepCtx, reservation.assignment, step, p.CheckpointVolumeRemoval)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
			if stepResult.FailedStepID != "" {
				failedStepID = stepResult.FailedStepID
			}
			if step.GetManagedVolumeDirectoryRemove() != nil {
				environmentDirectoryResult = &agentpb.EnvironmentDirectoryTaskResult{
					NextCursor: append([]byte(nil), stepResult.NextCursor...), MutationCount: stepResult.MutationCount,
					Complete: stepResult.Complete, ResponseSha256: append([]byte(nil), stepResult.ResponseSHA256...),
				}
			}
		} else if step.GetBackupArtifactPrune() != nil {
			err = p.executeBackupArtifactPrune(stepCtx, reservation.assignment, step)
		} else if step.GetComponentApply() != nil {
			var payload componentaction.ManagedConfigPayload
			if reservation.assignment.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
				step.GetComponentApply().GetManagedConfigContent() {
				payload, err = p.managedConfigs.Take(stepCtx, reservation.assignment.TaskID, step.GetStepId())
			}
			if err == nil {
				if p.componentActions == nil {
					err = componentaction.CloseSourceWithError(payload.Source, "agent: Component action runtime is not configured")
				} else {
					var actionResult *componentaction.ComponentActionResult
					actionResult, err = p.componentActions.ExecuteComponentAction(
						stepCtx, reservation.assignment, step, payload,
					)
					if actionResult != nil && actionResult.DNSResolverObservation != nil {
						dnsResolverCandidateObservation = actionResult.DNSResolverObservation
					}
					if step.GetComponentApply().GetManagedConfigContent() {
						if actionResult != nil {
							managedConfigStep = step
							mutationAttempted = true
						}
						if err == nil && !managedConfigPublicationProven(step, actionResult) {
							err = errs.New(
								errs.KindInternal,
								"agent: managed-config publication result is invalid",
							)
						}
					}
				}
			}
		} else if step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil {
			mutationAttempted = true
			if p.hostResolution == nil {
				err = errs.New(errs.KindInternal, "agent: host resolution runtime is not configured")
			} else {
				err = p.hostResolution.ExecuteHostResolution(stepCtx, reservation.assignment, step)
			}
		} else if step.GetRunScript() != nil {
			if p.scriptRuntime == nil {
				err = errs.New(errs.KindInternal, "agent: Script runtime is not configured")
			} else {
				exitCode, err = p.scriptRuntime.ExecuteScript(
					stepCtx, reservation.assignment, step, p.CheckpointScript,
				)
			}
		} else if p.compose == nil {
			err = p.executeStep(stepCtx, step)
		} else {
			var stepResult composeruntime.StepResult
			stepResult, err = p.compose.ExecuteStep(stepCtx, reservation.assignment, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
			if stepResult.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
				diagnostic = stepResult.Diagnostic
			}
			if stepResult.Observed != nil {
				projects[stepResult.Observed.GetProjectName()] = proto.Clone(stepResult.Observed).(*agentpb.ObservedProject)
			}
			reconciliationRequired = reconciliationRequired || stepResult.ReconciliationRequired
			mutationAttempted = mutationAttempted || stepResult.MutationAttempted
		}
		cancel()
		if err != nil {
			failedStepID = step.GetStepId()
		}
		p.emitProgress(runCtx, TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID,
			TaskID:       reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, ExecutionEpoch: reservation.assignment.ExecutionEpoch, Ordinal: 2,
			State: progressStateFor(reservation.ctx, err),
		})
	}
	if reservation.assignment.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if err == nil && managedConfigStep != nil {
			finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			var finalization componentaction.ManagedConfigTransactionState
			finalization, err = p.componentActions.FinalizeManagedConfig(
				finalizeCtx, reservation.assignment, managedConfigStep, true,
			)
			finalizeCancel()
			if err == nil && !managedConfigCommitProven(managedConfigStep, finalization) {
				err = errs.New(errs.KindInternal, "agent: managed-config commit result is invalid")
			}
			if err != nil {
				failedStepID = managedConfigStep.GetStepId()
			}
		}
		if err != nil {
			var compensationErr error
			dnsResolverRollbackObservation, compensationErr = p.compensateComponentLifecycle(
				reservation.assignment, managedConfigStep, attemptedSteps,
			)
			if compensationErr != nil {
				err = errors.Join(err, compensationErr)
				reconciliationRequired = true
			} else if mutationAttempted {
				componentCompensated = true
			}
		}
	}
	terminal := terminalFor(reservation.ctx, err)
	if terminal != TaskTerminalCompleted && mutationAttempted && !componentCompensated {
		reconciliationRequired = true
	}
	if err != nil && terminal == TaskTerminalFailed && p.logger != nil {
		p.logger.Error("agent: step failed", "task_id", reservation.assignment.TaskID, "error", err)
	}
	result := TaskResult{
		AssignmentID: reservation.assignment.AssignmentID,
		TaskID:       reservation.assignment.TaskID, PlanHash: planHash, Terminal: terminal,
		ExitCode: exitCode, ExecutionEpoch: reservation.assignment.ExecutionEpoch,
		ReleaseRecoveryRecordSHA256: append([]byte(nil), reservation.assignment.ReleaseRecoveryRecordSHA256...),
	}
	if environmentDirectoryTask {
		result.EnvironmentDirectory = &agentpb.EnvironmentDirectoryTaskResult{FailedStepId: failedStepID}
		if environmentDirectoryResult != nil {
			result.EnvironmentDirectory.NextCursor = environmentDirectoryResult.NextCursor
			result.EnvironmentDirectory.MutationCount = environmentDirectoryResult.MutationCount
			result.EnvironmentDirectory.Complete = environmentDirectoryResult.Complete
			result.EnvironmentDirectory.ResponseSha256 = environmentDirectoryResult.ResponseSha256
		}
	} else {
		result.Compose = composeTaskResult(projects, failedStepID, diagnostic, reconciliationRequired)
		result.Compose.DnsResolverCandidateObservation = dnsResolverCandidateObservation
		result.Compose.DnsResolverRollbackObservation = dnsResolverRollbackObservation
	}
	p.complete(runCtx, reservation, result)
}
