package agent

import (
	"context"
	"errors"
	"slices"
	"time"

	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func hasConfigurationRestoration(plan *agentpb.ExecutionPlan) bool {
	return executionplan.ConfigurationRestorationForPlan(plan) != nil
}

func releaseRecoveryPair(
	plan *agentpb.ExecutionPlan,
	stepID string,
) (probeID string, compensateID string, configuration bool) {
	for _, file := range executionplan.ConfigurationRestorationForPlan(plan).GetFiles() {
		if file.GetProbeStepId() == stepID || file.GetCompensateStepId() == stepID {
			return file.GetProbeStepId(), file.GetCompensateStepId(), true
		}
	}
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		selected := member.GetServingPredecessor()
		probe, compensate := selected.GetProbeStepId(), selected.GetCompensateStepId()
		if selected == nil {
			absence := member.GetCandidateAbsence()
			probe, compensate = absence.GetProbeStepId(), absence.GetCompensateStepId()
		}
		if probe == stepID || compensate == stepID {
			return probe, compensate, false
		}
	}
	if procedure := plan.GetServiceAbsenceRecoveryProcedure(); procedure != nil &&
		(procedure.GetProbeStepId() == stepID || procedure.GetCompensateStepId() == stepID) {
		return procedure.GetProbeStepId(), procedure.GetCompensateStepId(), false
	}
	return "", "", false
}

type recoverySelection struct {
	phase      agentpb.ReleaseRecoveryPhase
	stepIDs    []string
	cursor     uint32
	applicable []string
}

func selectedRecovery(assignment taskassignment.Assignment) recoverySelection {
	if directive := assignment.ReleaseRecoveryDirective; directive != nil {
		stepIDs := executionplan.RecoveryStepIDs(assignment.Plan.GetCandidateReleaseProcedure())
		if assignment.Plan.GetServiceAbsenceRecoveryProcedure() != nil {
			stepIDs = executionplan.ServiceAbsenceRecoveryStepIDs(assignment.Plan.GetServiceAbsenceRecoveryProcedure())
		}
		return recoverySelection{directive.GetPhase(), stepIDs,
			directive.GetCursor(), directive.GetApplicableCompensationStepIds()}
	}
	return recoverySelection{}
}

func (p *WorkerPool) runReleaseRecovery(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	directive := selectedRecovery(reservation.assignment)
	stepIDs := directive.stepIDs
	steps := make(map[string]*agentpb.ExecutionStep, len(reservation.assignment.Plan.GetSteps()))
	for _, step := range reservation.assignment.Plan.GetSteps() {
		steps[step.GetStepId()] = step
	}
	if directive.phase == agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN {
		for _, stepID := range stepIDs[:len(stepIDs)/2] {
			step := steps[stepID]
			required, err := p.probeReleaseRecoveryWithoutEvent(reservation, step, state)
			if err != nil || required {
				state.err, state.failedStepID = errors.Join(
					err,
					errs.New(errs.KindInternal, "agent: proven release restoration no longer has exact proof"),
				), stepID
				state.reconciliation = true
				return
			}
		}
		state.recoveryClosed = true
		return
	}
	compensationRequired := make(map[string]bool, len(stepIDs)/2)
	for _, stepID := range stepIDs[directive.cursor:] {
		step := steps[stepID]
		probeID, compensateID, configuration := releaseRecoveryPair(reservation.assignment.Plan, stepID)
		if step == nil || probeID == "" || compensateID == "" {
			state.err, state.failedStepID = errs.New(
				errs.KindInternal,
				"agent: release recovery directive step is absent",
			), stepID
			state.reconciliation = true
			return
		}
		switch step.GetPolicy() {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE:
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				state.reconciliation = true
				return
			}
			compensationRequired[compensateID] = state.probeRequired[probeID]
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:
			required, known := compensationRequired[compensateID]
			applicable := slices.Contains(directive.applicable, compensateID)
			// File restoration can make a previously unhealthy native runtime
			// healthy again. Its earlier read-only probe is not current proof for
			// the later native compensation decision.
			if !known || !configuration && hasConfigurationRestoration(reservation.assignment.Plan) {
				probe := steps[probeID]
				if probe == nil {
					state.err, state.failedStepID = errs.New(
						errs.KindInternal,
						"agent: release recovery compensation has no sealed probe",
					), stepID
					state.reconciliation = true
					return
				}
				var err error
				required, err = p.probeReleaseRecoveryWithoutEvent(reservation, probe, state)
				if err != nil {
					state.err, state.failedStepID, state.reconciliation = err, probeID, true
					return
				}
			}
			if required && !applicable {
				state.err, state.failedStepID = errs.New(
					errs.KindInternal,
					"agent: host state cannot create an unrecorded compensation obligation",
				), stepID
				state.reconciliation = true
				return
			}
			if required {
				if !configuration && !releaseCompensationEnabled(step) {
					state.err, state.failedStepID = errs.New(
						errs.KindInternal,
						"agent: required release recovery compensation is disabled",
					), stepID
					state.reconciliation = true
					return
				}
				p.runReleaseStep(runCtx, reservation, step, state)
				if state.err != nil || !state.restored {
					if state.err == nil {
						state.err = errs.New(
							errs.KindInternal,
							"agent: release compensation returned no exact restoration proof",
						)
					}
					state.reconciliation = true
					return
				}
			} else if err := p.completeReleaseRecoveryNoop(reservation, step, state); err != nil {
				state.err, state.failedStepID, state.reconciliation = err, stepID, true
				return
			}
		default:
			state.err, state.failedStepID = errs.New(
				errs.KindInternal,
				"agent: release recovery directive selected a forward step",
			), stepID
			state.reconciliation = true
			return
		}
	}
	state.recoveryClosed = true
}

func (p *WorkerPool) probeReleaseRecoveryWithoutEvent(
	reservation *taskReservation,
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) (bool, error) {
	if step == nil {
		return false, errs.New(errs.KindInternal, "agent: release recovery probe is absent")
	}
	stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(step.GetTimeoutSeconds())*time.Second)
	defer cancel()
	if executionplan.ConfigurationFilePair(reservation.assignment.Plan, step.GetStepId()) != nil {
		result, err := p.executeReleaseConfigurationStep(stepCtx, reservation.assignment, step)
		return result.RestorationRequired, err
	}
	result, err := p.compose.ExecuteStep(stepCtx, reservation.assignment, step, nil)
	if err != nil {
		return false, err
	}
	if result.ProxyEvidence != nil {
		state.evidence[result.ProxyEvidence.GetServiceId()] = proto.CloneOf(result.ProxyEvidence)
		delete(state.rolling, result.ProxyEvidence.GetServiceId())
	}
	if result.RecreateEvidence != nil {
		state.recreate[result.RecreateEvidence.GetServiceId()] = proto.CloneOf(result.RecreateEvidence)
		delete(state.rolling, result.RecreateEvidence.GetServiceId())
	}
	if result.RollingEvidence != nil {
		state.rolling[result.RollingEvidence.GetServiceId()] = proto.CloneOf(result.RollingEvidence)
		delete(state.evidence, result.RollingEvidence.GetServiceId())
		delete(state.recreate, result.RollingEvidence.GetServiceId())
	}
	if result.CandidateAbsenceEvidence != nil {
		if err := state.recordAbsenceEvidence(result.CandidateAbsenceEvidence); err != nil {
			return false, err
		}
	}
	if result.ServiceAbsenceEvidence != nil {
		state.serviceAbsence = proto.CloneOf(result.ServiceAbsenceEvidence)
	}
	if result.ReconciliationRequired {
		return false, errs.New(errs.KindInternal, "agent: release recovery probe requires reconciliation")
	}
	if step.GetServiceAbsenceProbe() != nil {
		return composeruntime.ServiceAbsenceProbeStatus(reservation.assignment, result)
	}
	return composeruntime.ReleaseProbeEvidenceStatus(reservation.assignment, step, result)
}

func (p *WorkerPool) executeReleaseConfigurationStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (composeruntime.StepResult, error) {
	if p.materializer == nil {
		return composeruntime.StepResult{ReconciliationRequired: true}, errs.New(
			errs.KindInternal,
			"agent: materialization runtime is not configured",
		)
	}
	payload, err := p.materializations.Take(ctx, assignment.TaskID, step.GetStepId())
	if err != nil {
		return composeruntime.StepResult{ReconciliationRequired: true}, err
	}
	if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
		err = p.materializer.VerifyStep(ctx, assignment, step, payload)
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return composeruntime.StepResult{RestorationRequired: true}, nil
		}
		return composeruntime.StepResult{ReconciliationRequired: err != nil}, err
	}
	if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		return composeruntime.StepResult{ReconciliationRequired: true}, filematerialization.CloseSourceWithError(
			payload.Source,
			"agent: configuration recovery selected a forward materialization",
		)
	}
	if err := p.materializer.ExecuteStep(ctx, assignment, step, payload); err != nil {
		return composeruntime.StepResult{ReconciliationRequired: true}, err
	}
	proof, err := p.materializations.Take(ctx, assignment.TaskID, step.GetStepId())
	if err != nil {
		return composeruntime.StepResult{ReconciliationRequired: true}, err
	}
	if err := p.materializer.VerifyStep(ctx, assignment, step, proof); err != nil {
		return composeruntime.StepResult{ReconciliationRequired: true}, err
	}
	return composeruntime.StepResult{}, nil
}

func (p *WorkerPool) completeReleaseRecoveryNoop(
	reservation *taskReservation,
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) error {
	planHash := taskassignment.PlanDigest(reservation.assignment.Plan)
	ordinal := uint64(1)
	for _, progressState := range []TaskProgressState{TaskProgressRunning, TaskProgressCompleted} {
		progress := TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID,
			PlanHash: planHash, StepID: step.GetStepId(), ExecutionEpoch: reservation.assignment.ExecutionEpoch,
			Ordinal: ordinal, State: progressState,
		}
		if reservation.eventsDurable {
			receipt, err := p.emitProgressForReceipt(reservation.ctx, progress)
			if err != nil {
				return err
			}
			if progressState == TaskProgressRunning {
				ordinal = receipt.GetEventOrdinalBase()
			}
		} else {
			p.emitProgress(reservation.ctx, progress)
		}
		ordinal++
	}
	state.completed[step.GetStepId()] = struct{}{}
	return nil
}
