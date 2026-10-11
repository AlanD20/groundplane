package composeruntime

import (
	"context"

	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) componentRestoration(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	procedure := assignment.Plan.GetComponentRecoveryProcedure()
	if procedure == nil {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Component recovery procedure is absent",
		)
	}
	artifactID, serviceID, projectName, target := componentRestorationTarget(procedure)
	artifact := taskassignment.ComposeArtifact(assignment.Plan, artifactID)
	if procedure.GetPredecessor() != nil {
		if artifact != nil {
			projectName = artifact.GetProjectName()
		}
	}
	if artifactID == "" || serviceID == "" || projectName == "" || artifact == nil ||
		target == agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_UNSPECIFIED {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Component recovery target is invalid",
		)
	}
	names, err := composeNamesForServiceIDs(artifact, []string{serviceID})
	if err != nil {
		return StepResult{ReconciliationRequired: true}, err
	}
	services, err := convergenceServices(artifact, names)
	if err != nil {
		return StepResult{ReconciliationRequired: true}, err
	}
	prove := func(observed *agentpb.ObservedProject) (*agentpb.ObservedProject, bool, error) {
		if observed == nil || observed.GetProjectName() != projectName {
			return observed, false, errs.New(errs.KindInternal, "agent: Component recovery observation is invalid")
		}
		scoped := scopeLifecycleComposeObservation(artifact, observed, services)
		if len(scoped.GetCollisions()) != 0 {
			return scoped, false, errs.New(errs.KindStateConflict, "agent: Component recovery ownership is ambiguous")
		}
		if target == agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_CANDIDATE_ABSENCE {
			return scoped, removedServices(scoped, []string{serviceID}) == nil, nil
		}
		convergence, err := evaluateSelectedComposeConvergence(artifact, scoped, names, true)
		return scoped, convergence.Ready, err
	}
	evidence := func(restored bool) *agentpb.ComponentRestorationEvidence {
		return &agentpb.ComponentRestorationEvidence{
			ComponentId: procedure.GetComponentId(), ArtifactId: artifactID, ServiceId: serviceID,
			ComposeProjectName: projectName, Target: target, Restored: restored,
		}
	}
	if step.GetComponentRestorationProbe() != nil {
		observed, err := runtime.observer.Observe(ctx, assignment.Plan, artifactID)
		result := StepResult{}
		if err != nil {
			result.ReconciliationRequired = true
			return result, err
		}
		scoped, restored, proveErr := prove(observed)
		result.Observed = scoped
		if proveErr != nil {
			result.ReconciliationRequired = true
			return result, proveErr
		}
		if restored && executionplan.ComponentConfigurationProofRequired(assignment.Plan) {
			response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
				Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID, TaskId: assignment.TaskID,
				OperationId: assignment.OperationID, Plan: assignment.Plan, StepId: step.GetStepId(),
				TimeoutSeconds: taskassignment.RemainingSeconds(ctx, step.GetTimeoutSeconds()),
			})
			if err != nil {
				result.ReconciliationRequired = true
				return result, err
			}
			if response == nil || response.GetSchema() != composeHelperSchema ||
				response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
				response.GetExitCode() != 0 || response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
				result.ReconciliationRequired = true
				return result, errs.New(errs.KindRequestFailed, "agent: Component configuration recovery probe failed")
			}
			matched, configurationRestored := componentRestorationEvidenceStatus(assignment, StepResult{
				ComponentRestorationEvidence: response.GetComponentRestorationEvidence(),
			})
			if !matched {
				result.ReconciliationRequired = true
				return result, errs.New(
					errs.KindInternal,
					"agent: Component configuration recovery proof is not bound to this Task",
				)
			}
			restored = configurationRestored
		}
		result.RestorationRequired = !restored
		result.ComponentRestorationEvidence = evidence(restored)
		return result, nil
	}
	if step.GetComponentRestorationCompensate() == nil {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Component recovery step is invalid",
		)
	}
	var proofObservation *agentpb.ObservedProject
	result, err := runtime.mutate(ctx, assignment, step, artifactID, func(observed *agentpb.ObservedProject) error {
		scoped, restored, proveErr := prove(observed)
		proofObservation = scoped
		if proveErr != nil {
			return proveErr
		}
		if !restored {
			return errs.New(errs.KindRequestFailed, "agent: Component runtime restoration is not exact")
		}
		return nil
	})
	if proofObservation != nil {
		result.Observed = proofObservation
	}
	if err == nil && executionplan.ComponentConfigurationProofRequired(assignment.Plan) &&
		!ComponentRestorationEvidenceProven(assignment, result) {
		result.ReconciliationRequired = true
		err = errs.New(
			errs.KindRequestFailed,
			"agent: Component runtime is running but its previous Routes are not proved restored",
		)
	}
	result.ComponentRestorationEvidence = evidence(err == nil && !result.ReconciliationRequired)
	return result, err
}

func componentRestorationTarget(
	procedure *agentpb.ComponentRecoveryProcedure,
) (artifactID, serviceID, projectName string, target agentpb.ComponentRestorationTarget) {
	if prior := procedure.GetPredecessor(); prior != nil {
		artifact := prior.GetPriorArtifactId()
		return artifact, prior.GetServiceId(), "", agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_PREDECESSOR
	}
	absence := procedure.GetCandidateAbsence()
	return procedure.GetCandidateArtifactId(), absence.GetServiceId(), absence.GetComposeProjectName(),
		agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_CANDIDATE_ABSENCE
}

func ComponentRestorationEvidenceProven(
	assignment taskassignment.Assignment,
	result StepResult,
) bool {
	matched, restored := componentRestorationEvidenceStatus(assignment, result)
	return matched && restored
}

func ComponentRestorationProbeStatus(
	assignment taskassignment.Assignment,
	result StepResult,
) (bool, error) {
	matched, restored := componentRestorationEvidenceStatus(assignment, result)
	if !matched || result.RestorationRequired == restored {
		return false, errs.New(errs.KindInternal, "agent: Component restoration probe evidence is invalid")
	}
	return !restored, nil
}

func componentRestorationEvidenceStatus(
	assignment taskassignment.Assignment,
	result StepResult,
) (bool, bool) {
	procedure := assignment.Plan.GetComponentRecoveryProcedure()
	if procedure == nil {
		return false, false
	}
	artifactID, serviceID, projectName, target := componentRestorationTarget(procedure)
	if procedure != nil && procedure.GetPredecessor() != nil {
		artifact := taskassignment.ComposeArtifact(assignment.Plan, artifactID)
		if artifact != nil {
			projectName = artifact.GetProjectName()
		}
	}
	evidence := result.ComponentRestorationEvidence
	matched := evidence != nil && evidence.GetComponentId() == procedure.GetComponentId() &&
		evidence.GetArtifactId() == artifactID && evidence.GetServiceId() == serviceID &&
		evidence.GetComposeProjectName() == projectName && evidence.GetTarget() == target
	return matched, matched && evidence.GetRestored()
}
