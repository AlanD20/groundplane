package composeruntime

import (
	"context"
	"slices"

	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) serviceAbsence(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (StepResult, error) {
	procedure := assignment.Plan.GetServiceAbsenceRecoveryProcedure()
	artifact := taskassignment.ComposeArtifact(assignment.Plan, procedure.GetArtifactId())
	if procedure == nil || artifact == nil || procedure.GetServiceId() == "" || artifact.GetProjectName() == "" {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Service absence authority is invalid",
		)
	}
	names, err := composeNamesForServiceIDs(artifact, []string{procedure.GetServiceId()})
	if err != nil || len(names) != len(artifact.GetServices()) {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Service absence identity is incomplete",
		)
	}
	slices.Sort(names)
	services, err := convergenceServices(artifact, names)
	if err != nil {
		return StepResult{ReconciliationRequired: true}, err
	}
	prove := func(observed *agentpb.ObservedProject) (*agentpb.ObservedProject, bool, error) {
		if observed == nil || observed.GetProjectName() != artifact.GetProjectName() {
			return observed, false, errs.New(errs.KindInternal, "agent: Service absence observation is invalid")
		}
		scoped := scopeLifecycleComposeObservation(artifact, observed, services)
		if len(scoped.GetCollisions()) != 0 {
			return scoped, false, errs.New(errs.KindStateConflict, "agent: Service absence ownership is ambiguous")
		}
		return scoped, removedServices(scoped, []string{procedure.GetServiceId()}) == nil, nil
	}
	evidence := func(absent bool) *agentpb.ServiceAbsenceEvidence {
		return &agentpb.ServiceAbsenceEvidence{
			ServiceId: procedure.GetServiceId(), ArtifactId: procedure.GetArtifactId(),
			ComposeProjectName: artifact.GetProjectName(), ComposeNames: slices.Clone(names), AbsenceProven: absent,
		}
	}
	if step.GetServiceAbsenceProbe() != nil {
		observed, observeErr := runtime.observer.Observe(ctx, assignment.Plan, procedure.GetArtifactId())
		result := StepResult{}
		if observeErr != nil {
			result.ReconciliationRequired = true
			return result, observeErr
		}
		scoped, absent, proveErr := prove(observed)
		result.Observed, result.RestorationRequired, result.ServiceAbsenceEvidence = scoped, !absent, evidence(absent)
		if proveErr != nil {
			result.ReconciliationRequired = true
			return result, proveErr
		}
		return result, nil
	}
	remove := step.GetComposeRemove()
	if remove == nil || remove.GetArtifactId() != procedure.GetArtifactId() ||
		!slices.Equal(remove.GetServiceIds(), []string{procedure.GetServiceId()}) || remove.GetWholeProject() {
		return StepResult{
			ReconciliationRequired: true,
		}, errs.New(
			errs.KindInternal,
			"agent: Service absence mutation is invalid",
		)
	}
	before, observeErr := runtime.observer.Observe(ctx, assignment.Plan, procedure.GetArtifactId())
	if observeErr != nil {
		return StepResult{Observed: before, ReconciliationRequired: true}, observeErr
	}
	before, absent, ownershipErr := prove(before)
	if ownershipErr != nil {
		return StepResult{Observed: before, ReconciliationRequired: true}, ownershipErr
	}
	if absent {
		return StepResult{Observed: before, ServiceAbsenceEvidence: evidence(true)}, nil
	}
	var proof *agentpb.ObservedProject
	result, mutationErr := runtime.mutate(
		ctx,
		assignment,
		step,
		procedure.GetArtifactId(),
		func(observed *agentpb.ObservedProject) error {
			scoped, absent, proveErr := prove(observed)
			proof = scoped
			if proveErr != nil {
				return proveErr
			}
			if !absent {
				return errs.New(errs.KindRequestFailed, "agent: Service absence did not converge")
			}
			return nil
		},
	)
	if proof != nil {
		result.Observed = proof
	}
	result.ServiceAbsenceEvidence = evidence(mutationErr == nil && !result.ReconciliationRequired)
	return result, mutationErr
}

func ServiceAbsenceEvidenceProven(assignment taskassignment.Assignment, result StepResult) bool {
	matched, absent := serviceAbsenceEvidenceStatus(assignment, result)
	return matched && absent
}

func ServiceAbsenceProbeStatus(assignment taskassignment.Assignment, result StepResult) (bool, error) {
	matched, absent := serviceAbsenceEvidenceStatus(assignment, result)
	if !matched || result.RestorationRequired == absent {
		return false, errs.New(errs.KindInternal, "agent: Service absence probe evidence is invalid")
	}
	return !absent, nil
}

func serviceAbsenceEvidenceStatus(assignment taskassignment.Assignment, result StepResult) (bool, bool) {
	procedure := assignment.Plan.GetServiceAbsenceRecoveryProcedure()
	artifact := taskassignment.ComposeArtifact(assignment.Plan, procedure.GetArtifactId())
	if procedure == nil || artifact == nil {
		return false, false
	}
	names, err := composeNamesForServiceIDs(artifact, []string{procedure.GetServiceId()})
	slices.Sort(names)
	evidence := result.ServiceAbsenceEvidence
	matched := err == nil && evidence != nil && evidence.GetServiceId() == procedure.GetServiceId() &&
		evidence.GetArtifactId() == procedure.GetArtifactId() && evidence.GetComposeProjectName() == artifact.GetProjectName() &&
		slices.Equal(evidence.GetComposeNames(), names)
	return matched, matched && evidence.GetAbsenceProven()
}
