package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RollingForwardSteps expands only the immutable procedure. Both the planner
// and plan validator use this same closed phase-to-operation mapping; execution
// still requires the Task's running barrier and durable replica cursor.
func RollingForwardSteps(
	member *agentpb.CandidateReleaseMember,
	artifacts []*agentpb.ComposeArtifact,
	prerequisite string,
) ([]*agentpb.ExecutionStep, error) {
	plan, err := OpenRollingPlan(member)
	if err != nil {
		return nil, err
	}
	if prerequisite != "" &&
		(ids.Validate(ids.KindStep, prerequisite) != nil || slices.Contains(member.GetForwardStepIds(), prerequisite)) {
		return nil, errs.New(errs.KindValidationFailed, "rolling prerequisite step identity is invalid")
	}
	if err := ValidateRollingArtifacts(plan, artifacts); err != nil {
		return nil, err
	}
	indexed := make(map[string]*agentpb.ComposeArtifact, len(artifacts))
	for _, artifact := range artifacts {
		indexed[artifact.GetArtifactId()] = artifact
	}
	proxy := releaseRuntimeService(
		indexed[member.GetCandidateArtifactId()],
		plan.ServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	if (proxy != nil) != member.GetRolling().GetHasProxy() {
		return nil, errs.New(errs.KindValidationFailed, "rolling proxy presence differs from its candidate artifact")
	}
	if err := validateRollingProxyConfigs(plan, member.GetRolling(), proxy); err != nil {
		return nil, err
	}
	ensureProxy := proxy != nil
	if len(plan.Prior) != 0 {
		priorProxy := releaseRuntimeService(
			indexed[plan.Prior[0].ArtifactID],
			plan.ServiceID,
			"",
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
		)
		ensureProxy = ensureProxy && priorProxy == nil
		if priorProxy != nil && proxy == nil {
			return nil, errs.New(errs.KindValidationFailed, "rolling cannot discard an existing stable Service proxy")
		}
		if priorProxy != nil {
			if err := validateStableProxyContinuity(proxy, priorProxy); err != nil {
				return nil, err
			}
		}
	}
	changes, err := plan.Changes()
	if err != nil {
		return nil, err
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(member.GetForwardStepIds()))
	for index, change := range changes {
		for phaseIndex, phase := range change.Phases() {
			step := &agentpb.ExecutionStep{StepId: member.GetRolling().GetChanges()[index].GetStepIds()[phaseIndex],
				TimeoutSeconds: plan.ReplicaTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				PrerequisiteStepId: prerequisite}
			switch phase {
			case rollingrelease.StartCandidate:
				step.Payload = &agentpb.ExecutionStep_ComposeWorkloadApply{
					ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{
						ArtifactId: change.Candidate.ArtifactID, ServiceId: plan.ServiceID,
						RollingReplicaOrdinal: change.Candidate.Ordinal, EnsureProxy: ensureProxy && index == 0},
				}
			case rollingrelease.WaitCandidateHealthy:
				step.Payload = &agentpb.ExecutionStep_WaitWorkloadHealthy{
					WaitWorkloadHealthy: &agentpb.WaitWorkloadHealthy{
						ArtifactId: change.Candidate.ArtifactID, ServiceId: plan.ServiceID, RollingReplicaOrdinal: change.Candidate.Ordinal},
				}
			case rollingrelease.SwitchTraffic:
				step.Payload = &agentpb.ExecutionStep_RollingTrafficSwitch{
					RollingTrafficSwitch: &agentpb.RollingTrafficSwitch{
						ServiceId: plan.ServiceID, ChangeOrdinal: change.Ordinal},
				}
			case rollingrelease.DrainPredecessor:
				step.Payload = &agentpb.ExecutionStep_RollingReplicaDrain{
					RollingReplicaDrain: &agentpb.RollingReplicaDrain{
						ServiceId: plan.ServiceID, ChangeOrdinal: change.Ordinal},
				}
			case rollingrelease.RemovePredecessor, rollingrelease.RemoveRetained:
				retire := change.Retire
				if phase == rollingrelease.RemoveRetained {
					retire = change.Retained
				}
				service, selectErr := RollingReplicaService(indexed[retire.ArtifactID], plan.ServiceID, *retire)
				if selectErr != nil {
					return nil, selectErr
				}
				remove := &agentpb.ComposeWorkloadRemove{ArtifactId: retire.ArtifactID, ServiceId: plan.ServiceID}
				switch service.GetRole() {
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
					remove.RollingReplicaOrdinal = retire.Ordinal
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
					remove.Target = "singleton"
				case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
					remove.Target = service.GetSlot()
				}
				step.Payload = &agentpb.ExecutionStep_ComposeWorkloadRemove{ComposeWorkloadRemove: remove}
			}
			steps = append(steps, step)
			prerequisite = step.StepId
		}
	}
	steps = append(steps, &agentpb.ExecutionStep{StepId: member.GetRolling().GetAcknowledgeStepId(),
		TimeoutSeconds: plan.ReplicaTimeoutSeconds, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		PrerequisiteStepId: prerequisite, Payload: &agentpb.ExecutionStep_RollingReleaseAcknowledge{
			RollingReleaseAcknowledge: &agentpb.RollingReleaseAcknowledge{ServiceId: plan.ServiceID}}})
	return steps, nil
}

func validateRollingMemberSteps(plan *agentpb.ExecutionPlan, member *agentpb.CandidateReleaseMember) error {
	rolling, err := OpenRollingPlan(member)
	if err != nil {
		return err
	}
	prior := member.GetServingPredecessor()
	if (len(rolling.Prior) != 0) != (prior.GetPriorArtifactId() != "") ||
		(len(rolling.Retained) != 0) != (prior.GetRetainedPriorArtifactId() != "") {
		return errs.New(
			errs.KindValidationFailed,
			"rolling predecessors differ from the captured restoration authority",
		)
	}
	if len(rolling.Prior) != 0 && (rolling.Prior[0].ArtifactID != prior.GetPriorArtifactId() ||
		rolling.Prior[0].ReleaseID != prior.GetPriorReleaseId()) {
		return errs.New(errs.KindValidationFailed, "rolling serving predecessor differs from its restoration source")
	}
	if len(rolling.Retained) != 0 && rolling.Retained[0].ArtifactID != prior.GetRetainedPriorArtifactId() {
		return errs.New(errs.KindValidationFailed, "rolling inactive slot differs from its restoration source")
	}
	stepsByID := make(map[string]*agentpb.ExecutionStep, len(plan.GetSteps()))
	for _, step := range plan.GetSteps() {
		stepsByID[step.GetStepId()] = step
	}
	first := stepsByID[member.GetForwardStepIds()[0]]
	if first == nil {
		return errs.New(errs.KindValidationFailed, "rolling first phase is absent from the execution plan")
	}
	expected, err := RollingForwardSteps(member, plan.GetArtifacts(), first.GetPrerequisiteStepId())
	if err != nil {
		return err
	}
	for _, step := range expected {
		if !proto.Equal(step, stepsByID[step.GetStepId()]) {
			return errs.New(
				errs.KindValidationFailed,
				"rolling step differs from its captured phase, workload or prerequisite",
			)
		}
	}
	return nil
}

func validateRollingPhaseReference(plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep,
	serviceID string, ordinal uint32, phases ...rollingrelease.Phase,
) error {
	if !releaseOperation(plan.GetOperation()) {
		return errs.New(errs.KindValidationFailed, "rolling phases require a Release operation")
	}
	member, err := RollingMember(plan, serviceID)
	if err != nil {
		return err
	}
	actualOrdinal, actualPhase, err := RollingStepPosition(member, step.GetStepId())
	if err != nil {
		return err
	}
	if !slices.Contains(phases, actualPhase) || ordinal != 0 && actualOrdinal != ordinal ||
		step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
		return errs.New(errs.KindValidationFailed, "rolling step does not match its captured phase")
	}
	return nil
}
