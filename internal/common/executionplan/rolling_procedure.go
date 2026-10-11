package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// OpenRollingPlan binds a stored member procedure to the shared rollout model.
// It does not grant execution or infer a predecessor from observed containers.
func OpenRollingPlan(member *agentpb.CandidateReleaseMember) (rollingrelease.Plan, error) {
	procedure := member.GetRolling()
	if procedure == nil {
		return rollingrelease.Plan{}, errs.New(errs.KindValidationFailed, "rolling member procedure is absent")
	}
	if procedure.GetHasProxy() {
		if _, err := OpenProxyImplementation(procedure.GetProxyImplementation()); err != nil {
			return rollingrelease.Plan{}, err
		}
	} else if procedure.GetProxyImplementation() != agentpb.ServiceProxyImplementation_SERVICE_PROXY_IMPLEMENTATION_UNSPECIFIED {
		return rollingrelease.Plan{}, errs.New(errs.KindValidationFailed, "portless rolling procedure carries proxy implementation authority")
	}
	plan := rollingrelease.Plan{ServiceID: member.GetServiceId(), ReleaseID: member.GetCandidateReleaseId(),
		ReplicaTimeoutSeconds: procedure.GetReplicaTimeoutSeconds(),
		Retained:              openRollingReplicas(procedure.GetRetained()),
		Prior: openRollingReplicas(
			procedure.GetPrior(),
		), Candidate: openRollingReplicas(procedure.GetCandidate())}
	if err := plan.Validate(); err != nil {
		return rollingrelease.Plan{}, err
	}
	if plan.Candidate[0].ArtifactID != member.GetCandidateArtifactId() {
		return rollingrelease.Plan{}, errs.New(
			errs.KindValidationFailed,
			"rolling candidate artifact differs from its Release member",
		)
	}
	changes, err := plan.Changes()
	if err != nil {
		return rollingrelease.Plan{}, err
	}
	if len(procedure.GetChanges()) != len(changes) {
		return rollingrelease.Plan{}, errs.New(
			errs.KindValidationFailed,
			"rolling procedure does not cover every replica change",
		)
	}
	seen := make(map[string]bool)
	var forward []string
	var generation uint64
	for index, change := range changes {
		captured := procedure.GetChanges()[index]
		if captured == nil || captured.GetOrdinal() != change.Ordinal ||
			len(captured.GetStepIds()) != len(change.Phases()) {
			return rollingrelease.Plan{}, errs.New(
				errs.KindValidationFailed,
				"rolling change does not match its required phases",
			)
		}
		for _, stepID := range captured.GetStepIds() {
			if ids.Validate(ids.KindStep, stepID) != nil || seen[stepID] {
				return rollingrelease.Plan{}, errs.New(
					errs.KindValidationFailed,
					"rolling phase identity is invalid or duplicated",
				)
			}
			seen[stepID] = true
			forward = append(forward, stepID)
		}
		if procedure.GetHasProxy() {
			if captured.GetProxyGeneration() == 0 || index > 0 && captured.GetProxyGeneration() != generation+1 ||
				!validSealedProxyConfig(captured.GetProxyConfigJson(), captured.GetProxyConfigSha256()) {
				return rollingrelease.Plan{}, errs.New(
					errs.KindValidationFailed,
					"rolling change has invalid proxy configuration or generation",
				)
			}
			parsed, parseErr := ProxyConfigGeneration(
				procedure.GetProxyImplementation(),
				captured.GetProxyConfigJson(),
				plan.ReleaseID,
			)
			if parseErr != nil || parsed != captured.GetProxyGeneration() {
				return rollingrelease.Plan{}, errs.New(
					errs.KindValidationFailed,
					"rolling proxy configuration generation differs from its captured change",
				)
			}
			generation = captured.GetProxyGeneration()
		} else if captured.GetProxyGeneration() != 0 || len(captured.GetProxyConfigJson()) != 0 || len(captured.GetProxyConfigSha256()) != 0 {
			return rollingrelease.Plan{}, errs.New(errs.KindValidationFailed, "portless rolling change carries proxy authority")
		}
	}
	if ids.Validate(ids.KindStep, procedure.GetAcknowledgeStepId()) != nil || seen[procedure.GetAcknowledgeStepId()] {
		return rollingrelease.Plan{}, errs.New(
			errs.KindValidationFailed,
			"rolling completion step identity is invalid or duplicated",
		)
	}
	forward = append(forward, procedure.GetAcknowledgeStepId())
	if !slices.Equal(forward, member.GetForwardStepIds()) {
		return rollingrelease.Plan{}, errs.New(
			errs.KindValidationFailed,
			"rolling phase order differs from its Release forward procedure",
		)
	}
	return plan, nil
}

func openRollingReplicas(values []*agentpb.RollingReplica) []rollingrelease.Replica {
	result := make([]rollingrelease.Replica, len(values))
	for index, value := range values {
		result[index] = rollingrelease.Replica{ReleaseID: value.GetReleaseId(), ArtifactID: value.GetArtifactId(),
			ComposeName: value.GetComposeName(), Ordinal: value.GetOrdinal(),
			ContainerID: value.GetContainerId(), ContainerName: value.GetContainerName(), ComposeReplica: value.GetComposeReplica()}
	}
	return result
}

// RollingMember returns one declared member, never a best-effort Service match.
func RollingMember(plan *agentpb.ExecutionPlan, serviceID string) (*agentpb.CandidateReleaseMember, error) {
	var selected *agentpb.CandidateReleaseMember
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		if member.GetServiceId() == serviceID && member.GetRolling() != nil {
			if selected != nil {
				return nil, errs.New(errs.KindValidationFailed, "rolling Service member is duplicated")
			}
			selected = member
		}
	}
	if selected == nil {
		return nil, errs.New(errs.KindValidationFailed, "rolling Service member is absent")
	}
	return selected, nil
}

// RollingStepPosition is the durable cursor binding, including final proof.
// The final acknowledgement has change zero and no phase; it is not a replica.
func RollingStepPosition(member *agentpb.CandidateReleaseMember, stepID string) (uint32, rollingrelease.Phase, error) {
	plan, err := OpenRollingPlan(member)
	if err != nil {
		return 0, "", err
	}
	if member.GetRolling().GetAcknowledgeStepId() == stepID {
		return 0, "", nil
	}
	changes, err := plan.Changes()
	if err != nil {
		return 0, "", err
	}
	for index, change := range member.GetRolling().GetChanges() {
		for phaseIndex, candidateID := range change.GetStepIds() {
			if candidateID == stepID {
				return change.GetOrdinal(), changes[index].Phases()[phaseIndex], nil
			}
		}
	}
	return 0, "", errs.New(errs.KindValidationFailed, "step is not part of the rolling member procedure")
}
