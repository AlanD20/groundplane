package agent

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func releaseRestorationEvidenceProven(
	assignment Assignment,
	step *agentpb.ExecutionStep,
	result composeStepResult,
) bool {
	if compensate := step.GetServiceProxyCompensate(); compensate != nil {
		return compensate.GetEnabled() && exclusiveProxyEvidence(result) && exactProxyEvidence(
			result.ProxyEvidence, compensate.GetServiceId(), compensate.GetPriorTarget(),
			compensate.GetProxyGeneration(), compensate.GetConfigSha256(), compensate.GetPriorReleaseId(), true,
		)
	}
	if compensate := step.GetServiceRecreateCompensate(); compensate != nil {
		return compensate.GetEnabled() && exclusiveRecreateEvidence(result) && exactRecreateEvidence(
			result.RecreateEvidence, compensate.GetServiceId(), compensate.GetArtifactId(),
			compensate.GetPriorReleaseId(), compensate.GetPriorTarget(), true,
		)
	}
	compensate := step.GetCandidateRestorationCompensate()
	if compensate == nil {
		return false
	}
	switch executionplan.RestorationTargetForService(assignment.RestorationAuthority, compensate.GetServiceId()) {
	case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
		return exclusiveAbsenceEvidence(result) && result.CandidateAbsenceEvidence.GetAbsenceProven() &&
			candidateAbsenceEvidenceMatches(
				assignment,
				compensate.GetServiceId(),
				compensate.GetCandidateReleaseId(),
				compensate.GetCandidateArtifactId(),
				result.CandidateAbsenceEvidence,
			)
	case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
		return candidateServingPredecessorEvidenceMatches(assignment, compensate.GetServiceId(), result)
	default:
		return false
	}
}

func releaseProbeEvidenceStatus(
	assignment Assignment,
	step *agentpb.ExecutionStep,
	result composeStepResult,
) (bool, error) {
	if result.RestorationRequired {
		if !selectedServingProbe(assignment, step) || result.ProxyEvidence != nil ||
			result.RecreateEvidence != nil || result.CandidateAbsenceEvidence != nil {
			return false, invalidReleaseProbeEvidence()
		}
		return true, nil
	}
	if probe := step.GetServiceProxyProbe(); probe != nil {
		if !exclusiveProxyEvidence(result) || result.ProxyEvidence.GetCompensated() {
			return false, invalidReleaseProbeEvidence()
		}
		prior := exactProxyEvidence(result.ProxyEvidence, probe.GetServiceId(), probe.GetExpectedTarget(),
			probe.GetProxyGeneration(), probe.GetConfigSha256(), probe.GetReleaseId(), false)
		candidate := exactProxyEvidence(result.ProxyEvidence, probe.GetServiceId(), probe.GetAlternateTarget(),
			probe.GetAlternateProxyGeneration(), probe.GetAlternateConfigSha256(), probe.GetAlternateReleaseId(), false)
		if prior == candidate {
			return false, invalidReleaseProbeEvidence()
		}
		return candidate, nil
	}
	if probe := step.GetServiceRecreateProbe(); probe != nil {
		if !exclusiveRecreateEvidence(result) {
			return false, invalidReleaseProbeEvidence()
		}
		candidate, candidateOK := recreateEvidenceExpectation(
			assignment.Plan, probe.GetCandidateArtifactId(), probe.GetServiceId(), probe.GetCandidateReleaseId(), false,
		)
		prior, priorOK := recreateEvidenceExpectation(
			assignment.Plan, probe.GetPriorArtifactId(), probe.GetServiceId(), probe.GetPriorReleaseId(), true,
		)
		matchesCandidate := candidateOK && exactRecreateEvidenceValue(result.RecreateEvidence, candidate)
		matchesPrior := priorOK && exactRecreateEvidenceValue(result.RecreateEvidence, prior)
		if matchesCandidate == matchesPrior {
			return false, invalidReleaseProbeEvidence()
		}
		return matchesCandidate, nil
	}
	probe := step.GetCandidateRestorationProbe()
	if probe == nil {
		return false, invalidReleaseProbeEvidence()
	}
	switch executionplan.RestorationTargetForService(assignment.RestorationAuthority, probe.GetServiceId()) {
	case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE:
		if !exclusiveAbsenceEvidence(result) || !candidateAbsenceEvidenceMatches(
			assignment, probe.GetServiceId(), probe.GetCandidateReleaseId(),
			probe.GetCandidateArtifactId(), result.CandidateAbsenceEvidence,
		) {
			return false, invalidReleaseProbeEvidence()
		}
		return !result.CandidateAbsenceEvidence.GetAbsenceProven(), nil
	case agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR:
		if !candidateServingPredecessorEvidenceMatches(assignment, probe.GetServiceId(), result) {
			return false, invalidReleaseProbeEvidence()
		}
		return false, nil
	default:
		return false, invalidReleaseProbeEvidence()
	}
}

type recreateEvidenceExpectationValue struct {
	serviceID, artifactID, releaseID, target string
	compensated                              bool
}

func recreateEvidenceExpectation(
	plan *agentpb.ExecutionPlan,
	artifactID, serviceID, releaseID string,
	compensated bool,
) (recreateEvidenceExpectationValue, bool) {
	artifact := composeArtifact(plan, artifactID)
	if artifact == nil || artifactID == "" || serviceID == "" || releaseID == "" {
		return recreateEvidenceExpectationValue{}, false
	}
	target, matches := "", 0
	for _, service := range artifact.GetServices() {
		serviceTarget, valid := sealedRecreateServiceTarget(service)
		if !valid || service.GetServiceId() != serviceID || restorationReleaseLabel(service) != releaseID ||
			!compensated && serviceTarget != recreateRuntimeRole {
			continue
		}
		target, matches = serviceTarget, matches+1
	}
	return recreateEvidenceExpectationValue{serviceID, artifactID, releaseID, target, compensated}, matches == 1
}

func exactRecreateEvidenceValue(
	evidence *agentpb.ServiceRecreateEvidence,
	expected recreateEvidenceExpectationValue,
) bool {
	return exactRecreateEvidence(evidence, expected.serviceID, expected.artifactID,
		expected.releaseID, expected.target, expected.compensated)
}

func exactProxyEvidence(
	evidence *agentpb.ServiceProxyEvidence,
	serviceID, target string,
	generation uint64,
	digest []byte,
	releaseID string,
	compensated bool,
) bool {
	return evidence != nil && serviceID != "" && target != "" && generation != 0 && len(digest) == sha256.Size &&
		releaseID != "" && evidence.GetServiceId() == serviceID && evidence.GetTarget() == target &&
		evidence.GetProxyGeneration() == generation && bytes.Equal(evidence.GetConfigSha256(), digest) &&
		evidence.GetReleaseId() == releaseID && evidence.GetCompensated() == compensated
}

func exactRecreateEvidence(
	evidence *agentpb.ServiceRecreateEvidence,
	serviceID, artifactID, releaseID, target string,
	compensated bool,
) bool {
	return evidence != nil && serviceID != "" && artifactID != "" && releaseID != "" && target != "" &&
		evidence.GetServiceId() == serviceID && evidence.GetArtifactId() == artifactID &&
		evidence.GetReleaseId() == releaseID && evidence.GetTarget() == target &&
		evidence.GetCompensated() == compensated
}

func candidateAbsenceEvidenceMatches(
	assignment Assignment,
	serviceID, releaseID, artifactID string,
	evidence *agentpb.CandidateAbsenceEvidence,
) bool {
	authority := assignment.RestorationAuthority
	plan := assignment.Plan
	if evidence == nil || authority == nil || plan == nil || serviceID == "" || releaseID == "" || artifactID == "" ||
		executionplan.RestorationTargetForService(
			authority,
			serviceID,
		) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE ||
		assignment.AssignmentID == "" || evidence.GetAssignmentId() != assignment.AssignmentID ||
		len(plan.GetPlanHash()) != sha256.Size || len(authority.GetPlanHash()) != sha256.Size ||
		len(
			authority.GetAuthoritySha256(),
		) != sha256.Size || !bytes.Equal(evidence.GetPlanHash(), plan.GetPlanHash()) ||
		!bytes.Equal(evidence.GetPlanHash(), authority.GetPlanHash()) ||
		!bytes.Equal(evidence.GetAuthoritySha256(), authority.GetAuthoritySha256()) ||
		evidence.GetCandidateArtifactId() != authority.GetCandidateArtifactId() ||
		evidence.GetCandidateArtifactId() != artifactID {
		return false
	}
	project, sealed, ok := candidateAbsenceRestorationTarget(plan, serviceID, releaseID, artifactID)
	return ok && evidence.GetComposeProjectName() == project &&
		candidateEvidenceMatchesAuthority(
			evidence.GetCandidates(),
			sealed,
			authority.GetCandidates(),
			serviceID,
			releaseID,
		)
}

func candidateAbsenceRestorationTarget(
	plan *agentpb.ExecutionPlan,
	serviceID, releaseID, artifactID string,
) (string, []*agentpb.CandidateReleaseService, bool) {
	var selected *agentpb.CandidateAbsenceRestoration
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		if member.GetServiceId() != serviceID || member.GetCandidateReleaseId() != releaseID ||
			member.GetCandidateArtifactId() != artifactID || member.GetCandidateAbsence() == nil {
			continue
		}
		if selected != nil {
			return "", nil, false
		}
		selected = member.GetCandidateAbsence()
	}
	if selected == nil || selected.GetComposeProjectName() == "" {
		return "", nil, false
	}
	return selected.GetComposeProjectName(), selected.GetServices(), true
}

func candidateEvidenceMatchesAuthority(
	evidence []*agentpb.CandidateReleaseService,
	sealed []*agentpb.CandidateReleaseService,
	authority []*agentpb.ReleaseRestorationCandidate,
	serviceID, releaseID string,
) bool {
	if len(evidence) != 1 || evidence[0].GetServiceId() != serviceID || evidence[0].GetReleaseId() != releaseID {
		return false
	}
	sealedSet, sealedOK := candidateServiceSet(sealed)
	authoritySet, authorityOK := restorationCandidateSet(authority)
	if !sealedOK || !authorityOK || len(sealedSet) != len(authoritySet) {
		return false
	}
	for identity := range sealedSet {
		if _, ok := authoritySet[identity]; !ok {
			return false
		}
	}
	_, selected := sealedSet[serviceID+"\x00"+releaseID]
	return selected
}

func candidateServiceSet(values []*agentpb.CandidateReleaseService) (map[string]struct{}, bool) {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == nil || value.GetServiceId() == "" || value.GetReleaseId() == "" {
			return nil, false
		}
		identity := value.GetServiceId() + "\x00" + value.GetReleaseId()
		if _, duplicate := set[identity]; duplicate {
			return nil, false
		}
		set[identity] = struct{}{}
	}
	return set, len(set) != 0
}

func restorationCandidateSet(values []*agentpb.ReleaseRestorationCandidate) (map[string]struct{}, bool) {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == nil || value.GetServiceId() == "" || value.GetReleaseId() == "" {
			return nil, false
		}
		identity := value.GetServiceId() + "\x00" + value.GetReleaseId()
		if _, duplicate := set[identity]; duplicate {
			return nil, false
		}
		set[identity] = struct{}{}
	}
	return set, len(set) != 0
}

func exclusiveProxyEvidence(result composeStepResult) bool {
	return !result.RestorationRequired && result.ProxyEvidence != nil && result.RecreateEvidence == nil &&
		result.CandidateAbsenceEvidence == nil
}

func exclusiveRecreateEvidence(result composeStepResult) bool {
	return !result.RestorationRequired && result.ProxyEvidence == nil && result.RecreateEvidence != nil &&
		result.CandidateAbsenceEvidence == nil
}

func exclusiveAbsenceEvidence(result composeStepResult) bool {
	return !result.RestorationRequired && result.ProxyEvidence == nil && result.RecreateEvidence == nil &&
		result.CandidateAbsenceEvidence != nil
}

func invalidReleaseProbeEvidence() error {
	return errs.New(errs.KindInternal, "agent: release recovery probe returned no exact sealed evidence")
}
