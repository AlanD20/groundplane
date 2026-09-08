package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Direct redeploys seal a concrete serving predecessor. Blueprint candidates
// instead use the generic pair whose alternative is selected at claim time.
// Validate the concrete probe/compensation together so one member cannot mix
// recovery families or restore a different predecessor than it observed.
func validateCandidateRestorationPair(
	member *agentpb.CandidateReleaseMember,
	probes, compensations []*agentpb.ExecutionStep,
) error {
	if len(probes) != 1 || len(compensations) != 1 {
		return errs.New(errs.KindValidationFailed, "candidate restoration anchors are not exact execution steps")
	}
	probe, compensate := probes[0], compensations[0]
	if probe.GetCandidateRestorationProbe() != nil || compensate.GetCandidateRestorationCompensate() != nil {
		if err := validateCandidateRestorationAnchor(member, probes, true); err != nil {
			return err
		}
		return validateCandidateRestorationAnchor(member, compensations, false)
	}
	if member.GetServingPredecessor() == nil || member.GetCandidateAbsence() != nil ||
		probe.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
		compensate.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		return errs.New(errs.KindValidationFailed, "serving restoration policy or alternative is invalid")
	}
	bound := func(artifactID, serviceID, releaseID string) bool {
		return artifactID == member.GetCandidateArtifactId() && serviceID == member.GetServiceId() &&
			releaseID == member.GetCandidateReleaseId()
	}
	switch payload := probe.GetPayload().(type) {
	case *agentpb.ExecutionStep_ServiceRecreateProbe:
		value, recovery := payload.ServiceRecreateProbe, compensate.GetServiceRecreateCompensate()
		if value != nil && recovery != nil &&
			bound(value.CandidateArtifactId, value.ServiceId, value.CandidateReleaseId) &&
			bound(recovery.CandidateArtifactId, recovery.ServiceId, recovery.CandidateReleaseId) &&
			value.PriorArtifactId == recovery.ArtifactId && value.PriorReleaseId == recovery.PriorReleaseId {
			return nil
		}
	case *agentpb.ExecutionStep_ServiceProxyProbe:
		value, recovery := payload.ServiceProxyProbe, compensate.GetServiceProxyCompensate()
		// Proxy compensation has no separate candidate Release field. Its exact
		// candidate artifact/Service is already bound to that Release by the
		// enclosing procedure; its target must also equal the probe's candidate.
		if value != nil && recovery != nil &&
			bound(value.CandidateArtifactId, value.ServiceId, value.AlternateReleaseId) &&
			recovery.CandidateArtifactId == value.CandidateArtifactId && recovery.ServiceId == value.ServiceId &&
			recovery.CandidateTarget == value.AlternateTarget && recovery.PriorTarget == value.ExpectedTarget &&
			recovery.PriorArtifactId == value.PriorArtifactId && recovery.PriorReleaseId == value.ReleaseId &&
			recovery.ProxyGeneration == value.ProxyGeneration &&
			bytes.Equal(recovery.ConfigJson, value.ConfigJson) && bytes.Equal(recovery.ConfigSha256, value.ConfigSha256) {
			return nil
		}
	}
	return errs.New(errs.KindValidationFailed, "serving restoration pair does not bind its procedure member")
}

func candidateRestorationStep(step *agentpb.ExecutionStep) bool {
	return step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil ||
		step.GetServiceRecreateProbe() != nil || step.GetServiceRecreateCompensate() != nil ||
		step.GetServiceProxyProbe() != nil || step.GetServiceProxyCompensate() != nil
}
