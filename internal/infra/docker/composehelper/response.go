package composehelper

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validateResponse(response *agentpb.ComposeHelperResponse) error {
	if response == nil || response.Schema != SchemaVersion {
		return errs.New(errs.KindValidationFailed, "Compose helper response schema is unsupported")
	}
	if err := executionplan.RejectUnknown(response); err != nil {
		return err
	}
	evidenceCount := 0
	for _, present := range []bool{response.ProxyEvidence != nil, response.RecreateEvidence != nil,
		response.CandidateAbsenceEvidence != nil, response.RollingEvidence != nil, response.RollingProxyEvidence != nil,
		response.ComponentRestorationEvidence != nil} {
		if present {
			evidenceCount++
		}
	}
	if evidenceCount > 1 {
		return errs.New(errs.KindValidationFailed, "Compose helper response carries conflicting release evidence")
	}
	switch response.Outcome {
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED:
		if response.ExitCode != 0 ||
			response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
			return errs.New(errs.KindValidationFailed, "completed Compose helper response is inconsistent")
		}
		if response.ProxyEvidence != nil && (response.ProxyEvidence.ServiceId == "" ||
			!validRuntimeTarget(response.ProxyEvidence.Target) ||
			response.ProxyEvidence.ProxyGeneration == 0 || len(response.ProxyEvidence.ConfigSha256) != sha256.Size ||
			response.ProxyEvidence.ReleaseId == "") {
			return errs.New(errs.KindValidationFailed, "completed Compose helper proxy evidence is invalid")
		}
		if response.RecreateEvidence != nil &&
			(response.RecreateEvidence.ServiceId == "" || response.RecreateEvidence.ReleaseId == "" || response.RecreateEvidence.ArtifactId == "" || !validRuntimeTarget(response.RecreateEvidence.Target)) {
			return errs.New(errs.KindValidationFailed, "completed Compose helper recreate evidence is invalid")
		}
		if evidence := response.GetCandidateAbsenceEvidence(); evidence != nil {
			if evidence.GetAssignmentId() == "" || len(evidence.GetPlanHash()) != sha256.Size ||
				len(evidence.GetAuthoritySha256()) != sha256.Size || evidence.GetComposeProjectName() == "" ||
				evidence.GetCandidateArtifactId() == "" || len(evidence.GetCandidates()) == 0 {
				return errs.New(errs.KindValidationFailed, "completed candidate absence evidence is invalid")
			}
		}
		if evidence := response.GetRollingEvidence(); evidence != nil {
			if !evidence.GetCompensated() ||
				rollingrelease.ValidateCompletion(executionplan.RollingCompletion(evidence)) != nil {
				return errs.New(
					errs.KindValidationFailed,
					"completed Compose helper rolling restoration evidence is invalid",
				)
			}
		}
		if evidence := response.GetRollingProxyEvidence(); evidence != nil &&
			(evidence.GetServiceId() == "" || evidence.GetChangeOrdinal() == 0 || evidence.GetProxyGeneration() == 0 ||
				len(evidence.GetConfigSha256()) != sha256.Size) {
			return errs.New(errs.KindValidationFailed, "completed Compose helper rolling proxy evidence is invalid")
		}
		if evidence := response.GetComponentRestorationEvidence(); evidence != nil &&
			(evidence.GetComponentId() == "" || evidence.GetArtifactId() == "" || evidence.GetServiceId() == "" ||
				evidence.GetComposeProjectName() == "" || evidence.GetTarget() != agentpb.ComponentRestorationTarget_COMPONENT_RESTORATION_TARGET_PREDECESSOR) {
			return errs.New(
				errs.KindValidationFailed,
				"completed Component configuration restoration evidence is invalid",
			)
		}
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED:
		if response.ExitCode != 0 ||
			response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
			evidenceCount != 0 {
			return errs.New(errs.KindValidationFailed, "unrestored Compose helper observation is inconsistent")
		}
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED:
		if response.ExitCode <= 0 ||
			(response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED &&
				response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED &&
				response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED &&
				response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED) {
			return errs.New(errs.KindValidationFailed, "failed Compose helper response is inconsistent")
		}
		if evidenceCount != 0 {
			return errs.New(errs.KindValidationFailed, "failed Compose helper response carries release evidence")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Compose helper response outcome is unsupported")
	}
	return nil
}

func validRuntimeTarget(value string) bool {
	return value == "singleton" || value == "blue" || value == "green"
}
