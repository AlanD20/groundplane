package composehelper

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
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
	case agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED:
		if response.ExitCode != 0 ||
			response.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
			response.ProxyEvidence != nil ||
			response.RecreateEvidence != nil ||
			response.CandidateAbsenceEvidence != nil {
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
		if response.ProxyEvidence != nil || response.RecreateEvidence != nil ||
			response.CandidateAbsenceEvidence != nil {
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
