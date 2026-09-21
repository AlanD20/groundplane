package agentchannel

import (
	"bytes"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"

	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func durableEnvironmentDirectoryTaskResult(acknowledgement *agentpb.TaskAck) taskjournal.TaskResultRecord {
	return taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultEnvironmentDirectory, ExitCode: acknowledgement.GetExitCode(),
		FailedStepID: acknowledgement.GetEnvironmentDirectoryResult().GetFailedStepId(),
		Diagnostic:   taskjournal.TaskResultDiagnosticNone,
	}
}

func validateEnvironmentDirectoryTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetEnvironmentDirectoryResult()
	if result == nil {
		return errs.New(
			errs.KindValidationFailed,
			"Agent Environment directory Task result is invalid",
		)
	}
	if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
		(acknowledgement.GetExitCode() != 0 || result.GetFailedStepId() != "") {
		return errs.New(
			errs.KindValidationFailed,
			"completed Agent Environment directory Task result is inconsistent",
		)
	}
	return nil
}

func durableComposeTaskResult(acknowledgement *agentpb.TaskAck) taskjournal.TaskResultRecord {
	result := acknowledgement.GetComposeResult()
	diagnostic := taskjournal.TaskResultDiagnosticNone
	switch result.GetDiagnostic() {
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_CONFIG_REJECTED:
		diagnostic = taskjournal.TaskResultDiagnosticConfigRejected
	case agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED:
		diagnostic = taskjournal.TaskResultDiagnosticComposeFailed
	}
	durable := taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultCompose, ExitCode: acknowledgement.GetExitCode(),
		FailedStepID: result.GetFailedStepId(), Diagnostic: diagnostic,
		ReconciliationRequired: result.GetReconciliationRequired(),
		Projects:               make([]taskjournal.TaskObservedProjectSummary, len(result.GetProjects())),
		ProxyEvidence:          make([]taskjournal.TaskProxyEvidence, len(result.GetProxyEvidence())),
		RecreateEvidence:       make([]taskjournal.TaskRecreateEvidence, len(result.GetRecreateEvidence())),
	}
	if evidence := result.GetCandidateAbsenceEvidence(); evidence != nil {
		durable.CandidateAbsenceEvidence = &taskjournal.TaskCandidateAbsenceEvidence{
			AssignmentID: evidence.GetAssignmentId(), PlanHash: hex.EncodeToString(evidence.GetPlanHash()),
			AuthoritySHA256:    hex.EncodeToString(evidence.GetAuthoritySha256()),
			ComposeProjectName: evidence.GetComposeProjectName(), CandidateArtifactID: evidence.GetCandidateArtifactId(),
			AbsenceProven: evidence.GetAbsenceProven(), Candidates: make([]taskjournal.TaskCandidateAbsenceCandidate, len(evidence.GetCandidates())),
		}
		for index, candidate := range evidence.GetCandidates() {
			durable.CandidateAbsenceEvidence.Candidates[index] = taskjournal.TaskCandidateAbsenceCandidate{
				ServiceID: candidate.GetServiceId(), ReleaseID: candidate.GetReleaseId(),
			}
		}
	}
	for index, project := range result.GetProjects() {
		durable.Projects[index] = taskjournal.TaskObservedProjectSummary{
			ProjectName: project.GetProjectName(),
			ObservedAt:  project.GetObservedAt().AsTime().UTC(),
			ContainerCount: uint32(
				len(project.GetContainers()),
			),
			NetworkCount: uint32(len(project.GetNetworks())),
			VolumeCount: uint32(
				len(project.GetVolumes()),
			),
			CollisionCount: uint32(len(project.GetCollisions())),
		}
	}
	for index, evidence := range result.GetProxyEvidence() {
		durable.ProxyEvidence[index] = taskjournal.TaskProxyEvidence{
			ServiceID:       evidence.GetServiceId(),
			Target:          evidence.GetTarget(),
			ProxyGeneration: evidence.GetProxyGeneration(),
			ConfigSHA256:    hex.EncodeToString(evidence.GetConfigSha256()),
			ReleaseID:       evidence.GetReleaseId(),
			Compensated:     evidence.GetCompensated(),
		}
	}
	for index, evidence := range result.GetRecreateEvidence() {
		durable.RecreateEvidence[index] = taskjournal.TaskRecreateEvidence{
			ServiceID:   evidence.GetServiceId(),
			ReleaseID:   evidence.GetReleaseId(),
			ArtifactID:  evidence.GetArtifactId(),
			Compensated: evidence.GetCompensated(),
			Target:      evidence.GetTarget(),
		}
	}
	if evidence := result.GetDnsResolverCandidateObservation(); evidence != nil {
		durable.DNSResolverCandidateObservation = durableDNSResolverObservation(evidence)
	}
	if evidence := result.GetDnsResolverRollbackObservation(); evidence != nil {
		durable.DNSResolverRollbackObservation = durableDNSResolverObservation(evidence)
	}
	return durable
}

func validateComposeTaskResult(acknowledgement *agentpb.TaskAck) error {
	result := acknowledgement.GetComposeResult()
	if result == nil || len(result.GetProjects()) > 64 || len(result.GetProxyEvidence()) > 32 ||
		len(result.GetRecreateEvidence()) > 32 {
		return errs.New(errs.KindValidationFailed, "Agent Compose Task result is invalid")
	}
	if evidence := result.GetCandidateAbsenceEvidence(); evidence != nil {
		if evidence.GetAssignmentId() != acknowledgement.GetAssignmentId() ||
			!bytes.Equal(evidence.GetPlanHash(), acknowledgement.GetPlanHash()) ||
			len(evidence.GetAuthoritySha256()) != sha256.Size || evidence.GetComposeProjectName() == "" ||
			ids.Validate(ids.KindConfig, evidence.GetCandidateArtifactId()) != nil ||
			len(evidence.GetCandidates()) == 0 || len(evidence.GetCandidates()) > 32 {
			return errs.New(errs.KindValidationFailed, "Agent candidate absence evidence is invalid")
		}
		previous := ""
		for _, candidate := range evidence.GetCandidates() {
			identity := candidate.GetServiceId() + "\x00" + candidate.GetReleaseId()
			if ids.Validate(ids.KindService, candidate.GetServiceId()) != nil ||
				ids.Validate(ids.KindDeployment, candidate.GetReleaseId()) != nil || identity <= previous {
				return errs.New(errs.KindValidationFailed, "Agent candidate absence evidence is invalid or unsorted")
			}
			previous = identity
		}
	}
	if !validComposeTaskDiagnostic(result.GetDiagnostic()) {
		return errs.New(errs.KindValidationFailed, "Agent Compose Task diagnostic is invalid")
	}
	if acknowledgement.GetTerminal() == agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED &&
		(acknowledgement.GetExitCode() != 0 || result.GetFailedStepId() != "" ||
			result.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE ||
			result.GetReconciliationRequired()) {
		return errs.New(
			errs.KindValidationFailed,
			"completed Agent Compose Task result is inconsistent",
		)
	}
	for _, project := range result.GetProjects() {
		if project == nil || project.GetProjectName() == "" || project.GetObservedAt() == nil ||
			project.GetObservedAt().CheckValid() != nil || len(project.GetContainers()) > 4096 ||
			len(project.GetNetworks()) > 4096 || len(project.GetVolumes()) > 4096 ||
			len(project.GetCollisions()) > 4096 {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task observation is invalid")
		}
		for _, collision := range project.GetCollisions() {
			if collision == nil ||
				collision.GetKind() == agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_UNSPECIFIED ||
				collision.GetName() == "" {
				return errs.New(
					errs.KindValidationFailed,
					"Agent Compose Task collision is invalid",
				)
			}
		}
	}
	previousServiceID := ""
	for _, evidence := range result.GetProxyEvidence() {
		if evidence == nil || ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			!validAgentReleaseTarget(evidence.GetTarget()) || evidence.GetProxyGeneration() == 0 ||
			len(evidence.GetConfigSha256()) != sha256.Size || evidence.GetReleaseId() == "" ||
			evidence.GetServiceId() <= previousServiceID {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task proxy evidence is invalid or unsorted")
		}
		previousServiceID = evidence.GetServiceId()
	}
	previousServiceID = ""
	for _, evidence := range result.GetRecreateEvidence() {
		if evidence == nil || ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			ids.Validate(ids.KindDeployment, evidence.GetReleaseId()) != nil ||
			ids.Validate(
				ids.KindConfig,
				evidence.GetArtifactId(),
			) != nil || !validAgentReleaseTarget(evidence.GetTarget()) || evidence.GetServiceId() <= previousServiceID {
			return errs.New(errs.KindValidationFailed, "Agent Compose Task recreate evidence is invalid or unsorted")
		}
		previousServiceID = evidence.GetServiceId()
	}
	for _, evidence := range []*agentpb.DNSResolverObservationEvidence{
		result.GetDnsResolverCandidateObservation(),
		result.GetDnsResolverRollbackObservation(),
	} {
		if evidence == nil {
			continue
		}
		staticValid := evidence.GetStaticQuery() == nil
		if static := evidence.GetStaticQuery(); static != nil {
			staticValid = static.GetName() != "" && static.GetType() == agentpb.DNSQueryType_DNS_QUERY_TYPE_A &&
				len(static.GetAnswers()) != 0
		}
		if dnsproof.Verify(evidence) != nil || ids.Validate(ids.KindComponent, evidence.GetComponentId()) != nil ||
			ids.Validate(ids.KindService, evidence.GetServiceId()) != nil ||
			ids.Validate(ids.KindConfig, evidence.GetArtifactId()) != nil ||
			len(evidence.GetArtifactSha256()) != sha256.Size || evidence.GetRenderGeneration() == 0 ||
			!imageref.IsDigestPinned(
				evidence.GetImageReference(),
			) || len(evidence.GetVerifiedImageDigest()) != sha256.Size ||
			len(evidence.GetImageConfigDigest()) != sha256.Size ||
			evidence.GetListenEndpoint() != "127.0.0.1:53" || len(evidence.GetReloadSha512()) != sha512.Size ||
			evidence.GetObservedAt() == nil || evidence.GetObservedAt().CheckValid() != nil || !staticValid ||
			evidence.GetCatchAllQuery() == nil || len(evidence.GetForwarderQueries()) > 8 ||
			len(evidence.GetProofSha256()) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "Agent DNS resolver observation evidence is invalid")
		}
	}
	return nil
}

func validAgentReleaseTarget(value string) bool {
	return value == "singleton" || value == "blue" || value == "green"
}
