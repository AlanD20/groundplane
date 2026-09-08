package composehelper

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func executeCandidateAbsence(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	request *agentpb.ComposeHelperRequest,
	artifact *agentpb.ComposeArtifact,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, error) {
	authority := request.GetRestorationAuthority()
	projectName, err := validateCandidateAbsenceRequest(request, artifact, step, authority)
	if err != nil {
		return nil, err
	}
	var selected *agentpb.ReleaseRestorationCandidate
	for _, member := range authority.GetCandidates() {
		if member.GetServiceId() == restorationStepServiceID(step) {
			selected = member
		}
	}
	if selected == nil {
		return nil, errs.New(errs.KindValidationFailed, "candidate absence member is missing")
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	containers, err := exactCandidateContainers(executionCtx, taskRunner, artifact, selected, projectName)
	if err != nil {
		return failedResponse(1), nil
	}
	if step.GetCandidateRestorationCompensate() != nil && len(containers) != 0 {
		result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: append([]string{"container", "rm", "--force", "--"}, containers...),
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		if runErr != nil || result.ExitCode != 0 {
			return failedResponse(1), nil
		}
		containers, err = exactCandidateContainers(executionCtx, taskRunner, artifact, selected, projectName)
		if err != nil || len(containers) != 0 {
			return failedResponse(1), nil
		}
	}
	evidence := &agentpb.CandidateAbsenceEvidence{
		AssignmentId: request.GetAssignmentId(), PlanHash: append([]byte(nil), request.GetPlan().GetPlanHash()...),
		AuthoritySha256: append([]byte(nil), authority.GetAuthoritySha256()...), ComposeProjectName: projectName,
		CandidateArtifactId: authority.GetCandidateArtifactId(), AbsenceProven: len(containers) == 0,
		Candidates: []*agentpb.CandidateReleaseService{
			{ServiceId: selected.GetServiceId(), ReleaseId: selected.GetReleaseId()},
		},
	}
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic:               agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		CandidateAbsenceEvidence: evidence,
	}, nil
}

func validateCandidateAbsenceRequest(
	request *agentpb.ComposeHelperRequest,
	artifact *agentpb.ComposeArtifact,
	step *agentpb.ExecutionStep,
	authority *agentpb.ReleaseRestorationAuthority,
) (string, error) {
	if artifact == nil || authority == nil ||
		executionplan.RestorationTargetForService(
			authority,
			restorationStepServiceID(step),
		) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_CANDIDATE_ABSENCE ||
		authority.GetTaskId() != request.GetTaskId() || authority.GetOperationId() != request.GetOperationId() ||
		authority.GetCandidateArtifactId() != artifact.GetArtifactId() ||
		len(authority.GetPlanHash()) != 32 || !bytes.Equal(authority.GetPlanHash(), request.GetPlan().GetPlanHash()) ||
		len(authority.GetAuthoritySha256()) != 32 || len(authority.GetCandidates()) == 0 {
		return "", errs.New(errs.KindValidationFailed, "candidate absence authority is invalid")
	}
	serviceID, releaseID := "", ""
	if probe := step.GetCandidateRestorationProbe(); probe != nil {
		serviceID, releaseID = probe.GetServiceId(), probe.GetCandidateReleaseId()
	} else if compensate := step.GetCandidateRestorationCompensate(); compensate != nil {
		serviceID, releaseID = compensate.GetServiceId(), compensate.GetCandidateReleaseId()
	}
	for _, member := range authority.GetCandidates() {
		if member.GetServiceId() == serviceID && member.GetReleaseId() != releaseID {
			return "", errs.New(errs.KindValidationFailed, "candidate absence member release diverges")
		}
	}
	projectName := ""
	for _, member := range request.GetPlan().GetCandidateReleaseProcedure().GetMembers() {
		if member.GetServiceId() == serviceID && member.GetCandidateReleaseId() == releaseID &&
			member.GetCandidateArtifactId() == artifact.GetArtifactId() && member.GetCandidateAbsence() != nil {
			projectName = member.GetCandidateAbsence().GetComposeProjectName()
			break
		}
	}
	if projectName == "" || projectName != artifact.GetProjectName() {
		return "", errs.New(errs.KindValidationFailed, "candidate absence project authority is invalid")
	}
	return projectName, nil
}

func exactCandidateContainers(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	candidate *agentpb.ReleaseRestorationCandidate,
	projectName string,
) ([]string, error) {
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{
			"container",
			"ls",
			"--all",
			"--filter",
			"label=com.docker.compose.project=" + projectName,
			"--format",
			"{{.ID}}",
		},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return nil, errs.New(errs.KindRequestFailed, "candidate absence observation failed")
	}
	found := make([]string, 0)
	for _, containerID := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(containerID) {
			return nil, errs.New(errs.KindStateConflict, "candidate absence observed an invalid container identity")
		}
		inspected, inspectErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if inspectErr != nil || inspected.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
			labels["com.docker.compose.project"] != projectName {
			return nil, errs.New(errs.KindStateConflict, "candidate absence container evidence is ambiguous")
		}
		if labels["com.groundplane.service-id"] != candidate.GetServiceId() {
			continue
		}
		matched := false
		for _, service := range artifact.GetServices() {
			releaseMatches := labels["com.groundplane.release-id"] == candidate.GetReleaseId()
			if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				releaseMatches = labels["com.groundplane.release-id"] == "" &&
					expectedLabel(service.GetExpectedLabels(), "com.groundplane.release-id") == ""
			}
			if service.GetServiceId() == labels["com.groundplane.service-id"] &&
				releaseMatches && hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
				matched = true
				break
			}
		}
		if !matched {
			return nil, errs.New(errs.KindStateConflict, "candidate absence observed foreign or stale candidate state")
		}
		found = append(found, containerID)
	}
	return found, nil
}
