package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func executeServingPredecessor(
	ctx context.Context,
	taskRunner runner.Runner,
	timeoutSeconds uint32,
	request *agentpb.ComposeHelperRequest,
	candidate *agentpb.ComposeArtifact,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeHelperResponse, error) {
	predecessor, service, releaseID, target, err := openServingPredecessor(request, candidate, step)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	proxy, err := servingPredecessorProxy(predecessor, service.GetServiceId())
	if err != nil {
		return nil, err
	}
	proven, probeErr := servingPredecessorProven(executionCtx, taskRunner, predecessor, service)
	if probeErr == nil && proven && proxy != nil {
		proven, probeErr = servingPredecessorProven(executionCtx, taskRunner, predecessor, proxy)
	}
	if step.GetCandidateRestorationCompensate() != nil && (probeErr != nil || !proven) {
		base := []string{
			"compose", "--project-name", predecessor.GetProjectName(), "--project-directory",
			composeProjectDirectory(predecessor), "--file", "-",
		}
		for index, suffix := range [][]string{{"config", "--quiet"}, {"up", "--detach", "--remove-orphans"}} {
			result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: append(append([]string(nil), base...), suffix...),
				Dir: composeProjectDirectory(predecessor), Env: append([]string(nil), fixedEnvironment...),
				ReplaceEnv: true, Stdin: append([]byte(nil), predecessor.GetCanonicalYaml()...),
			})
			if runErr != nil || result.ExitCode != 0 {
				diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
				if index == 0 {
					diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED
				}
				return &agentpb.ComposeHelperResponse{Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 1, Diagnostic: diagnostic}, nil
			}
		}
		proven, probeErr = servingPredecessorProven(executionCtx, taskRunner, predecessor, service)
		if probeErr == nil && proven && proxy != nil {
			proven, probeErr = servingPredecessorProven(executionCtx, taskRunner, predecessor, proxy)
		}
	}
	if probeErr != nil || !proven {
		return failedResponse(1), nil
	}
	if proxy != nil {
		generation, generationErr := executionplan.ProxyConfigGeneration(proxy.GetProxyConfigJson(), releaseID)
		if generationErr != nil {
			return nil, generationErr
		}
		return &agentpb.ComposeHelperResponse{
			Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			ProxyEvidence: &agentpb.ServiceProxyEvidence{
				ServiceId: service.GetServiceId(), Target: target, ProxyGeneration: generation,
				ConfigSha256: slices.Clone(proxy.GetProxyConfigSha256()), ReleaseId: releaseID, Compensated: true,
			},
		}, nil
	}
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		RecreateEvidence: &agentpb.ServiceRecreateEvidence{
			ServiceId: service.GetServiceId(), ReleaseId: releaseID, ArtifactId: predecessor.GetArtifactId(),
			Compensated: true, Target: target,
		},
	}, nil
}

func servingPredecessorProxy(artifact *agentpb.ComposeArtifact, serviceID string) (*agentpb.ComposeService, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID || service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if selected != nil {
			return nil, errs.New(errs.KindStateConflict, "serving predecessor proxy is ambiguous")
		}
		selected = service
	}
	return selected, nil
}

func openServingPredecessor(
	request *agentpb.ComposeHelperRequest,
	candidate *agentpb.ComposeArtifact,
	step *agentpb.ExecutionStep,
) (*agentpb.ComposeArtifact, *agentpb.ComposeService, string, string, error) {
	authority := request.GetRestorationAuthority()
	sealed := authority.GetServingPredecessor()
	if authority.GetTarget() != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR ||
		sealed == nil || len(sealed.GetComposeArtifact()) == 0 || len(sealed.GetComposeArtifactSha256()) != sha256.Size {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor authority is incomplete")
	}
	digest := sha256.Sum256(sealed.GetComposeArtifact())
	if !bytes.Equal(digest[:], sealed.GetComposeArtifactSha256()) {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor artifact digest diverges")
	}
	predecessor := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(sealed.GetComposeArtifact(), predecessor) != nil || executionplan.RejectUnknown(predecessor) != nil {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor artifact is invalid")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(predecessor)
	if err != nil || !bytes.Equal(canonical, sealed.GetComposeArtifact()) || predecessor.GetProjectName() == "" ||
		candidate == nil || predecessor.GetProjectName() != candidate.GetProjectName() {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor artifact is not canonical")
	}
	serviceID := ""
	if probe := step.GetCandidateRestorationProbe(); probe != nil {
		serviceID = probe.GetServiceId()
	} else if compensate := step.GetCandidateRestorationCompensate(); compensate != nil {
		serviceID = compensate.GetServiceId()
	}
	var selected *agentpb.ComposeService
	for _, service := range predecessor.GetServices() {
		if service.GetServiceId() != serviceID || service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			if expectedLabel(service.GetExpectedLabels(), "com.groundplane.release-id") == "" {
				continue
			}
			if selected != nil {
				return nil, nil, "", "", errs.New(errs.KindStateConflict, "serving predecessor service is ambiguous")
			}
			selected = service
		}
	}
	if selected == nil {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor service is absent")
	}
	releaseID := expectedLabel(selected.GetExpectedLabels(), "com.groundplane.release-id")
	target := selected.GetSlot()
	if target == "" {
		target = "singleton"
	}
	if releaseID == "" || !validRuntimeTarget(target) {
		return nil, nil, "", "", errs.New(errs.KindValidationFailed, "serving predecessor identity is incomplete")
	}
	return predecessor, selected, releaseID, target, nil
}

func servingPredecessorProven(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	service *agentpb.ComposeService,
) (bool, error) {
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{"container", "ls", "--all", "--filter", "label=com.docker.compose.project=" + artifact.GetProjectName(), "--format", "{{.ID}}"},
		Dir:  WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "serving predecessor observation failed")
	}
	count := uint32(0)
	for _, containerID := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(containerID) {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container identity is invalid")
		}
		inspected, inspectErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if inspectErr != nil || inspected.ExitCode != 0 || json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
			labels["com.docker.compose.project"] != artifact.GetProjectName() {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container evidence is ambiguous")
		}
		if labels["com.groundplane.service-id"] != service.GetServiceId() {
			continue
		}
		expectedRole := expectedLabel(service.GetExpectedLabels(), "com.groundplane.role")
		if expectedRole != "" && labels["com.groundplane.role"] != expectedRole {
			continue
		}
		expectedSlot := expectedLabel(service.GetExpectedLabels(), "com.groundplane.slot")
		if expectedSlot != "" && labels["com.groundplane.slot"] != expectedSlot {
			continue
		}
		if !hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
			return false, errs.New(errs.KindStateConflict, "serving predecessor observed foreign service state")
		}
		state, stateErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .State}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		observed := struct {
			Running bool `json:"Running"`
			Health  *struct {
				Status string `json:"Status"`
			} `json:"Health"`
		}{}
		if stateErr != nil || state.ExitCode != 0 || json.Unmarshal([]byte(strings.TrimSpace(string(state.Stdout))), &observed) != nil ||
			!observed.Running || service.GetHasHealthcheck() && (observed.Health == nil || observed.Health.Status != "healthy") {
			return false, errs.New(errs.KindStateConflict, "serving predecessor workload is not running and healthy")
		}
		count++
	}
	if count != service.GetExpectedReplicas() {
		return false, errs.New(errs.KindStateConflict, "serving predecessor workload count is ambiguous")
	}
	return true, nil
}

func expectedLabel(labels []*agentpb.LabelPair, key string) string {
	for _, label := range labels {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}
