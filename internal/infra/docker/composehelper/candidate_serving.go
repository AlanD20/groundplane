package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
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
	serving, err := openServingPredecessor(request, candidate, step)
	if err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
	defer cancel()
	predecessor, services, proxy := serving.Artifact, serving.Workloads, serving.Proxy
	serviceID, releaseID, target := restorationStepServiceID(step), serving.ReleaseID, serving.Target
	retained := &agentpb.ComposeArtifact{}
	for _, witness := range request.GetRestorationAuthority().GetNativePredecessors() {
		if witness.GetServiceId() == serviceID && len(witness.GetRetainedPriorArtifact()) != 0 {
			if err := proto.Unmarshal(witness.GetRetainedPriorArtifact(), retained); err != nil {
				return nil, errs.Wrap(errs.KindValidationFailed, err)
			}
		}
	}
	complete := proto.CloneOf(predecessor)
	complete.Services = append(complete.Services, retained.GetServices()...)
	inventory, err := inspectServingInventory(executionCtx, taskRunner, complete, candidate,
		serviceID, restorationStepCandidateReleaseID(step), proxy)
	if err != nil {
		return failedResponse(1), nil
	}
	// Rolling compensation reverses traffic before retiring its candidates. The
	// ordinary absence path below is safe only after that handoff is proven.
	if step.GetCandidateRestorationCompensate() != nil && proxy != nil && inventory.proxyID != "" {
		for _, member := range request.GetPlan().GetCandidateReleaseProcedure().GetMembers() {
			if member.GetServiceId() == serviceID && member.GetRolling() != nil {
				if err := restoreRollingTraffic(executionCtx, taskRunner, complete, candidate, serving, member); err != nil {
					return failedProxyResponse(), nil
				}
				inventory, err = inspectServingInventory(executionCtx, taskRunner, complete, candidate,
					serviceID, restorationStepCandidateReleaseID(step), proxy)
				if err != nil {
					return failedResponse(1), nil
				}
				break
			}
		}
	}
	if len(inventory.candidates) != 0 {
		if step.GetCandidateRestorationCompensate() == nil {
			return restorationRequiredResponse(), nil
		}
		removed, removeErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: append([]string{"container", "rm", "--force", "--"}, inventory.candidates...),
			Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
		})
		if removeErr != nil || removed.ExitCode != 0 {
			return failedResponse(1), nil
		}
		inventory, err = inspectServingInventory(executionCtx, taskRunner, complete, candidate,
			serviceID, restorationStepCandidateReleaseID(step), proxy)
		if err != nil || len(inventory.candidates) != 0 {
			return failedResponse(1), nil
		}
	}
	var proven bool
	var probeErr error
	if inventory.proxyNeedsRestore {
		_, probeErr = servingMemberProven(executionCtx, taskRunner, predecessor, services, nil)
	} else {
		proven, probeErr = servingMemberProven(executionCtx, taskRunner, predecessor, services, proxy)
	}
	if probeErr != nil {
		return failedResponse(1), nil
	}
	if step.GetCandidateRestorationCompensate() != nil && !proven {
		input, err := composeCLIInput(predecessor.GetCanonicalYaml())
		if err != nil {
			return nil, err
		}
		names := servingPredecessorNames(services, proxy)
		base := []string{
			"compose", "--project-name", predecessor.GetProjectName(), "--project-directory",
			composeProjectDirectory(predecessor), "--file", "-",
		}
		up := append([]string{"up", "--detach", "--no-deps", "--"}, names...)
		for index, suffix := range [][]string{{"config", "--quiet"}, up} {
			result, runErr := taskRunner.Run(executionCtx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: append(append([]string(nil), base...), suffix...),
				Dir: composeProjectDirectory(predecessor), Env: append([]string(nil), fixedEnvironment...),
				ReplaceEnv: true, Stdin: input,
			})
			if runErr != nil || result.ExitCode != 0 {
				diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
				if index == 0 {
					diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_CONFIG_REJECTED
				}
				return &agentpb.ComposeHelperResponse{
					Schema:     SchemaVersion,
					Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
					ExitCode:   1,
					Diagnostic: diagnostic,
				}, nil
			}
		}
		proven, probeErr = waitServingMember(executionCtx, taskRunner, predecessor, services, proxy)
	}
	for _, retainedService := range retained.GetServices() {
		retainedProven, retainedErr := servingPredecessorProven(executionCtx, taskRunner, retained, retainedService)
		if retainedErr != nil {
			return failedResponse(1), nil
		}
		if !retainedProven && step.GetCandidateRestorationCompensate() != nil {
			input, err := composeCLIInput(retained.GetCanonicalYaml())
			if err != nil {
				return nil, err
			}
			base := []string{"compose", "--project-name", retained.GetProjectName(), "--project-directory",
				composeProjectDirectory(retained), "--file", "-"}
			for _, suffix := range [][]string{{"config", "--quiet"}, {"up", "--detach", "--no-deps", "--", retainedService.GetComposeName()}} {
				result, err := taskRunner.Run(executionCtx, runner.RunCmdOpts{
					Name: DockerExecutable, Args: append(slices.Clone(base), suffix...),
					Dir: composeProjectDirectory(retained), Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
					Stdin: input,
				})
				if err != nil || result.ExitCode != 0 {
					return failedResponse(1), nil
				}
			}
			retainedProven, retainedErr = waitServingMember(executionCtx, taskRunner, retained,
				[]*agentpb.ComposeService{retainedService}, nil)
		}
		proven = proven && retainedProven
		if retainedErr != nil {
			return failedResponse(1), nil
		}
	}
	if probeErr == nil && !proven && step.GetCandidateRestorationProbe() != nil {
		return restorationRequiredResponse(), nil
	}
	if probeErr != nil || !proven {
		return failedResponse(1), nil
	}
	inventory, err = inspectServingInventory(executionCtx, taskRunner, complete, candidate,
		serviceID, restorationStepCandidateReleaseID(step), proxy)
	if err != nil || len(inventory.candidates) != 0 || inventory.proxyNeedsRestore {
		return failedResponse(1), nil
	}
	if proxy != nil {
		proven, err := restorationProxyProven(
			executionCtx,
			taskRunner,
			inventory.proxyID,
			proxy,
			step.GetCandidateRestorationCompensate() != nil,
		)
		if err == nil && !proven && step.GetCandidateRestorationProbe() != nil {
			return restorationRequiredResponse(), nil
		}
		if err != nil || !proven {
			return failedProxyResponse(), nil
		}
		if serving.Rolling != nil {
			return restoredRollingResponse(serving.Rolling), nil
		}
		generation, generationErr := executionplan.ProxyConfigGeneration(
			proxy.GetProxyImplementation(),
			proxy.GetProxyConfigJson(),
			releaseID,
		)
		if generationErr != nil {
			return nil, generationErr
		}
		return &agentpb.ComposeHelperResponse{
			Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			ProxyEvidence: &agentpb.ServiceProxyEvidence{
				ServiceId: serviceID, Target: target, ProxyGeneration: generation,
				ConfigSha256: slices.Clone(proxy.GetProxyConfigSha256()), ReleaseId: releaseID, Compensated: true,
			},
		}, nil
	}
	if serving.Rolling != nil {
		return restoredRollingResponse(serving.Rolling), nil
	}
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		RecreateEvidence: &agentpb.ServiceRecreateEvidence{
			ServiceId: serviceID, ReleaseId: releaseID, ArtifactId: predecessor.GetArtifactId(),
			Compensated: true, Target: target,
		},
	}, nil
}

func restoredRollingResponse(evidence *agentpb.RollingReleaseEvidence) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{Schema: SchemaVersion,
		Outcome:         agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic:      agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		RollingEvidence: proto.CloneOf(evidence)}
}

func restorationRequiredResponse() *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Schema: SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_RESTORATION_REQUIRED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}
}

func servingMemberProven(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	services []*agentpb.ComposeService,
	proxy *agentpb.ComposeService,
) (bool, error) {
	proven := true
	for _, service := range services {
		ready, err := servingPredecessorProven(ctx, taskRunner, artifact, service)
		if err != nil {
			return false, err
		}
		proven = proven && ready
	}
	if proxy == nil {
		return proven, nil
	}
	proxyProven, err := servingPredecessorProven(ctx, taskRunner, artifact, proxy)
	return proven && proxyProven, err
}

func servingPredecessorNames(services []*agentpb.ComposeService, proxy *agentpb.ComposeService) []string {
	names := make([]string, 0, len(services)+1)
	for _, service := range services {
		names = append(names, service.GetComposeName())
	}
	if proxy != nil {
		names = append(names, proxy.GetComposeName())
	}
	return names
}

// Newly restored workloads may still be starting. Readiness consumes the same
// compensation deadline; it never starts another Compose action or renews time.
func waitServingMember(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact,
	services []*agentpb.ComposeService, proxy *agentpb.ComposeService,
) (bool, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		proven, err := servingMemberProven(ctx, taskRunner, artifact, services, proxy)
		if err != nil || proven {
			return proven, err
		}
		select {
		case <-ctx.Done():
			return false, errs.Wrap(errs.KindRequestFailed, ctx.Err())
		case <-ticker.C:
		}
	}
}

func servingPredecessorProxy(artifact *agentpb.ComposeArtifact, serviceID string) (*agentpb.ComposeService, error) {
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() != serviceID ||
			service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
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
) (executionplan.NativeServingRuntime, error) {
	authority := request.GetRestorationAuthority()
	serviceID := restorationStepServiceID(step)
	if executionplan.RestorationTargetForService(
		authority,
		serviceID,
	) != agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
		return executionplan.NativeServingRuntime{}, errs.New(
			errs.KindValidationFailed,
			"serving predecessor authority is incomplete",
		)
	}
	var nativeWitness *agentpb.ReleaseNativePredecessorAuthority
	for _, witness := range authority.GetNativePredecessors() {
		if witness.GetServiceId() != serviceID {
			continue
		}
		if nativeWitness != nil {
			return executionplan.NativeServingRuntime{}, errs.New(
				errs.KindStateConflict,
				"native serving predecessor witness is duplicated",
			)
		}
		nativeWitness = witness
	}
	if nativeWitness == nil {
		return executionplan.NativeServingRuntime{}, errs.New(
			errs.KindValidationFailed,
			"native serving predecessor witness is absent",
		)
	}
	serving, err := executionplan.OpenNativeServingRuntime(authority.GetEnvironmentId(), serviceID,
		nativeWitness.GetCurrentArtifact(), nativeWitness.GetRetainedPriorArtifact())
	if err != nil {
		return executionplan.NativeServingRuntime{}, err
	}
	if candidate == nil || serving.Artifact.GetProjectName() != candidate.GetProjectName() {
		return executionplan.NativeServingRuntime{}, errs.New(
			errs.KindValidationFailed,
			"serving predecessor Compose project differs from candidate",
		)
	}
	candidateReleaseID := restorationStepCandidateReleaseID(step)
	if ids.Validate(ids.KindDeployment, candidateReleaseID) != nil {
		return executionplan.NativeServingRuntime{}, errs.New(
			errs.KindValidationFailed,
			"serving restoration candidate Release is invalid",
		)
	}
	for _, member := range authority.GetCandidates() {
		if member.GetServiceId() == serviceID && member.GetReleaseId() != candidateReleaseID {
			return executionplan.NativeServingRuntime{}, errs.New(
				errs.KindValidationFailed,
				"serving restoration candidate Release diverges",
			)
		}
	}
	if serving.ReleaseID == candidateReleaseID {
		return executionplan.NativeServingRuntime{}, errs.New(
			errs.KindValidationFailed,
			"serving predecessor is the candidate Release",
		)
	}
	return serving, nil
}

func restorationStepCandidateReleaseID(step *agentpb.ExecutionStep) string {
	if probe := step.GetCandidateRestorationProbe(); probe != nil {
		return probe.GetCandidateReleaseId()
	}
	return step.GetCandidateRestorationCompensate().GetCandidateReleaseId()
}

func restorationStepServiceID(step *agentpb.ExecutionStep) string {
	if probe := step.GetCandidateRestorationProbe(); probe != nil {
		return probe.GetServiceId()
	}
	return step.GetCandidateRestorationCompensate().GetServiceId()
}

func servingPredecessorProven(
	ctx context.Context,
	taskRunner runner.Runner,
	artifact *agentpb.ComposeArtifact,
	service *agentpb.ComposeService,
) (bool, error) {
	expectedRole, roleErr := sealedServingPredecessorRuntimeRole(service)
	workloadWithoutImage := service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY &&
		service.GetImageReference() == ""
	if roleErr != nil || workloadWithoutImage {
		return false, errs.New(errs.KindValidationFailed, "serving predecessor runtime identity is incomplete")
	}
	list, err := taskRunner.Run(ctx, runner.RunCmdOpts{
		Name: DockerExecutable,
		Args: []string{
			"container",
			"ls",
			"--all",
			"--filter",
			"label=com.docker.compose.project=" + artifact.GetProjectName(),
			"--format",
			"{{.ID}}",
		},
		Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
	})
	if err != nil || list.ExitCode != 0 {
		return false, errs.New(errs.KindRequestFailed, "serving predecessor observation failed")
	}
	count := uint32(0)
	healthy := true
	seen := make(map[string]bool)
	for _, containerID := range strings.Fields(string(list.Stdout)) {
		if !validComponentContainerID(containerID) || seen[containerID] {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container identity is invalid")
		}
		seen[containerID] = true
		inspected, inspectErr := taskRunner.Run(ctx, runner.RunCmdOpts{
			Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", containerID},
			Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		})
		labels := map[string]string{}
		if inspectErr != nil || inspected.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(inspected.Stdout))), &labels) != nil ||
			labels["com.docker.compose.project"] != artifact.GetProjectName() {
			return false, errs.New(errs.KindStateConflict, "serving predecessor container evidence is ambiguous")
		}
		if labels["com.groundplane.service-id"] != service.GetServiceId() {
			continue
		}
		if labels["com.groundplane.runtime-role"] != expectedRole {
			continue
		}
		if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA &&
			labels["com.docker.compose.service"] != service.GetComposeName() {
			continue // The complete inventory separately rejects uncaptured replicas.
		}
		expectedSlot := expectedLabel(service.GetExpectedLabels(), "com.groundplane.slot")
		if expectedSlot != "" && labels["com.groundplane.slot"] != expectedSlot {
			continue
		}
		if !hasAllExpectedLabels(labels, service.GetExpectedLabels()) {
			return false, errs.New(errs.KindStateConflict, "serving predecessor observed foreign service state")
		}
		if service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			image, imageErr := taskRunner.Run(ctx, runner.RunCmdOpts{
				Name: DockerExecutable, Args: []string{"container", "inspect", "--format", "{{.Config.Image}}", containerID},
				Dir: WorkDirectory, Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
			})
			if imageErr != nil || image.ExitCode != 0 ||
				strings.TrimSpace(string(image.Stdout)) != service.GetImageReference() {
				return false, errs.New(errs.KindStateConflict, "serving predecessor workload image diverges")
			}
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
		if stateErr != nil || state.ExitCode != 0 ||
			json.Unmarshal([]byte(strings.TrimSpace(string(state.Stdout))), &observed) != nil {
			return false, errs.New(errs.KindStateConflict, "serving predecessor workload state is ambiguous")
		}
		if !observed.Running ||
			service.GetHasHealthcheck() && (observed.Health == nil || observed.Health.Status != "healthy") {
			healthy = false
		}
		count++
	}
	if count > service.GetExpectedReplicas() {
		return false, errs.New(errs.KindStateConflict, "serving predecessor workload count is ambiguous")
	}
	return count == service.GetExpectedReplicas() && healthy, nil
}

func sealedServingPredecessorRuntimeRole(service *agentpb.ComposeService) (string, error) {
	role := ""
	switch service.GetRole() {
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON:
		role = "singleton"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT:
		role = "slot"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_ROLLING_REPLICA:
		role = "replica"
	case agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY:
		role = "proxy"
	default:
		return "", errs.New(errs.KindValidationFailed, "serving predecessor runtime role is unsupported")
	}
	if expectedLabel(service.GetExpectedLabels(), "com.groundplane.runtime-role") != role {
		return "", errs.New(errs.KindValidationFailed, "serving predecessor runtime role diverges")
	}
	return role, nil
}

func expectedLabel(labels []*agentpb.LabelPair, key string) string {
	for _, label := range labels {
		if label.GetKey() == key {
			return label.GetValue()
		}
	}
	return ""
}
