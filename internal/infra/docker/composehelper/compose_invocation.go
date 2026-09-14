package composehelper

import (
	"sort"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	WorkDirectory    = "/run/groundplane/compose"
	DockerExecutable = "docker"
)

var fixedEnvironment = []string{
	"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	"HOME=/nonexistent",
	"DOCKER_HOST=unix:///var/run/docker.sock",
	"COMPOSE_DISABLE_ENV_FILE=1",
	"COMPOSE_PARALLEL_LIMIT=1",
}

func commandsFor(
	request *agentpb.ComposeHelperRequest,
	step *agentpb.ExecutionStep,
	artifact *agentpb.ComposeArtifact,
) ([]runner.RunCmdOpts, error) {
	prefix := []string{
		"compose", "--project-name", artifact.ProjectName,
		"--project-directory", composeProjectDirectory(artifact), "--file", "-",
	}
	base := runner.RunCmdOpts{
		Name: DockerExecutable, Dir: WorkDirectory,
		Env: append([]string(nil), fixedEnvironment...), ReplaceEnv: true,
		Stdin: append([]byte(nil), artifact.CanonicalYaml...),
	}
	var commands []runner.RunCmdOpts
	if apply := step.GetComposeApply(); apply != nil {
		names, err := applyServiceNames(request.Plan, step, artifact)
		_, attachSelection, attachErr := executionplan.AttachMutationServices(request.Plan, step.StepId)
		if err != nil || attachErr != nil || !apply.FullReconcile && len(names) == 0 && !attachSelection {
			if err == nil {
				err = attachErr
			}
			return nil, err
		}
		validation := base
		validation.Args = append(append([]string(nil), prefix...), "config", "--quiet", "--no-interpolate")
		commands = append(commands, validation)
		if attachSelection && len(names) == 0 {
			return commands, nil
		}
		mutation := base
		activeServices := false
		for _, service := range artifact.Services {
			if service.ExpectedReplicas != 0 {
				activeServices = true
				break
			}
		}
		if apply.FullReconcile && !activeServices {
			mutation.Args = append(append([]string(nil), prefix...), "down", "--remove-orphans")
			commands = append(commands, mutation)
			return commands, nil
		}
		mutation.Args = append(append([]string(nil), prefix...), "up", "--detach")
		if apply.ForceRecreate {
			mutation.Args = append(mutation.Args, "--force-recreate")
		}
		if apply.NoDependencies {
			mutation.Args = append(mutation.Args, "--no-deps")
		}
		if apply.FullReconcile {
			mutation.Args = append(mutation.Args, "--remove-orphans")
		} else {
			mutation.Args = append(mutation.Args, names...)
		}
		commands = append(commands, mutation)
		return commands, nil
	}
	if compensate := step.GetServiceRecreateCompensate(); compensate != nil {
		validation := base
		validation.Args = append(append([]string(nil), prefix...), "config", "--quiet", "--no-interpolate")
		mutation := base
		mutation.Args = append(
			append([]string(nil), prefix...),
			"up",
			"--detach",
			"--force-recreate",
			"--no-deps",
			"--remove-orphans",
		)
		mutation.Args = append(mutation.Args, serviceNames(artifact, []string{compensate.ServiceId})...)
		return []runner.RunCmdOpts{validation, mutation}, nil
	}
	if apply := step.GetComposeWorkloadApply(); apply != nil {
		validation := base
		validation.Args = append(append([]string(nil), prefix...), "config", "--quiet", "--no-interpolate")
		mutation := base
		mutation.Args = append(append([]string(nil), prefix...), "up", "--detach")
		if apply.EnsureProxy {
			mutation.Args = append(mutation.Args, releaseProxyName(artifact, apply.ServiceId))
		}
		mutation.Args = append(mutation.Args, releaseWorkloadName(artifact, apply.ServiceId, apply.Target))
		if !apply.EnsureProxy {
			// Start the existing stable proxy without reconciling its candidate
			// YAML: it retains predecessor ownership and live connections.
			start := base
			start.Args = append(
				append([]string(nil), prefix...),
				"start",
				releaseProxyName(artifact, apply.ServiceId),
			)
			return []runner.RunCmdOpts{validation, mutation, start}, nil
		}
		return []runner.RunCmdOpts{validation, mutation}, nil
	}
	mutation := base
	switch payload := step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeStop:
		mutation.Args = append(append([]string(nil), prefix...),
			"stop", "--timeout", strconv.FormatUint(uint64(payload.ComposeStop.GraceSeconds), 10))
		mutation.Args = append(mutation.Args, serviceNames(artifact, payload.ComposeStop.ServiceIds)...)
	case *agentpb.ExecutionStep_ComposeRemove:
		if payload.ComposeRemove.WholeProject {
			mutation.Args = append(append([]string(nil), prefix...), "down", "--remove-orphans", "--volumes")
		} else {
			mutation.Args = append(append([]string(nil), prefix...), "rm", "--stop", "--force")
			mutation.Args = append(mutation.Args, serviceNames(artifact, payload.ComposeRemove.ServiceIds)...)
		}
	default:
		return nil, errs.New(errs.KindValidationFailed, "Compose helper step payload is unsupported")
	}
	return []runner.RunCmdOpts{mutation}, nil
}

func applyServiceNames(
	plan *agentpb.ExecutionPlan, step *agentpb.ExecutionStep, artifact *agentpb.ComposeArtifact,
) ([]string, error) {
	if plan.GetEntryMutationProcedure() != nil {
		return executionplan.EntryMutationServices(plan, step.StepId)
	}
	names, selected, err := executionplan.VolumeRemovalServices(plan, step.StepId)
	if selected || err != nil {
		return names, err
	}
	names, selected, err = executionplan.AttachMutationServices(plan, step.StepId)
	if selected || err != nil {
		return names, err
	}
	return serviceNames(artifact, step.GetComposeApply().ServiceIds), nil
}

func composeProjectDirectory(artifact *agentpb.ComposeArtifact) string {
	if artifact.AuthorizedVolumeDir != "" {
		return artifact.AuthorizedVolumeDir
	}
	return WorkDirectory
}

func serviceNames(artifact *agentpb.ComposeArtifact, selected []string) []string {
	selectedIDs := make(map[string]struct{}, len(selected))
	for _, serviceID := range selected {
		selectedIDs[serviceID] = struct{}{}
	}
	names := make([]string, 0, len(selected)*3)
	for _, service := range artifact.Services {
		if _, ok := selectedIDs[service.ServiceId]; ok {
			names = append(names, service.ComposeName)
		}
	}
	sort.Strings(names)
	return names
}

func releaseWorkloadName(artifact *agentpb.ComposeArtifact, serviceID, target string) string {
	for _, service := range artifact.GetServices() {
		if target == "singleton" && service.GetServiceId() == serviceID &&
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
			service.GetServiceId() == serviceID && service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT &&
				service.GetSlot() == target {
			return service.GetComposeName()
		}
	}
	return ""
}
