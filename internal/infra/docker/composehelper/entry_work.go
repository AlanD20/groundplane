package composehelper

import (
	"context"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func ExecuteEntryConfigurationWork(ctx context.Context, taskRunner runner.Runner,
	request *agentpb.EntryConfigurationWorkHelperRequest,
) (*agentpb.EntryConfigurationWorkHelperResponse, error) {
	input, err := entryexecution.OpenHelperRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil || taskRunner == nil {
		return nil, errs.New(errs.KindInternal, "Entry workload helper is not configured")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	if input.InspectOnly {
		cancel()
		bounded, cancel = context.WithTimeout(ctx, 15*time.Second)
	}
	defer cancel()
	artifact := input.Procedure.Candidate
	if input.Recovery != nil {
		artifact = input.Procedure.Baseline
	}
	services := entryWorkConsumers(artifact, input.Procedure.RunningConsumerIds)
	ready, err := inspectEntryConsumers(bounded, taskRunner, artifact, services, input.Recovery != nil)
	if err != nil {
		return nil, err
	}
	if !input.InspectOnly && !ready {
		names := make([]string, len(services))
		for index, service := range services {
			names[index] = service.ComposeName
		}
		slices.Sort(names)
		if _, err := runEntryCompose(bounded, taskRunner, artifact, "config", "--quiet"); err != nil {
			return nil, err
		}
		args := append([]string{"up", "--detach", "--no-deps", "--pull", "never", "--"}, names...)
		if _, err := runEntryCompose(bounded, taskRunner, artifact, args...); err != nil {
			return nil, err
		}
		ready, err = inspectEntryConsumers(bounded, taskRunner, artifact, services, input.Recovery != nil)
		if err != nil {
			return nil, err
		}
	}
	if input.Recovery != nil {
		return entryexecution.HelperRestoration(input, entryexecution.ConsumerRestoration{AttemptID: input.Attempt.ID, Input: input.Attempt.Input, Restored: ready, At: time.Now().UTC()})
	}
	observed := entryexecution.Observation{EnvironmentID: input.Procedure.EnvironmentId, Input: input.Attempt.Input,
		Stage: input.Progress.Stage, At: time.Now().UTC()}
	if input.Progress.Stage == entryexecution.ReconcileConsumers {
		observed.ConsumersReconciled = ready
	} else {
		observed.ConsumersVerified = ready
	}
	return entryexecution.HelperObservation(input, observed)
}

func entryWorkConsumers(artifact *agentpb.ComposeArtifact, consumers []string) []*agentpb.ComposeService {
	var services []*agentpb.ComposeService
	for _, service := range artifact.Services {
		if slices.Contains(consumers, service.ServiceId) &&
			service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY && service.OwnerComponentId == "" {
			services = append(services, service)
		}
	}
	return services
}

// Check captured lineage and replica count before Compose can recreate anything.
// Matching labels alone do not prove the new env-file/config content was loaded:
// Compose's resolved service hash must also match on every running replica.
func inspectEntryConsumers(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact,
	services []*agentpb.ComposeService, restoring bool,
) (bool, error) {
	if len(services) == 0 {
		return false, errs.New(errs.KindValidationFailed, "Entry workload selection is empty")
	}
	ready := true
	for _, service := range services {
		containers, err := inspectCapturedWorkloads(ctx, taskRunner, artifact, service.ComposeName)
		if err != nil {
			return false, err
		}
		if service.ExpectedReplicas == 0 || uint64(len(containers)) > uint64(service.ExpectedReplicas) ||
			!restoring && uint64(len(containers)) != uint64(service.ExpectedReplicas) {
			return false, errs.New(errs.KindStateConflict, "Entry consumers no longer match the captured running replicas; no workloads were started")
		}
		if uint64(len(containers)) != uint64(service.ExpectedReplicas) {
			ready = false
		}
		result, err := runEntryCompose(ctx, taskRunner, artifact, "config", "--hash", service.ComposeName)
		if err != nil {
			return false, err
		}
		fields := strings.Fields(string(result.Stdout))
		clear(result.Stdout)
		if len(fields) != 2 || fields[0] != service.ComposeName || len(fields[1]) != 64 {
			return false, errs.New(errs.KindStateConflict, "Compose did not return the selected Entry consumer configuration hash")
		}
		if _, err := hex.DecodeString(fields[1]); err != nil {
			return false, errs.New(errs.KindStateConflict, "Compose returned an invalid consumer configuration hash")
		}
		for _, id := range containers {
			actual, err := runEntryCommand(ctx, taskRunner, runner.RunCmdOpts{
				Name: DockerExecutable, Args: []string{"container", "inspect", "--format",
					`{{.State.Running}} {{index .Config.Labels "com.docker.compose.config-hash"}}`, id},
			})
			if err != nil {
				return false, err
			}
			state := strings.Fields(string(actual.Stdout))
			clear(actual.Stdout)
			if len(state) != 2 || state[0] != "true" && state[0] != "false" {
				return false, errs.New(errs.KindStateConflict, "Entry consumer inspection returned invalid runtime state")
			}
			if state[0] != "true" && !restoring {
				return false, errs.New(errs.KindStateConflict, "Entry consumer is no longer running; Deploy it explicitly instead of starting it through an Entry edit")
			}
			ready = ready && state[0] == "true" && state[1] == fields[1]
		}
	}
	return ready, nil
}

func runEntryCompose(ctx context.Context, taskRunner runner.Runner, artifact *agentpb.ComposeArtifact, args ...string) (runner.Result, error) {
	input, err := composeCLIInput(artifact.CanonicalYaml)
	if err != nil {
		return runner.Result{}, err
	}
	prefix := []string{"compose", "--project-name", artifact.ProjectName,
		"--project-directory", composeProjectDirectory(artifact), "--file", "-"}
	return runEntryCommand(ctx, taskRunner, runner.RunCmdOpts{Name: DockerExecutable,
		Args: append(prefix, args...), Stdin: input})
}

func runEntryCommand(ctx context.Context, taskRunner runner.Runner, command runner.RunCmdOpts) (runner.Result, error) {
	command.Dir, command.Env, command.ReplaceEnv = WorkDirectory, slices.Clone(fixedEnvironment), true
	command.CaptureLimitBytes = 32 * 1024
	result, err := taskRunner.Run(ctx, command)
	clear(result.Stderr)
	if ctx.Err() != nil || err != nil || result.ExitCode != 0 || int64(len(result.Stdout)) >= command.CaptureLimitBytes {
		clear(result.Stdout)
		if ctx.Err() != nil {
			return runner.Result{}, ctx.Err()
		}
		return runner.Result{}, errs.New(errs.KindRequestFailed, "Docker could not inspect or reconcile the captured Entry consumers")
	}
	return result, nil
}
