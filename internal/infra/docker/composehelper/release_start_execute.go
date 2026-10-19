package composehelper

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ReleaseStartupResult deliberately does not claim readiness or serving state.
// Once MutationAttempted is true, any error requires observation/reconciliation.
type ReleaseStartupResult struct {
	MutationAttempted bool
	ExitCode          int
}

// ExecuteReleaseStartup is the native helper operation, with no generic plan.
// It never retries Compose up: the caller must inspect interrupted effects.
func ExecuteReleaseStartup(
	ctx context.Context,
	taskRunner runner.Runner,
	input releaseexecution.Input,
	report releaseexecution.CheckpointReport,
) (ReleaseStartupResult, error) {
	result := ReleaseStartupResult{}
	if ctx == nil || taskRunner == nil {
		return result, errs.New(errs.KindInternal, "Release startup requires a context and subprocess runner")
	}
	commands, err := ReleaseStartupCommands(input, report)
	if err != nil {
		return result, err
	}
	defer func() {
		for _, command := range commands {
			clear(command.Stdin)
		}
	}()
	artifacts, err := executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		return result, err
	}
	deadline := input.Attempt.Deadline
	if cursor := report.Progress.Rolling; cursor != nil && cursor.ReplicaDeadline.Before(deadline) {
		deadline = cursor.ReplicaDeadline
	}
	bounded, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return result, err
	}
	prior := &agentpb.ComposeArtifact{ProjectName: artifacts.Candidate.ProjectName}
	var proxy *agentpb.ComposeService
	if artifacts.Prior != nil {
		prior = proto.CloneOf(artifacts.Prior.Artifact)
		proxy = artifacts.Prior.Proxy
		if artifacts.Prior.Retained != nil {
			prior.Services = append(prior.Services, proto.CloneOf(artifacts.Prior.Retained).Services...)
		}
	}
	inventory, err := inspectServingInventory(
		bounded,
		taskRunner,
		prior,
		artifacts.Candidate,
		input.Release.ServiceId,
		input.Release.ReleaseId,
		proxy,
	)
	if err != nil {
		return result, err
	}
	if input.Release.Strategy == agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE &&
		len(inventory.predecessors) != 0 {
		return result, errs.New(
			errs.KindStateConflict,
			"previous workload still exists; recreate removal has not completed",
		)
	}
	names := make(map[string]bool)
	if report.Progress.Rolling == nil {
		for _, service := range artifacts.Workloads {
			names[service.ComposeName] = true
		}
	} else {
		changes, err := artifacts.Rolling.Changes()
		if err != nil {
			return result, err
		}
		names[changes[report.Progress.Rolling.Change-1].Candidate.ComposeName] = true
	}
	for _, id := range inventory.candidates {
		inspect, err := taskRunner.Run(bounded, runner.RunCmdOpts{Name: DockerExecutable,
			Args: []string{"container", "inspect", "--format", "{{json .Config.Labels}}", id},
			Dir:  WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024})
		var labels map[string]string
		if err != nil || inspect.ExitCode != 0 || json.Unmarshal(inspect.Stdout, &labels) != nil {
			return result, errs.New(errs.KindStateConflict, "candidate container ownership cannot be inspected")
		}
		if names[labels["com.docker.compose.service"]] {
			return result, errs.New(
				errs.KindStateConflict,
				"candidate already exists; reconcile its startup instead of recreating it",
			)
		}
	}
	for index, command := range commands {
		if err := bounded.Err(); err != nil {
			return result, err
		}
		if index > 0 {
			result.MutationAttempted = true
		}
		output, err := taskRunner.Run(bounded, command)
		result.ExitCode = output.ExitCode
		clear(output.Stdout)
		clear(output.Stderr)
		if err != nil {
			return result, err
		}
		if result.ExitCode != 0 {
			if index == 0 {
				return result, errs.New(
					errs.KindRequestFailed,
					"captured candidate Compose configuration was rejected before startup",
				)
			}
			return result, errs.New(
				errs.KindRequestFailed,
				"candidate startup failed; inspect the captured containers before retrying",
			)
		}
	}
	return result, nil
}
