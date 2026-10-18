package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type lifecycleContainerState struct {
	ID         string `json:"id"`
	Running    bool   `json:"running"`
	Paused     bool   `json:"paused"`
	Restarting bool   `json:"restarting"`
	Dead       bool   `json:"dead"`
	Health     *struct {
		Status string `json:"Status"`
	} `json:"health,omitempty"`
}

func lifecycleMember(input executionplan.LifecycleHelperInput) (*agentpb.ServiceLifecycleMember, error) {
	for _, member := range input.Procedure.Members {
		if member.Ownership.StepId == input.Stage.ID && member.Ownership.ComposeNames[0] == input.Stage.Member {
			return member, nil
		}
	}
	return nil, errs.New(errs.KindValidationFailed, "Service lifecycle stage has no captured runtime member")
}

func inspectLifecycleMember(ctx context.Context, taskRunner runner.Runner, input executionplan.LifecycleHelperInput) (*agentpb.ServiceLifecycleHelperResponse, error) {
	member, err := lifecycleMember(input)
	if err != nil {
		return nil, err
	}
	service := member.Artifact.Services[0]
	containers, err := inspectCapturedContainers(ctx, taskRunner, member.Artifact, service)
	if err != nil {
		return nil, err
	}
	if len(containers) != 0 && len(containers) != int(service.ExpectedReplicas) {
		return nil, errs.New(errs.KindStateConflict, "Service lifecycle found an incomplete set of captured containers")
	}
	states := make([]lifecycleContainerState, 0, len(containers))
	satisfied := input.Stage.Expected != lifecycleexecution.Running || len(containers) == int(service.ExpectedReplicas)
	if input.Stage.Expected == lifecycleexecution.Absent {
		satisfied = len(containers) == 0
	}
	for _, id := range containers {
		output, err := lifecycleDocker(ctx, taskRunner, "container", "inspect", "--format", "{{json .State}}", id)
		if err != nil {
			return nil, err
		}
		var state lifecycleContainerState
		if json.Unmarshal(output.Stdout, &state) != nil {
			return nil, errs.New(errs.KindStateConflict, "Service container state could not be read")
		}
		state.ID = id
		states = append(states, state)
		if input.Stage.Expected == lifecycleexecution.Running {
			satisfied = satisfied && state.Running && !state.Paused && !state.Restarting && !state.Dead && (!service.HasHealthcheck || state.Health != nil && state.Health.Status == "healthy")
		} else if input.Stage.Expected == lifecycleexecution.Stopped {
			satisfied = satisfied && !state.Running && !state.Restarting
		}
	}
	at := time.Now().UTC()
	response := &agentpb.ServiceLifecycleHelperResponse{StageId: input.Stage.ID, Member: input.Stage.Member, ObservedAt: timestamppb.New(at), Inspection: input.InspectOnly, Satisfied: satisfied}
	if satisfied {
		raw, err := json.Marshal(struct {
			Attempt    string                    `json:"attempt"`
			Stage      string                    `json:"stage"`
			State      lifecycleexecution.State  `json:"state"`
			Containers []lifecycleContainerState `json:"containers"`
			At         time.Time                 `json:"at"`
		}{input.Attempt.ID, input.Stage.ID, input.Stage.Expected, states, at})
		if err != nil {
			return nil, errs.Wrap(errs.KindInternal, err)
		}
		digest := sha256.Sum256(raw)
		response.State, response.Evidence = string(input.Stage.Expected), &agentpb.WorkInputReference{InputId: input.Attempt.Input.ID, Sha256: digest[:]}
	}
	return response, nil
}

func mutateLifecycleMember(ctx context.Context, taskRunner runner.Runner, input executionplan.LifecycleHelperInput) error {
	member, err := lifecycleMember(input)
	if err != nil {
		return err
	}
	service := member.Artifact.Services[0]
	containers, err := inspectCapturedContainers(ctx, taskRunner, member.Artifact, service)
	if err != nil {
		return err
	}
	if len(containers) != 0 && len(containers) != int(service.ExpectedReplicas) {
		return errs.New(errs.KindStateConflict, "Service lifecycle cannot mutate an incomplete captured runtime")
	}
	if input.Stage.Expected == lifecycleexecution.Running {
		commands, err := candidateStartupCommands(member.Artifact, member.Artifact.Services)
		if err != nil {
			return err
		}
		defer func() {
			for _, command := range commands {
				clear(command.Stdin)
			}
		}()
		// Starting retained runtime must not recreate already existing members.
		for index := range commands[1].Args {
			if commands[1].Args[index] == "--force-recreate" {
				commands[1].Args[index] = "--no-recreate"
			}
		}
		for _, command := range commands {
			result, err := taskRunner.Run(ctx, command)
			clear(result.Stdout)
			clear(result.Stderr)
			if err != nil {
				return err
			}
			if result.ExitCode != 0 {
				return errs.New(errs.KindRequestFailed, "Service lifecycle could not start its captured Compose member")
			}
		}
		return nil
	}
	for _, id := range containers {
		if _, err := lifecycleDocker(ctx, taskRunner, "container", "stop", "--time", strconv.Itoa(30), id); err != nil {
			return err
		}
		if input.Stage.Expected == lifecycleexecution.Absent {
			if _, err := lifecycleDocker(ctx, taskRunner, "container", "rm", id); err != nil {
				return err
			}
		}
	}
	return nil
}

func lifecycleDocker(ctx context.Context, taskRunner runner.Runner, args ...string) (runner.Result, error) {
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args, Dir: WorkDirectory,
		Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 32 * 1024})
	if err != nil {
		return runner.Result{}, err
	}
	if result.ExitCode != 0 || len(result.Stdout) >= 32*1024 {
		return runner.Result{}, errs.New(errs.KindRequestFailed, "Docker could not complete the captured Service lifecycle operation")
	}
	return result, nil
}
