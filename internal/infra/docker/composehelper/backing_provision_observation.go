package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingContainerInspection struct {
	ID     string `json:"Id"`
	Image  string `json:"Image"`
	Config struct {
		Image  string            `json:"Image"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	State struct {
		Status     string `json:"Status"`
		Running    bool   `json:"Running"`
		Paused     bool   `json:"Paused"`
		Restarting bool   `json:"Restarting"`
		Dead       bool   `json:"Dead"`
		Health     *struct {
			Status string `json:"Status"`
		} `json:"Health,omitempty"`
	} `json:"State"`
}

func observeBackingProvision(ctx context.Context, taskRunner runner.Runner, input backingprovision.HelperInput) (backingprovision.Observation, *backingruntimefact.Observation, error) {
	procedure, attempt := input.Assignment.Procedure, input.Assignment.Attempt
	observation := backingprovision.Observation{ServiceID: input.Layout.ServiceID, Input: attempt.Input, Stage: input.Progress.Stage, At: time.Now().UTC()}
	workload := procedure.Runtime.Services[0]
	containers, err := inspectCapturedContainers(ctx, taskRunner, procedure.Runtime, workload)
	if err != nil {
		return observation, nil, err
	}
	if len(containers) == 0 {
		return observation, nil, nil
	}
	if len(containers) != 1 {
		return observation, nil, errs.New(errs.KindStateConflict, "backing creation found more than its captured singleton container")
	}
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable,
		Args: []string{"container", "inspect", "--format", "{{json .}}", containers[0]}, Dir: WorkDirectory,
		Env: slices.Clone(fixedEnvironment), ReplaceEnv: true, CaptureLimitBytes: 128 * 1024})
	defer clear(result.Stdout)
	defer clear(result.Stderr)
	if err != nil || result.ExitCode != 0 || len(result.Stdout) >= 128*1024 {
		return observation, nil, errs.New(errs.KindRequestFailed, "backing creation could not inspect its captured container")
	}
	var inspected backingContainerInspection
	if json.Unmarshal(result.Stdout, &inspected) != nil || inspected.ID != containers[0] || inspected.Image != workload.ImageReference ||
		inspected.Config.Image != workload.ImageReference || !hasAllExpectedLabels(inspected.Config.Labels, workload.ExpectedLabels) ||
		inspected.Config.Labels["com.docker.compose.project"] != procedure.Runtime.ProjectName ||
		inspected.Config.Labels["com.docker.compose.service"] != workload.ComposeName {
		return observation, nil, errs.New(errs.KindStateConflict, "backing creation container differs from its captured image and ownership")
	}
	for _, key := range []string{"com.groundplane.release-id", "com.groundplane.slot", "com.groundplane.runtime-role", "com.groundplane.component-id", "com.groundplane.tenant-id"} {
		if inspected.Config.Labels[key] != "" {
			return observation, nil, errs.New(errs.KindStateConflict, "backing creation found a container owned by a different runtime operation")
		}
	}
	observation.At = time.Now().UTC()
	if input.Progress.Last != nil && input.Progress.Last.ContainerID != "" && input.Progress.Last.ContainerID != inspected.ID {
		return observation, nil, errs.New(errs.KindStateConflict, "backing creation container changed between acknowledged execution stages")
	}
	if observation.Stage == backingprovision.ReconcileRuntime {
		if inspected.State.Running && !inspected.State.Restarting && !inspected.State.Paused && !inspected.State.Dead {
			observation.RuntimeReconciled, observation.ContainerID = true, inspected.ID
		}
		return observation, nil, nil
	}
	running := inspected.State.Status == "running" && inspected.State.Running && !inspected.State.Restarting && !inspected.State.Paused && !inspected.State.Dead
	health := agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE
	if workload.HasHealthcheck {
		if inspected.State.Health == nil {
			return observation, nil, errs.New(errs.KindStateConflict, "backing container is missing its configured Docker healthcheck")
		}
		switch inspected.State.Health.Status {
		case "healthy":
			health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY
		case "starting":
			return observation, nil, nil
		case "unhealthy":
			return observation, nil, errs.New(errs.KindRequestFailed, "backing container healthcheck failed")
		default:
			return observation, nil, errs.New(errs.KindStateConflict, "backing container reported an unknown healthcheck state")
		}
	}
	if !running {
		if inspected.State.Dead || inspected.State.Status == "exited" {
			return observation, nil, errs.New(errs.KindRequestFailed, "backing container stopped before it became ready")
		}
		return observation, nil, nil
	}
	observation.ContainerID, observation.RuntimeReady, observation.Healthy = inspected.ID, true, workload.HasHealthcheck
	if observation.Stage != backingprovision.VerifyRuntime {
		return observation, nil, nil
	}
	observation.RuntimeVerified = true
	labels := make([]*agentpb.LabelPair, 0, len(workload.ExpectedLabels))
	for _, expected := range workload.ExpectedLabels {
		labels = append(labels, &agentpb.LabelPair{Key: expected.Key, Value: inspected.Config.Labels[expected.Key]})
	}
	runtime := &backingruntimefact.Observation{ProjectName: procedure.Runtime.ProjectName, ObservedAt: observation.At,
		ContainerID: inspected.ID, ServiceID: procedure.ServiceId, ImageReference: inspected.Config.Image, LocalImageID: inspected.Image,
		State: agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING, Health: health, Labels: labels}
	return observation, runtime, nil
}
