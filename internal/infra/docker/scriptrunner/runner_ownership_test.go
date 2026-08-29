package scriptrunner

import (
	"strings"
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/scriptexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestValidateOwnedContainer(t *testing.T) {
	t.Parallel()

	containerID := strings.Repeat("a", 64)
	prepared := preparedBody{hostPath: "/var/lib/groundplane/agent/tasks/assignment/execution/body"}
	request := scriptexecution.Request{
		Projection: &agentpb.ScriptRunnerProjection{
			Name:       "gp-script-01k5v8k8yr0000000000000000",
			Image:      "registry.example.test/app@sha256:" + strings.Repeat("b", 64),
			Uid:        1000,
			Gid:        1000,
			Entrypoint: []string{"/bin/sh"},
			Command:    []string{bodyTarget},
			Labels: []*agentpb.ScriptStringPair{
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.kind", Value: "script-runner"},
			},
		},
	}
	inspected := client.ContainerInspectResult{Container: container.InspectResponse{
		ID:   containerID,
		Name: "/" + request.Projection.Name,
		Config: &container.Config{
			Image: request.Projection.Image, User: "1000:1000",
			Entrypoint: request.Projection.Entrypoint, Cmd: request.Projection.Command,
			Labels: pairMap(request.Projection.Labels),
		},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "none"}},
		State:      &container.State{Status: container.StateCreated},
		Mounts: []container.MountPoint{{
			Type: mount.TypeBind, Source: prepared.hostPath, Destination: bodyTarget, RW: false,
		}},
	}}

	if err := validateOwnedContainer(inspected, containerID, request, prepared); err != nil {
		t.Fatalf("validate matching container: %v", err)
	}

	inspected.Container.Config.Labels["com.groundplane.task-id"] = "task_01k5v8k8yr0000000000000000"
	if err := validateOwnedContainer(inspected, containerID, request, prepared); err == nil {
		t.Fatal("expected an unsealed label to fail ownership validation")
	}
}

func TestValidateOwnedContainerRejectsBodyMountMismatch(t *testing.T) {
	t.Parallel()

	containerID := strings.Repeat("a", 64)
	prepared := preparedBody{hostPath: "/var/lib/groundplane/agent/tasks/assignment/execution/body"}
	request := scriptexecution.Request{Projection: &agentpb.ScriptRunnerProjection{
		Name: "gp-script-01k5v8k8yr0000000000000000", Image: "app@sha256:" + strings.Repeat("b", 64),
		Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
	}}
	inspected := client.ContainerInspectResult{Container: container.InspectResponse{
		ID: containerID, Name: "/" + request.Projection.Name,
		Config:     &container.Config{Image: request.Projection.Image, User: "0:0", Entrypoint: request.Projection.Entrypoint, Cmd: request.Projection.Command},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "none"}},
		State:      &container.State{Status: container.StateCreated},
		Mounts:     []container.MountPoint{{Type: mount.TypeBind, Source: prepared.hostPath + ".other", Destination: bodyTarget}},
	}}

	if err := validateOwnedContainer(inspected, containerID, request, prepared); err == nil {
		t.Fatal("expected a different body source to fail ownership validation")
	}
}

func TestValidDockerContainerID(t *testing.T) {
	t.Parallel()

	if !validDockerContainerID(strings.Repeat("f", 64)) {
		t.Fatal("expected a lowercase hexadecimal id to be valid")
	}
	if validDockerContainerID(strings.Repeat("F", 64)) || validDockerContainerID(strings.Repeat("a", 63)) {
		t.Fatal("expected malformed ids to be invalid")
	}
}
