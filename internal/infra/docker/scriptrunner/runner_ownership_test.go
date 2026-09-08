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
			Image:      "sha256:" + strings.Repeat("b", 64),
			Uid:        1000,
			Gid:        1000,
			WorkingDir: "/srv/app",
			Entrypoint: []string{"/bin/sh"},
			Command:    []string{bodyTarget},
			Environment: []*agentpb.ScriptStringPair{
				{Key: "APP_ENV", Value: "production"},
			},
			Labels: []*agentpb.ScriptStringPair{
				{Key: "com.groundplane.managed", Value: "true"},
				{Key: "com.groundplane.kind", Value: "script-runner"},
			},
		},
	}
	effectiveLabels := pairMap(request.Projection.Labels)
	effectiveLabels["org.opencontainers.image.title"] = "application-base"
	inspected := client.ContainerInspectResult{Container: container.InspectResponse{
		ID:    containerID,
		Image: request.Projection.Image,
		Name:  "/" + request.Projection.Name,
		Config: &container.Config{
			Image: request.Projection.Image, User: "1000:1000",
			Entrypoint: request.Projection.Entrypoint, Cmd: request.Projection.Command,
			WorkingDir: request.Projection.WorkingDir,
			Env: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
				"APP_ENV=production",
				"PHP_INI_SCAN_DIR=/usr/local/etc/php/conf.d",
			},
			Labels: effectiveLabels,
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

	// Rationale: matching Config.Image does not prove the actual container image.
	for _, imageID := range []string{"", "sha256:" + strings.Repeat("c", 64)} {
		candidate := inspected
		candidate.Container.Image = imageID
		if err := validateOwnedContainer(candidate, containerID, request, prepared); err == nil {
			t.Fatal("accepted container with incorrect actual image identity")
		}
	}

	tests := []struct {
		name   string
		mutate func(*container.Config)
	}{
		{name: "extra Groundplane label", mutate: func(config *container.Config) {
			config.Labels["com.groundplane.task-id"] = "task_01k5v8k8yr0000000000000000"
		}},
		{name: "missing sealed label", mutate: func(config *container.Config) {
			delete(config.Labels, "com.groundplane.managed")
		}},
		{name: "mismatched sealed label", mutate: func(config *container.Config) {
			config.Labels["com.groundplane.kind"] = "service"
		}},
		{name: "overridden sealed environment", mutate: func(config *container.Config) {
			config.Env[1] = "APP_ENV=development"
		}},
		{name: "duplicate sealed environment", mutate: func(config *container.Config) {
			config.Env = append(config.Env, "APP_ENV=production")
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := inspected
			config := *inspected.Container.Config
			config.Env = append([]string(nil), config.Env...)
			config.Labels = make(map[string]string, len(inspected.Container.Config.Labels))
			for key, value := range inspected.Container.Config.Labels {
				config.Labels[key] = value
			}
			candidate.Container.Config = &config
			test.mutate(&config)
			if err := validateOwnedContainer(candidate, containerID, request, prepared); err == nil {
				t.Fatal("expected sealed ownership mismatch to fail validation")
			}
		})
	}
}

// Rationale: an empty sealed working directory delegates to the digest-pinned
// image while image environment defaults and OCI labels remain outside ownership.
func TestValidateOwnedContainerAllowsInheritedImageDefaults(t *testing.T) {
	t.Parallel()

	containerID := strings.Repeat("a", 64)
	prepared := preparedBody{hostPath: "/var/lib/groundplane/agent/tasks/assignment/execution/body"}
	request := scriptexecution.Request{Projection: &agentpb.ScriptRunnerProjection{
		Name: "gp-script-01k5v8k8yr0000000000000000", Image: "sha256:" + strings.Repeat("b", 64),
		Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
		Labels: []*agentpb.ScriptStringPair{
			{Key: "com.groundplane.managed", Value: "true"},
			{Key: "com.groundplane.kind", Value: "script-runner"},
		},
	}}
	labels := pairMap(request.Projection.Labels)
	labels["org.opencontainers.image.title"] = "application-base"
	inspected := client.ContainerInspectResult{Container: container.InspectResponse{
		ID: containerID, Image: request.Projection.Image, Name: "/" + request.Projection.Name,
		Config: &container.Config{
			Image: request.Projection.Image, User: "0:0", WorkingDir: "/usr/src/app",
			Entrypoint: request.Projection.Entrypoint, Cmd: request.Projection.Command,
			Env: []string{"PATH=/usr/local/bin:/usr/bin:/bin"}, Labels: labels,
		},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "none"}},
		State:      &container.State{Status: container.StateCreated},
		Mounts: []container.MountPoint{{
			Type: mount.TypeBind, Source: prepared.hostPath, Destination: bodyTarget,
		}},
	}}

	if err := validateOwnedContainer(inspected, containerID, request, prepared); err != nil {
		t.Fatalf("validate inherited image defaults: %v", err)
	}
}

func TestValidateOwnedContainerRejectsBodyMountMismatch(t *testing.T) {
	t.Parallel()

	containerID := strings.Repeat("a", 64)
	prepared := preparedBody{hostPath: "/var/lib/groundplane/agent/tasks/assignment/execution/body"}
	request := scriptexecution.Request{Projection: &agentpb.ScriptRunnerProjection{
		Name: "gp-script-01k5v8k8yr0000000000000000", Image: "sha256:" + strings.Repeat("b", 64),
		Entrypoint: []string{"/bin/sh"}, Command: []string{bodyTarget},
	}}
	inspected := client.ContainerInspectResult{Container: container.InspectResponse{
		ID: containerID, Image: request.Projection.Image, Name: "/" + request.Projection.Name,
		Config: &container.Config{
			Image:      request.Projection.Image,
			User:       "0:0",
			Entrypoint: request.Projection.Entrypoint,
			Cmd:        request.Projection.Command,
		},
		HostConfig: &container.HostConfig{LogConfig: container.LogConfig{Type: "none"}},
		State:      &container.State{Status: container.StateCreated},
		Mounts: []container.MountPoint{
			{Type: mount.TypeBind, Source: prepared.hostPath + ".other", Destination: bodyTarget},
		},
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
