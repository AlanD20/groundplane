package etcdcontainer

import (
	"testing"

	"github.com/moby/moby/api/types/container"
)

func TestDesiredContainerIsPrivatePersistentAndControllerOwned(t *testing.T) {
	options := createOptions()
	if options.Name != ContainerName || options.Config.Image != Image || options.Config.User != "0" {
		t.Fatalf("identity = %q/%q/%q", options.Name, options.Config.Image, options.Config.User)
	}
	if options.Config.Labels[labelManaged] != "true" || options.Config.Labels[labelKind] != "etcd" {
		t.Fatalf("labels = %#v", options.Config.Labels)
	}
	if options.HostConfig.NetworkMode != container.NetworkMode("host") ||
		options.HostConfig.RestartPolicy.Name != container.RestartPolicyDisabled {
		t.Fatalf("runtime policy = %#v", options.HostConfig)
	}
	if len(options.HostConfig.Mounts) != 1 || options.HostConfig.Mounts[0].Source != DataDirectory ||
		options.HostConfig.Mounts[0].Target != DataDirectory {
		t.Fatalf("mounts = %#v", options.HostConfig.Mounts)
	}
	if len(options.Config.Cmd) == 0 || options.Config.Cmd[0] != "--name=groundplane" {
		t.Fatalf("command = %#v", options.Config.Cmd)
	}

	inspected := container.InspectResponse{Config: options.Config, HostConfig: options.HostConfig}
	if !matchesDesired(inspected) {
		t.Fatal("exact desired container did not match")
	}
	inspected.HostConfig.NetworkMode = container.NetworkMode("bridge")
	if matchesDesired(inspected) {
		t.Fatal("drifted network mode matched desired container")
	}
}

func TestEndpointsReturnsAnIndependentFixedProjection(t *testing.T) {
	first := Endpoints()
	first[0] = "changed"
	if got := Endpoints(); len(got) != 1 || got[0] != endpoint {
		t.Fatalf("Endpoints() = %#v", got)
	}
}
