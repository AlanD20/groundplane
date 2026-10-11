package edgeconnectioncheck

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type runtimeIdentity struct {
	containerID string
	startedAt   string
}

func (executor *Executor) inspectRuntime(ctx context.Context, target Target) (runtimeIdentity, error) {
	filters := client.Filters{}.Add("label", "com.docker.compose.project="+target.project).
		Add("label", "com.docker.compose.service="+target.service)
	listed, err := executor.engine.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: filters})
	if err != nil || len(listed.Items) != 1 || !validContainerID(listed.Items[0].ID) {
		return runtimeIdentity{}, errs.New(
			errs.KindStateConflict,
			"Tunnel check requires one running connector container",
		)
	}
	inspected, err := executor.engine.ContainerInspect(ctx, listed.Items[0].ID, client.ContainerInspectOptions{})
	if err != nil {
		return runtimeIdentity{}, errs.New(errs.KindStorageUnavailable, "Tunnel container ownership could not be read")
	}
	observed := inspected.Container
	if observed.ID != listed.Items[0].ID || observed.Config == nil || observed.State == nil ||
		observed.HostConfig == nil ||
		!observed.State.Running ||
		observed.State.Paused ||
		observed.State.Restarting ||
		observed.State.Dead ||
		observed.Config.Image != target.imageReference ||
		observed.HostConfig.NetworkMode == "host" ||
		observed.HostConfig.NetworkMode == "none" ||
		strings.HasPrefix(string(observed.HostConfig.NetworkMode), "container:") {
		return runtimeIdentity{}, errs.New(
			errs.KindStateConflict,
			"Tunnel runtime changed or is not running in its selected Zones",
		)
	}
	for name, expected := range target.labels {
		if observed.Config.Labels[name] != expected {
			return runtimeIdentity{}, errs.New(
				errs.KindStateConflict,
				"Tunnel container differs from its captured runtime ownership",
			)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, observed.State.StartedAt); err != nil {
		return runtimeIdentity{}, errs.New(errs.KindStateConflict, "Tunnel process start time is unavailable")
	}
	platform := ocispec.Platform{
		OS:           target.platform.OS,
		Architecture: target.platform.Architecture,
		Variant:      target.platform.Variant,
	}
	if err := managedimage.Verify(observed.Image, observed.ImageManifestDescriptor, "sha256:"+target.platform.ChildDigest, "sha256:"+target.platform.ConfigDigest, platform); err != nil {
		return runtimeIdentity{}, err
	}
	return runtimeIdentity{containerID: observed.ID, startedAt: observed.State.StartedAt}, nil
}

func validContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}
