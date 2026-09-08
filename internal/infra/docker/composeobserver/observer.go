// Package composeobserver provides the Agent's read-only Moby observation
// port. Its Engine interface intentionally contains no mutation method.
package composeobserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/managedimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	dockerSocketHost         = "unix:///var/run/docker.sock"
	maximumObservedResources = 4096
	groundplaneLabelPrefix   = "com.groundplane."
	composeProjectLabel      = "com.docker.compose.project"
	composeServiceLabel      = "com.docker.compose.service"
	composeNetworkLabel      = "com.docker.compose.network"
	composeVolumeLabel       = "com.docker.compose.volume"
	mountpointDigestDomain   = "groundplane:docker-volume-mountpoint:v1\x00"
)

// Engine is the complete read-only Docker authority available to Observer.
type Engine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	NetworkList(context.Context, client.NetworkListOptions) (client.NetworkListResult, error)
	NetworkInspect(context.Context, string, client.NetworkInspectOptions) (client.NetworkInspectResult, error)
	VolumeList(context.Context, client.VolumeListOptions) (client.VolumeListResult, error)
	VolumeInspect(context.Context, string, client.VolumeInspectOptions) (client.VolumeInspectResult, error)
	Close() error
}

type Observer struct {
	engine Engine
	now    func() time.Time
}

func New() (*Observer, error) {
	engine, err := client.New(client.WithHost(dockerSocketHost))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return &Observer{engine: engine, now: time.Now}, nil
}

func NewWithEngine(engine Engine) (*Observer, error) {
	if engine == nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose observer Engine is required")
	}
	return &Observer{engine: engine, now: time.Now}, nil
}

func (observer *Observer) Close() error {
	if observer == nil || observer.engine == nil {
		return nil
	}
	if err := observer.engine.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func (observer *Observer) Observe(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	if observer == nil || observer.engine == nil || observer.now == nil {
		return nil, errs.New(errs.KindInternal, "Compose observer is not configured")
	}
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "Compose observer context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedPlan, err := executionplan.Validate(plan)
	if err != nil {
		return nil, err
	}
	artifact := findArtifact(ownedPlan, artifactID)
	if artifact == nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose observation artifact is not in the plan")
	}
	return observer.observeArtifact(ctx, artifact)
}

// ObserveRestoration accepts only the immutable read-only witness descriptor,
// never a manufactured execution plan with rewritten historical ownership.
func (observer *Observer) ObserveRestoration(
	ctx context.Context,
	observation *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	if observer == nil || observer.engine == nil || observer.now == nil || ctx == nil {
		return nil, errs.New(errs.KindInternal, "Compose restoration observer is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	artifact := observation.Artifact()
	if artifact == nil {
		return nil, errs.New(errs.KindValidationFailed, "Compose restoration observation is empty")
	}
	return observer.observeArtifact(ctx, artifact)
}

func (observer *Observer) observeArtifact(
	ctx context.Context,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ObservedProject, error) {
	observedAt := timestamppb.New(observer.now().UTC())
	if err := observedAt.CheckValid(); err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	result := &agentpb.ObservedProject{ProjectName: artifact.ProjectName, ObservedAt: observedAt}
	if err := observer.observeContainers(ctx, artifact, result); err != nil {
		return nil, err
	}
	if err := observer.observeNetworks(ctx, artifact, result); err != nil {
		return nil, err
	}
	if err := observer.observeVolumes(ctx, artifact, result); err != nil {
		return nil, err
	}
	sort.Slice(result.Containers, func(i, j int) bool {
		return result.Containers[i].ContainerId < result.Containers[j].ContainerId
	})
	sort.Slice(result.Networks, func(i, j int) bool {
		return result.Networks[i].NetworkId < result.Networks[j].NetworkId
	})
	sort.Slice(result.Volumes, func(i, j int) bool { return result.Volumes[i].Name < result.Volumes[j].Name })
	sort.Slice(result.Collisions, func(i, j int) bool {
		if result.Collisions[i].Kind != result.Collisions[j].Kind {
			return result.Collisions[i].Kind < result.Collisions[j].Kind
		}
		return result.Collisions[i].Name < result.Collisions[j].Name
	})
	return result, nil
}

func (observer *Observer) observeContainers(
	ctx context.Context,
	artifact *agentpb.ComposeArtifact,
	result *agentpb.ObservedProject,
) error {
	listed, err := observer.engine.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		return operationError(ctx, "list containers", err)
	}
	if len(listed.Items) > maximumObservedResources {
		return errs.New(errs.KindInternal, "Compose observation container result exceeds its bound")
	}
	expected := make(map[string]*agentpb.ComposeService, len(artifact.Services))
	for _, service := range artifact.Services {
		expected[service.ComposeName] = service
	}
	for _, summary := range listed.Items {
		if summary.Labels[composeProjectLabel] != artifact.ProjectName {
			continue
		}
		inspected, err := observer.engine.ContainerInspect(ctx, summary.ID, client.ContainerInspectOptions{})
		if err != nil {
			return operationError(ctx, "inspect container", err)
		}
		item := inspected.Container
		if item.ID == "" || item.Config == nil || item.State == nil {
			return errs.New(errs.KindInternal, "Compose observation container inspect is incomplete")
		}
		name := strings.TrimPrefix(item.Name, "/")
		service := expected[item.Config.Labels[composeServiceLabel]]
		if name == "" {
			return errs.New(errs.KindInternal, "Compose observation container name is empty")
		}
		if service == nil || item.Config.Labels[composeProjectLabel] != artifact.ProjectName ||
			!labelsMatch(item.Config.Labels, service.ExpectedLabels) {
			result.Collisions = append(result.Collisions, &agentpb.ObservedCollision{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER, Name: name,
				ComposeServiceName: item.Config.Labels[composeServiceLabel],
				ComponentId:        item.Config.Labels["com.groundplane.component-id"],
				ServiceId:          item.Config.Labels["com.groundplane.service-id"],
			})
			continue
		}
		if service.GetOwnerComponentId() != "" || len(service.GetImageConfigDigest()) != 0 {
			if item.Config.Image != service.GetImageReference() {
				return errs.New(
					errs.KindStateConflict,
					"managed Component container image differs from sealed authority",
				)
			}
			if err := managedimage.Verify(item.Image, item.ImageManifestDescriptor,
				"sha256:"+hex.EncodeToString(service.GetImageChildDigest()),
				"sha256:"+hex.EncodeToString(service.GetImageConfigDigest()),
				ocispec.Platform{OS: service.ImageOs, Architecture: service.ImageArchitecture, Variant: service.ImageVariant},
			); err != nil {
				return err
			}
		}
		state, exitCode, err := containerState(item.State)
		if err != nil {
			return err
		}
		health, err := containerHealth(item.State)
		if err != nil {
			return err
		}
		result.Containers = append(result.Containers, &agentpb.ObservedContainer{
			ContainerId: item.ID, Name: name, ServiceId: service.ServiceId,
			ImageReference: item.Config.Image, ImageId: item.Image,
			State: state, Health: health, ExitCode: exitCode,
			Labels: cloneLabels(service.ExpectedLabels),
		})
	}
	return nil
}

func (observer *Observer) observeNetworks(
	ctx context.Context,
	artifact *agentpb.ComposeArtifact,
	result *agentpb.ObservedProject,
) error {
	listed, err := observer.engine.NetworkList(ctx, client.NetworkListOptions{})
	if err != nil {
		return operationError(ctx, "list networks", err)
	}
	if len(listed.Items) > maximumObservedResources {
		return errs.New(errs.KindInternal, "Compose observation network result exceeds its bound")
	}
	byCompose := make(map[string]*agentpb.ComposeNetwork, len(artifact.Networks))
	byName := make(map[string]*agentpb.ComposeNetwork, len(artifact.Networks))
	for _, network := range artifact.Networks {
		byCompose[network.ComposeName] = network
		byName[network.DockerName] = network
	}
	for _, summary := range listed.Items {
		if summary.Labels[composeProjectLabel] != artifact.ProjectName && byName[summary.Name] == nil {
			continue
		}
		inspected, err := observer.engine.NetworkInspect(ctx, summary.ID, client.NetworkInspectOptions{})
		if err != nil {
			return operationError(ctx, "inspect network", err)
		}
		item := inspected.Network
		expected := byCompose[item.Labels[composeNetworkLabel]]
		if item.ID == "" || item.Name == "" {
			return errs.New(errs.KindInternal, "Compose observation network inspect is incomplete")
		}
		if expected == nil || expected.DockerName != item.Name ||
			item.Labels[composeProjectLabel] != artifact.ProjectName ||
			!labelsMatch(item.Labels, expected.ExpectedLabels) {
			result.Collisions = append(result.Collisions, &agentpb.ObservedCollision{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK, Name: item.Name,
			})
			continue
		}
		result.Networks = append(result.Networks, &agentpb.ObservedNetwork{
			NetworkId: item.ID, Name: item.Name, Labels: cloneLabels(expected.ExpectedLabels),
		})
	}
	return nil
}

func (observer *Observer) observeVolumes(
	ctx context.Context,
	artifact *agentpb.ComposeArtifact,
	result *agentpb.ObservedProject,
) error {
	listed, err := observer.engine.VolumeList(ctx, client.VolumeListOptions{})
	if err != nil {
		return operationError(ctx, "list volumes", err)
	}
	if len(listed.Warnings) != 0 {
		return errs.New(errs.KindInternal, "Compose observation volume list returned warnings")
	}
	if len(listed.Items) > maximumObservedResources {
		return errs.New(errs.KindInternal, "Compose observation volume result exceeds its bound")
	}
	byCompose := make(map[string]*agentpb.ComposeVolume, len(artifact.Volumes))
	byName := make(map[string]*agentpb.ComposeVolume, len(artifact.Volumes))
	for _, volume := range artifact.Volumes {
		byCompose[volume.ComposeName] = volume
		byName[volume.DockerName] = volume
	}
	for _, summary := range listed.Items {
		if summary.Labels[composeProjectLabel] != artifact.ProjectName && byName[summary.Name] == nil {
			continue
		}
		inspected, err := observer.engine.VolumeInspect(ctx, summary.Name, client.VolumeInspectOptions{})
		if err != nil {
			return operationError(ctx, "inspect volume", err)
		}
		item := inspected.Volume
		expected := byCompose[item.Labels[composeVolumeLabel]]
		if item.Name == "" {
			return errs.New(errs.KindInternal, "Compose observation volume inspect is incomplete")
		}
		if expected == nil || expected.DockerName != item.Name ||
			item.Labels[composeProjectLabel] != artifact.ProjectName ||
			!labelsMatch(item.Labels, expected.ExpectedLabels) {
			result.Collisions = append(result.Collisions, &agentpb.ObservedCollision{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_VOLUME, Name: item.Name,
			})
			continue
		}
		digest := sha256.Sum256(append([]byte(mountpointDigestDomain), []byte(item.Mountpoint)...))
		result.Volumes = append(result.Volumes, &agentpb.ObservedVolume{
			Name: item.Name, MountpointSha256: append([]byte(nil), digest[:]...),
			Labels: cloneLabels(expected.ExpectedLabels),
		})
	}
	return nil
}

func findArtifact(plan *agentpb.ExecutionPlan, artifactID string) *agentpb.ComposeArtifact {
	for _, artifact := range plan.Artifacts {
		if artifact.ArtifactId == artifactID {
			return artifact
		}
	}
	return nil
}

func labelsMatch(actual map[string]string, expected []*agentpb.LabelPair) bool {
	expectedValues := make(map[string]string, len(expected))
	for _, pair := range expected {
		expectedValues[pair.Key] = pair.Value
		if actual[pair.Key] != pair.Value {
			return false
		}
	}
	for key := range actual {
		if strings.HasPrefix(key, groundplaneLabelPrefix) {
			if _, exists := expectedValues[key]; !exists {
				return false
			}
		}
	}
	return true
}

func cloneLabels(labels []*agentpb.LabelPair) []*agentpb.LabelPair {
	cloned := make([]*agentpb.LabelPair, len(labels))
	for index, label := range labels {
		cloned[index] = proto.Clone(label).(*agentpb.LabelPair)
	}
	return cloned
}

func containerState(
	state *container.State,
) (agentpb.ObservedContainerState, *int32, error) {
	var observed agentpb.ObservedContainerState
	switch state.Status {
	case container.StateCreated, container.StatePaused, container.StateRestarting, container.StateRemoving:
		observed = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED
	case container.StateRunning:
		observed = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING
	case container.StateExited:
		observed = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED
	case container.StateDead:
		observed = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD
	default:
		return 0, nil, errs.New(errs.KindInternal, "Compose observation container state is unsupported")
	}
	if observed != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED &&
		observed != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_DEAD {
		return observed, nil, nil
	}
	if state.ExitCode < math.MinInt32 || state.ExitCode > math.MaxInt32 {
		return 0, nil, errs.New(errs.KindInternal, "Compose observation exit code is outside int32")
	}
	exitCode := int32(state.ExitCode)
	return observed, &exitCode, nil
}

func containerHealth(state *container.State) (agentpb.ObservedContainerHealth, error) {
	if state.Health == nil {
		return agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE, nil
	}
	switch state.Health.Status {
	case container.NoHealthcheck:
		return agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE, nil
	case container.Starting:
		return agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING, nil
	case container.Healthy:
		return agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY, nil
	case container.Unhealthy:
		return agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY, nil
	default:
		return 0, errs.New(errs.KindInternal, "compose observation container health is unsupported")
	}
}

func operationError(ctx context.Context, operation string, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("compose observation: %s: %w", operation, err))
}
