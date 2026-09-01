package volume

import (
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateVolumeMutationAgainstProjection(
	request volumeMutationRequest,
	projection etcd.EnvironmentComposeProjection,
	hasProjection bool,
) error {
	var found *etcd.EnvironmentVolumeIdentity
	for index := range projection.Volumes {
		volume := &projection.Volumes[index]
		if volume.ID == request.volumeID {
			found = volume
		}
		if request.action == volumeMutationActionAdd && (volume.Slug == request.slug || volume.Key == request.key) {
			return errs.New(errs.KindNameConflict, "Volume slug or immutable key is already in use")
		}
		if request.action == volumeMutationActionEdit && volume.ID != request.volumeID && volume.Slug == request.slug {
			return errs.New(errs.KindNameConflict, "Volume slug is already in use")
		}
	}
	if request.action == volumeMutationActionAdd {
		return nil
	}
	if !hasProjection || found == nil || found.Key != request.key {
		return errs.New(errs.KindVolumeNotFound, "volume was not found")
	}
	return nil
}

func buildVolumeMutationProjection(
	tenantID string,
	projectID string,
	environment etcd.EnvironmentRecord,
	current etcd.EnvironmentComposeProjection,
	hasCurrent bool,
	request volumeMutationRequest,
	revisionID string,
	generation uint64,
) (etcd.EnvironmentComposeProjection, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact, error) {
	candidate := cloneVolumeMutationProjection(current)
	candidate.EnvironmentID = environment.ID
	candidate.RevisionID = revisionID
	candidate.RenderGeneration = generation
	oldArtifact := &agentpb.ComposeArtifact{
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId:   environment.ID, ProjectName: "gp-" + strings.ToLower(environment.ID),
		AuthorizedVolumeDir: environment.VolumeDir,
		CanonicalYaml:       []byte("services: {}\nnetworks: {}\n"),
	}
	if hasCurrent {
		if err := proto.Unmarshal(current.ComposeArtifact, oldArtifact); err != nil {
			return etcd.EnvironmentComposeProjection{}, nil, nil, errs.New(errs.KindInternal, "Volume baseline artifact is corrupt")
		}
	}
	normalizedArtifact := proto.Clone(oldArtifact).(*agentpb.ComposeArtifact)
	var err error
	if hasCurrent {
		normalizedArtifact, err = controller.NormalizedEnvironmentArtifact(current)
		if err != nil {
			return etcd.EnvironmentComposeProjection{}, nil, nil, err
		}
	}
	action := controller.VolumeArtifactAdd
	switch request.action {
	case volumeMutationActionAdd:
		candidate.Volumes = append(candidate.Volumes, etcd.EnvironmentVolumeIdentity{
			ID: request.volumeID, Slug: request.slug, Key: request.key,
		})
		sort.Slice(candidate.Volumes, func(left, right int) bool { return candidate.Volumes[left].Key < candidate.Volumes[right].Key })
	case volumeMutationActionEdit:
		action = controller.VolumeArtifactEdit
		for index := range candidate.Volumes {
			if candidate.Volumes[index].ID == request.volumeID {
				candidate.Volumes[index].Slug = request.slug
			}
		}
	case volumeMutationActionRemove:
		action = controller.VolumeArtifactRemove
		keptVolumes := candidate.Volumes[:0]
		for _, volume := range candidate.Volumes {
			if volume.ID != request.volumeID {
				keptVolumes = append(keptVolumes, volume)
			}
		}
		candidate.Volumes = keptVolumes
		keptMounts := candidate.VolumeMounts[:0]
		for _, mount := range candidate.VolumeMounts {
			if mount.VolumeID != request.volumeID {
				keptMounts = append(keptMounts, mount)
			}
		}
		candidate.VolumeMounts = keptMounts
	default:
		return etcd.EnvironmentComposeProjection{}, nil, nil, errs.New(errs.KindInternal, "Volume mutation action is invalid")
	}
	newArtifact, err := controller.MutateEnvironmentVolumeArtifact(oldArtifact, controller.VolumeArtifactMutation{
		Action: action, VolumeID: request.volumeID, Key: request.key,
		ArtifactID: stableIDFromTask(ids.KindConfig, revisionID), PlanID: stableIDFromTask(ids.KindPlan, revisionID),
		TenantID: tenantID, ProjectID: projectID, RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, nil, err
	}
	normalizedArtifact, err = controller.MutateEnvironmentVolumeArtifact(normalizedArtifact, controller.VolumeArtifactMutation{
		Action: action, VolumeID: request.volumeID, Key: request.key,
		ArtifactID: stableIDFromTask(ids.KindConfig, revisionID), PlanID: stableIDFromTask(ids.KindPlan, revisionID),
		TenantID: tenantID, ProjectID: projectID, RenderGeneration: generation,
	})
	if err != nil {
		return etcd.EnvironmentComposeProjection{}, nil, nil, err
	}
	candidate.NormalizedCompose = append([]byte(nil), normalizedArtifact.GetCanonicalYaml()...)
	if request.action == volumeMutationActionRemove {
		cleanupArtifactID, cleanupErr := controller.StableVolumeCleanupArtifactID(
			stableIDFromTask(ids.KindConfig, revisionID),
		)
		if cleanupErr != nil {
			return etcd.EnvironmentComposeProjection{}, nil, nil, cleanupErr
		}
		oldArtifact, err = controller.MutateEnvironmentVolumeArtifact(oldArtifact, controller.VolumeArtifactMutation{
			Action: controller.VolumeArtifactEdit, VolumeID: request.volumeID, Key: request.key,
			ArtifactID: cleanupArtifactID,
			PlanID:     stableIDFromTask(ids.KindPlan, revisionID), TenantID: tenantID, ProjectID: projectID,
			RenderGeneration: generation,
		})
		if err != nil {
			return etcd.EnvironmentComposeProjection{}, nil, nil, err
		}
	}
	return candidate, oldArtifact, newArtifact, nil
}

func cloneVolumeMutationProjection(current etcd.EnvironmentComposeProjection) etcd.EnvironmentComposeProjection {
	runtimeFiles := make([]core.BlueprintFile, len(current.RuntimeFiles))
	for index, file := range current.RuntimeFiles {
		runtimeFiles[index] = core.BlueprintFile{Path: file.Path, Content: append([]byte(nil), file.Content...)}
	}
	serviceExtensions := make(map[string]core.ServiceExtensionSpec, len(current.ServiceExtensions))
	for name, extension := range current.ServiceExtensions {
		clone := extension
		if extension.Release != nil {
			release := *extension.Release
			clone.Release = &release
		}
		if extension.DependsOn != nil {
			clone.DependsOn = make(map[string]core.ServiceDependency, len(extension.DependsOn))
			for dependency, decision := range extension.DependsOn {
				decision.Phases = append([]core.ServiceDependencyPhase(nil), decision.Phases...)
				clone.DependsOn[dependency] = decision
			}
		}
		serviceExtensions[name] = clone
	}
	if current.ServiceExtensions == nil {
		serviceExtensions = nil
	}
	return etcd.EnvironmentComposeProjection{
		EnvironmentID: current.EnvironmentID, RevisionID: current.RevisionID,
		RenderGeneration:  current.RenderGeneration,
		ComposeArtifact:   append([]byte(nil), current.ComposeArtifact...),
		NormalizedCompose: append([]byte(nil), current.NormalizedCompose...),
		RuntimeFiles:      runtimeFiles, ServiceExtensions: serviceExtensions,
		DesiredZones:           append([]etcd.EnvironmentZoneProjection(nil), current.DesiredZones...),
		DesiredServices:        append([]etcd.EnvironmentServiceProjection(nil), current.DesiredServices...),
		DesiredRoutes:          append([]etcd.EnvironmentRouteProjection(nil), current.DesiredRoutes...),
		Volumes:                append([]etcd.EnvironmentVolumeIdentity(nil), current.Volumes...),
		VolumeMounts:           append([]etcd.EnvironmentServiceVolumeMount(nil), current.VolumeMounts...),
		Components:             append([]etcd.ComponentRecord(nil), current.Components...),
		Entries:                append([]etcd.EntryRecord(nil), current.Entries...),
		ServiceDependencyPlans: current.ServiceDependencyPlans.Clone(),
	}
}

func buildVolumeMutationCandidate(
	tenantID string,
	projectID string,
	environment etcd.EnvironmentRecord,
	current etcd.EnvironmentComposeProjection,
	hasCurrent bool,
	request volumeMutationRequest,
	revisionID string,
	generation uint64,
) (volumeMutationRequest, etcd.EnvironmentComposeProjection, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact, error) {
	if request.action == volumeMutationActionAdd {
		request.volumeID = stableIDFromTask(ids.KindVolume, revisionID)
	}
	candidate, oldArtifact, newArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, current, hasCurrent, request, revisionID, generation,
	)
	if err != nil {
		return volumeMutationRequest{}, etcd.EnvironmentComposeProjection{}, nil, nil, err
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(newArtifact)
	if err != nil {
		return volumeMutationRequest{}, etcd.EnvironmentComposeProjection{}, nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	candidate.ComposeArtifact = artifactBytes
	return request, candidate, oldArtifact, newArtifact, nil
}
