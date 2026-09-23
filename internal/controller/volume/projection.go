package volume

import (
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateVolumeMutationAgainstProjection(
	request volumeMutationRequest,
	projection projectionrecord.EnvironmentComposeProjection,
	hasProjection bool,
) error {
	var found *projectionrecord.EnvironmentVolumeIdentity
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
	environment hierarchyrecord.EnvironmentRecord,
	current projectionrecord.EnvironmentComposeProjection,
	hasCurrent bool,
	request volumeMutationRequest,
	revisionID string,
	generation uint64,
) (projectionrecord.EnvironmentComposeProjection, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact, error) {
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
			return projectionrecord.EnvironmentComposeProjection{}, nil, nil, errs.New(
				errs.KindInternal,
				"Volume baseline artifact is corrupt",
			)
		}
	}
	normalizedArtifact := proto.Clone(oldArtifact).(*agentpb.ComposeArtifact)
	var err error
	if hasCurrent {
		normalizedArtifact, err = composerender.NormalizedEnvironmentArtifact(current)
		if err != nil {
			return projectionrecord.EnvironmentComposeProjection{}, nil, nil, err
		}
	}
	action := composerender.VolumeArtifactAdd
	switch request.action {
	case volumeMutationActionAdd:
		candidate.Volumes = append(candidate.Volumes, projectionrecord.EnvironmentVolumeIdentity{
			ID: request.volumeID, Slug: request.slug, Key: request.key,
		})
		sort.Slice(
			candidate.Volumes,
			func(left, right int) bool { return candidate.Volumes[left].Key < candidate.Volumes[right].Key },
		)
	case volumeMutationActionEdit:
		action = composerender.VolumeArtifactEdit
		for index := range candidate.Volumes {
			if candidate.Volumes[index].ID == request.volumeID {
				candidate.Volumes[index].Slug = request.slug
			}
		}
	case volumeMutationActionRemove:
		action = composerender.VolumeArtifactRemove
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
		return projectionrecord.EnvironmentComposeProjection{}, nil, nil, errs.New(
			errs.KindInternal,
			"Volume mutation action is invalid",
		)
	}
	newArtifact, err := composerender.MutateEnvironmentVolumeArtifact(oldArtifact, composerender.VolumeArtifactMutation{
		Action: action, VolumeID: request.volumeID, Key: request.key,
		ArtifactID: stableIDFromTask(ids.KindConfig, revisionID), PlanID: stableIDFromTask(ids.KindPlan, revisionID),
		TenantID: tenantID, ProjectID: projectID, RenderGeneration: generation,
	})
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, nil, err
	}
	// This second mutation retains only authored YAML. Its logical Service
	// identities have no execution labels and are not runtime validation input;
	// the complete runtime artifact was independently checked above.
	normalizedArtifact.Services = nil
	normalizedArtifact, err = composerender.MutateEnvironmentVolumeArtifact(
		normalizedArtifact,
		composerender.VolumeArtifactMutation{
			Action: action, VolumeID: request.volumeID, Key: request.key,
			ArtifactID: stableIDFromTask(
				ids.KindConfig,
				revisionID,
			), PlanID: stableIDFromTask(ids.KindPlan, revisionID),
			TenantID: tenantID, ProjectID: projectID, RenderGeneration: generation,
		},
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, nil, err
	}
	candidate.NormalizedCompose, err = composerender.AuthoringComposeVolumes(
		normalizedArtifact.GetCanonicalYaml(), candidate.Volumes, environment.VolumeDir,
	)
	if err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, nil, nil, err
	}
	if request.action == volumeMutationActionRemove {
		cleanupArtifactID, cleanupErr := taskplanning.StableVolumeCleanupArtifactID(
			stableIDFromTask(ids.KindConfig, revisionID),
		)
		if cleanupErr != nil {
			return projectionrecord.EnvironmentComposeProjection{}, nil, nil, cleanupErr
		}
		// The cleanup artifact is the exact historical mount/ownership evidence;
		// only its plan-local artifact id changes.
		oldArtifact.ArtifactId = cleanupArtifactID
	}
	return candidate, oldArtifact, newArtifact, nil
}

func cloneVolumeMutationProjection(
	current projectionrecord.EnvironmentComposeProjection,
) projectionrecord.EnvironmentComposeProjection {
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
	return projectionrecord.EnvironmentComposeProjection{
		EnvironmentID: current.EnvironmentID, RevisionID: current.RevisionID,
		RenderGeneration:  current.RenderGeneration,
		ComposeArtifact:   append([]byte(nil), current.ComposeArtifact...),
		NormalizedCompose: append([]byte(nil), current.NormalizedCompose...),
		RuntimeFiles:      runtimeFiles, ServiceExtensions: serviceExtensions,
		DesiredZones:           append([]projectionrecord.EnvironmentZoneProjection(nil), current.DesiredZones...),
		DesiredServices:        append([]servicerecord.EnvironmentServiceProjection(nil), current.DesiredServices...),
		DesiredRoutes:          append([]projectionrecord.EnvironmentRouteProjection(nil), current.DesiredRoutes...),
		Volumes:                append([]projectionrecord.EnvironmentVolumeIdentity(nil), current.Volumes...),
		VolumeMounts:           append([]projectionrecord.EnvironmentServiceVolumeMount(nil), current.VolumeMounts...),
		Components:             append([]componentrecord.Record(nil), current.Components...),
		Entries:                append([]entryrecord.Record(nil), current.Entries...),
		Backup:                 projectionrecord.CloneEnvironmentBlueprintBackupPolicy(current.Backup),
		ServiceDependencyPlans: current.ServiceDependencyPlans.Clone(),
	}
}

func buildVolumeMutationCandidate(
	tenantID string,
	projectID string,
	environment hierarchyrecord.EnvironmentRecord,
	current projectionrecord.EnvironmentComposeProjection,
	hasCurrent bool,
	request volumeMutationRequest,
	revisionID string,
	generation uint64,
) (volumeMutationRequest, projectionrecord.EnvironmentComposeProjection, *agentpb.ComposeArtifact, *agentpb.ComposeArtifact, error) {
	if request.action == volumeMutationActionAdd {
		request.volumeID = stableIDFromTask(ids.KindVolume, revisionID)
	}
	candidate, oldArtifact, newArtifact, err := buildVolumeMutationProjection(
		tenantID, projectID, environment, current, hasCurrent, request, revisionID, generation,
	)
	if err != nil {
		return volumeMutationRequest{}, projectionrecord.EnvironmentComposeProjection{}, nil, nil, err
	}
	artifactBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(newArtifact)
	if err != nil {
		return volumeMutationRequest{}, projectionrecord.EnvironmentComposeProjection{}, nil, nil, errs.Wrap(
			errs.KindInternal,
			err,
		)
	}
	candidate.ComposeArtifact = artifactBytes
	return request, candidate, oldArtifact, newArtifact, nil
}
