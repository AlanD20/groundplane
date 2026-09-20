package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Candidate execution starts from the already resolved runtime. Authored
// normalized Compose supplies native membership, not Entry or Attach values.
func renderBlueprintCandidateArtifact(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ReleaseRenderInput,
	images map[string]domain.WorkloadSeal,
	releases map[string]ComposeReleaseIdentity,
) (*agentpb.ComposeArtifact, error) {
	projection := input.Projection
	artifact := &agentpb.ComposeArtifact{}
	if projection.EnvironmentID != input.EnvironmentID || input.PlanID != task.PlanID ||
		proto.Unmarshal(projection.ComposeArtifact, artifact) != nil || executionplan.RejectUnknown(artifact) != nil ||
		artifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT || artifact.OwnerId != input.EnvironmentID {
		return nil, errs.New(errs.KindInternal, "Blueprint resolved runtime ownership is invalid")
	}
	digest := sha256.Sum256(artifact.CanonicalYaml)
	if !bytes.Equal(digest[:], artifact.YamlSha256) {
		return nil, errs.New(errs.KindInternal, "Blueprint resolved runtime digest diverges")
	}
	project, err := loadNormalizedEnvironmentProject(ctx, projection)
	if err != nil {
		return nil, err
	}
	// This local loader input is never published as desired state. It removes
	// renderer metadata while preserving the frozen runtime fields/resources.
	runtimeProjection := projection
	runtimeProjection.NormalizedCompose = artifact.CanonicalYaml
	runtime, err := loadNormalizedEnvironmentProject(ctx, runtimeProjection)
	if err != nil {
		return nil, err
	}
	nativeProjection := projection
	nativeProjection.Components = nil
	identities, err := ComposeIdentitySnapshotFromProjection(nativeProjection)
	if err != nil {
		return nil, err
	}
	for name, seal := range images {
		matched := 0
		for _, metadata := range artifact.Services {
			if metadata.ComposeName != name {
				continue
			}
			if metadata.OwnerComponentId != "" ||
				metadata.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
				return nil, errs.New(errs.KindInternal, "Blueprint candidate source is not unresolved native runtime")
			}
			for _, identity := range identities.Services {
				if identity.Name == name && identity.ID == metadata.ServiceId {
					matched++
				}
			}
		}
		if matched != 1 {
			return nil, errs.New(errs.KindInternal, "Blueprint candidate resolved runtime identity diverges")
		}
		service, err := runtime.GetService(name)
		if err != nil {
			return nil, errs.New(errs.KindInternal, "Blueprint candidate is absent from resolved runtime")
		}
		if err := applySealedWorkload(&service, seal); err != nil {
			return nil, err
		}
		if _, enabled := project.Services[name]; enabled {
			project.Services[name] = service
		} else if _, disabled := project.DisabledServices[name]; disabled {
			project.DisabledServices[name] = service
		} else {
			return nil, errs.New(errs.KindInternal, "Blueprint candidate is not an authored native Service")
		}
	}
	owners := make(map[string]string)
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if _, duplicate := owners[serviceID]; duplicate {
				return nil, errs.New(errs.KindInternal, "Blueprint generated Service ownership is duplicated")
			}
			owners[serviceID] = component.Desired.ID
		}
	}
	for _, service := range artifact.Services {
		owner, generated := owners[service.ServiceId]
		if !generated {
			if service.OwnerComponentId != "" {
				return nil, errs.New(errs.KindInternal, "Blueprint generated Service owner is absent")
			}
			continue
		}
		identity, err := pinnedComponentServiceIdentity(service, owner)
		if err != nil {
			return nil, err
		}
		for _, existing := range identities.Services {
			if existing.ID == identity.ID || existing.Name == identity.Name {
				return nil, errs.New(errs.KindInternal, "Blueprint generated Service collides with native identity")
			}
		}
		resolved, err := runtime.GetService(identity.Name)
		if err != nil {
			return nil, errs.New(errs.KindInternal, "Blueprint generated Service runtime is absent")
		}
		project.Services[identity.Name] = resolved
		identities.Services = append(identities.Services, identity)
		delete(owners, service.ServiceId)
	}
	if len(owners) != 0 {
		return nil, errs.New(errs.KindInternal, "Blueprint generated Service metadata is absent")
	}
	project.Networks, project.Volumes = runtime.Networks, runtime.Volumes
	project.Configs, project.Secrets = runtime.Configs, runtime.Secrets
	external, err := managedAttachExternalNetworks(project)
	if err != nil {
		return nil, err
	}
	sort.Slice(
		identities.Services,
		func(i, j int) bool { return identities.Services[i].Name < identities.Services[j].Name },
	)
	return RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: input.ArtifactID, ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID: input.TenantID, ProjectID: input.ProjectID, EnvironmentID: input.EnvironmentID,
		PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
		AuthorizedVolumeDir: input.AuthorizedVolumeDir, Identities: identities, ExternalNetworks: external, Releases: releases,
		RetainedComponentRuntime: projection.ComposeArtifact,
	})
}
