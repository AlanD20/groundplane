package taskplanning

import (
	"context"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// RenderRetainedServiceRuntime reconstructs only the sealed serving workload,
// its stable proxy, and any captured inactive blue/green workload.
func (resolver *TaskPlanResolver) RenderRetainedServiceRuntime(
	ctx context.Context,
	source releaserender.ServiceLifecycleRelease,
) ([]*agentpb.ComposeArtifact, error) {
	return resolver.renderRetainedServiceRuntimeArtifacts(ctx, source)
}
func (resolver *TaskPlanResolver) renderRetainedServiceRuntimeArtifacts(
	ctx context.Context,
	source releaserender.ServiceLifecycleRelease,
) ([]*agentpb.ComposeArtifact, error) {
	current, err := resolver.renderRetainedServiceRuntimeArtifact(ctx, source.Current, "", true)
	if err != nil {
		return nil, err
	}
	artifacts := []*agentpb.ComposeArtifact{current}
	if source.RetainedPrior != nil {
		prior, renderErr := resolver.renderRetainedServiceRuntimeArtifact(ctx, *source.RetainedPrior, "", false)
		if renderErr != nil {
			return nil, renderErr
		}
		artifacts = append(artifacts, prior)
	}
	return artifacts, nil
}

func (resolver *TaskPlanResolver) renderRetainedServiceRuntimeArtifact(
	ctx context.Context,
	source releaserender.ReleaseRenderInput,
	phase core.ServiceLifecyclePhase,
	includeProxy bool,
) (*agentpb.ComposeArtifact, error) {
	projection := releaseWorkloadProjection(source.Projection)
	selected := make([]servicerecord.EnvironmentServiceProjection, 0, 1)
	for _, service := range projection.DesiredServices {
		if service.Desired.ID == source.ServiceID {
			selected = append(selected, service)
		}
	}
	projection.DesiredServices = selected
	if len(selected) != 1 {
		return nil, errs.New(errs.KindInternal, "lifecycle source projection does not contain selected Service")
	}
	sourceTask := etcd.TaskRecord{PlanID: source.PlanID, RenderGeneration: int32(source.Projection.RenderGeneration)}
	identity := composerender.ComposeReleaseIdentity{
		ProxyAddresses: source.ProxyAddresses,
		ProxyImage:     source.ProxyImage, ReleaseID: source.ReleaseID, Target: source.CandidateTarget,
		Image: source.CandidateWorkload.LocalImageID, ServingReleaseID: source.ReleaseID,
		ServingTarget: source.CandidateTarget, ServingProxyGeneration: source.ProxyGeneration,
		Strategy: source.Strategy,
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifactForPhaseWithReleases(
		ctx,
		sourceTask,
		pinnedEnvironmentIdentity{
			TenantID: source.TenantID, TenantSlug: source.TenantSlug,
			ProjectID: source.ProjectID, ProjectSlug: source.ProjectSlug,
			EnvironmentID: source.EnvironmentID, EnvironmentName: source.EnvironmentName,
			AuthorizedVolumeDir: source.AuthorizedVolumeDir,
		},
		projection.RevisionID,
		source.ArtifactID,
		projection,
		phase,
		func(project *composetypes.Project, _ projectionrecord.EnvironmentComposeProjection) ([]composeidentity.Resource, error) {
			if err := projectReleaseWorkloadServices(project, projection); err != nil {
				return nil, err
			}
			service, active := project.Services[source.ServiceName]
			if !active {
				service = project.DisabledServices[source.ServiceName]
			}
			service.DependsOn = nil
			if err := composerender.ApplySealedWorkload(&service, source.CandidateWorkload); err != nil {
				return nil, err
			}
			if active {
				project.Services[source.ServiceName] = service
			} else {
				project.DisabledServices[source.ServiceName] = service
			}
			return managedAttachExternalNetworks(project)
		},
		map[string]composerender.ComposeReleaseIdentity{source.ServiceID: identity},
	)
	if err != nil {
		return nil, err
	}
	allowed := map[string]bool{}
	workloadName, err := domain.WorkloadComposeName(source.ServiceName, source.CandidateTarget)
	if err != nil {
		return nil, err
	}
	if len(source.ProxyPorts) == 0 {
		workloadName = source.ServiceName
	}
	allowed[workloadName] = true
	if includeProxy && len(source.ProxyPorts) != 0 {
		allowed[source.ServiceName] = true
	}
	return pruneServiceLifecycleArtifact(artifact, allowed)
}

// RetainBlueprintNativeRuntimeSources merges all physical members before
// replacing a Service, so retaining an inactive slot cannot erase its active slot.
func RetainBlueprintNativeRuntimeSources(
	current *agentpb.ComposeArtifact,
	sources []*agentpb.ComposeArtifact,
	serviceIDs []string,
) (*agentpb.ComposeArtifact, error) {
	if current == nil || len(sources) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint retained runtime sources are absent")
	}
	combined := proto.CloneOf(current)
	combined.Services, combined.Networks, combined.Volumes = nil, nil, nil
	root := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	services := composerender.EnsureMappingValue(root, "services")
	for _, source := range sources {
		if source == nil || source.OwnerKind != current.OwnerKind || source.OwnerId != current.OwnerId ||
			source.ProjectName != current.ProjectName ||
			source.AuthorizedVolumeDir != current.AuthorizedVolumeDir {
			return nil, errs.New(errs.KindStateConflict, "Blueprint retained source ownership changed")
		}
		var document yaml.Node
		if yaml.Unmarshal(source.CanonicalYaml, &document) != nil || len(document.Content) != 1 ||
			document.Content[0].Kind != yaml.MappingNode {
			return nil, errs.New(errs.KindValidationFailed, "Blueprint retained source YAML is invalid")
		}
		mapping, err := composerender.ServiceArtifactMapping(document.Content[0])
		if err != nil {
			return nil, err
		}
		references := make(map[string]map[string]bool)
		for _, service := range source.Services {
			index := composerender.MappingIndex(mapping, service.ComposeName)
			if service.OwnerComponentId != "" ||
				service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED ||
				index < 0 ||
				composerender.MappingIndex(services, service.ComposeName) >= 0 {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint retained physical member is duplicated or invalid",
				)
			}
			for _, existing := range combined.Services {
				if existing.ServiceId == service.ServiceId && existing.Role == service.Role &&
					service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained physical role is duplicated")
				}
			}
			composerender.AppendMappingValue(services, service.ComposeName, mapping.Content[index+1])
			if err := composerender.RetainedServiceResourceReferences(mapping.Content[index+1], references); err != nil {
				return nil, err
			}
			combined.Services = append(combined.Services, proto.CloneOf(service))
		}
		for _, section := range []string{"networks", "volumes", "configs", "secrets"} {
			index := composerender.MappingIndex(document.Content[0], section)
			if index < 0 {
				continue
			}
			prior := document.Content[0].Content[index+1]
			if prior.Kind != yaml.MappingNode {
				return nil, errs.New(errs.KindValidationFailed, "Blueprint retained resource is invalid")
			}
			next := composerender.EnsureMappingValue(root, section)
			for entry := 0; entry < len(prior.Content); entry += 2 {
				name, value := prior.Content[entry].Value, prior.Content[entry+1]
				if !references[section][name] {
					continue
				}
				existing := composerender.MappingIndex(next, name)
				if existing < 0 {
					composerender.AppendMappingValue(next, name, value)
					continue
				}
				if !composerender.SameComponentRuntimeNode(value, next.Content[existing+1]) {
					return nil, errs.New(errs.KindStateConflict, "Blueprint retained resource sources disagree")
				}
			}
		}
		combined.Networks = append(combined.Networks, source.Networks...)
		combined.Volumes = append(combined.Volumes, source.Volumes...)
	}
	var err error
	combined.CanonicalYaml, err = yaml.Marshal(root)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return composerender.RetainBlueprintNativeRuntime(current, combined, serviceIDs)
}
