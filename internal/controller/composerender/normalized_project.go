package composerender

import (
	"context"
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
	"strings"
)

// LoadNormalizedEnvironmentProject reconstructs the immutable authored Compose
// candidate stored at an Environment desired revision. The projection, not its
// optional Blueprint audit stream, is the desired-state authority.
func LoadNormalizedEnvironmentProject(
	ctx context.Context,
	projection etcd.EnvironmentComposeProjection,
) (*composetypes.Project, error) {
	return loadNormalizedEnvironmentProject(ctx, projection)
}

// MarshalNormalizedEnvironmentProject freezes the complete authored Compose
// topology before Components, Entries, Attaches, releases, or ownership
// metadata add execution-only state. Profile-disabled Services are folded back
// into the document so a later load can classify them deterministically.
func MarshalNormalizedEnvironmentProject(project *composetypes.Project) ([]byte, error) {
	if project == nil {
		return nil, errs.New(errs.KindInternal, "Environment normalized Compose project is missing")
	}
	normalized := *project
	normalized.Services = make(composetypes.Services, len(project.Services)+len(project.DisabledServices))
	for name, service := range project.Services {
		normalized.Services[name] = service
	}
	for name, service := range project.DisabledServices {
		if _, duplicate := normalized.Services[name]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment normalized Compose Service is duplicated")
		}
		normalized.Services[name] = service
	}
	normalized.DisabledServices = nil
	value, err := normalized.MarshalYAML()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

// NormalizedEnvironmentArtifact adapts the authored normalized Compose stream
// to the existing deterministic direct-mutation helpers. Runtime-only generated
// Component and release Service metadata is deliberately excluded.
func NormalizedEnvironmentArtifact(
	projection etcd.EnvironmentComposeProjection,
) (*agentpb.ComposeArtifact, error) {
	runtime := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, runtime); err != nil ||
		runtime.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		runtime.GetOwnerId() != projection.EnvironmentID ||
		ids.Validate(ids.KindConfig, runtime.GetArtifactId()) != nil {
		return nil, errs.New(errs.KindInternal, "Environment normalized Compose projection is corrupt")
	}
	project, err := loadNormalizedEnvironmentProject(context.Background(), projection)
	if err != nil {
		return nil, err
	}
	authoredNames := make(map[string]struct{}, len(project.Services)+len(project.DisabledServices))
	for name := range project.Services {
		authoredNames[name] = struct{}{}
	}
	for name := range project.DisabledServices {
		authoredNames[name] = struct{}{}
	}
	services := make([]*agentpb.ComposeService, 0, len(authoredNames))
	for _, desired := range projection.DesiredServices {
		identity := desired.Desired
		if _, authored := authoredNames[identity.Name]; !authored || ids.Validate(ids.KindService, identity.ID) != nil {
			return nil, errs.New(errs.KindInternal, "Environment normalized Compose Service identity is invalid")
		}
		services = append(services, &agentpb.ComposeService{ServiceId: identity.ID, ComposeName: identity.Name})
		delete(authoredNames, identity.Name)
	}
	if len(authoredNames) != 0 {
		return nil, errs.New(errs.KindInternal, "Environment normalized Compose Service identity is missing")
	}
	networks := make([]*agentpb.ComposeNetwork, len(projection.DesiredZones))
	for index, desired := range projection.DesiredZones {
		identity := desired.Desired
		dockerName, err := networkname.New(identity.ID)
		if err != nil {
			return nil, errs.New(errs.KindInternal, "Environment normalized Compose Network identity is invalid")
		}
		networks[index] = &agentpb.ComposeNetwork{
			NetworkId: identity.ID, ComposeName: identity.Name, DockerName: dockerName,
		}
	}
	volumes := make([]*agentpb.ComposeVolume, len(projection.Volumes))
	for index, identity := range projection.Volumes {
		volumes[index] = &agentpb.ComposeVolume{
			VolumeId: identity.ID, ComposeName: identity.Key, DockerName: "gp_vol_" + strings.ToLower(identity.ID),
		}
	}
	result := proto.Clone(runtime).(*agentpb.ComposeArtifact)
	result.CanonicalYaml = append([]byte(nil), projection.NormalizedCompose...)
	digest := sha256.Sum256(result.CanonicalYaml)
	result.YamlSha256 = digest[:]
	result.Services = services
	result.Networks = networks
	result.Volumes = volumes
	return result, nil
}

func loadNormalizedEnvironmentProject(
	ctx context.Context,
	projection etcd.EnvironmentComposeProjection,
) (*composetypes.Project, error) {
	if len(projection.NormalizedCompose) == 0 {
		return nil, errs.New(errs.KindInternal, "Environment normalized authored Compose is missing")
	}
	project, err := loader.LoadWithContext(ctx, composetypes.ConfigDetails{
		WorkingDir: "/",
		ConfigFiles: []composetypes.ConfigFile{{
			Filename: "compose.yaml", Content: append([]byte(nil), projection.NormalizedCompose...),
		}},
	}, func(options *loader.Options) {
		options.ResolvePaths = false
		options.SkipInterpolation = true
		options.SkipNormalization = true
		options.SkipResolveEnvironment = true
		options.SetProjectName("gp-"+strings.ToLower(projection.EnvironmentID), true)
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	for name, service := range project.Services {
		stripControllerLabels(service.Labels)
		service.Extensions = stripControllerServiceExtensions(service.Extensions)
		project.Services[name] = service
	}
	for name, service := range project.DisabledServices {
		stripControllerLabels(service.Labels)
		service.Extensions = stripControllerServiceExtensions(service.Extensions)
		project.DisabledServices[name] = service
	}
	ownedNetworks := make(map[string]struct{}, len(projection.DesiredZones))
	for _, network := range projection.DesiredZones {
		ownedNetworks[network.Desired.Name] = struct{}{}
	}
	for name, network := range project.Networks {
		if _, owned := ownedNetworks[name]; owned {
			network.Name = ""
			stripControllerLabels(network.Labels)
			delete(network.Extensions, composeResourceExtension)
		}
		project.Networks[name] = network
	}
	ownedVolumes := make(map[string]etcd.EnvironmentVolumeIdentity, len(projection.Volumes))
	for _, volume := range projection.Volumes {
		ownedVolumes[volume.Key] = volume
	}
	for name, volume := range project.Volumes {
		if identity, owned := ownedVolumes[name]; owned {
			volume.Name = ""
			volume.Driver = ""
			volume.DriverOpts = nil
			stripControllerLabels(volume.Labels)
			delete(volume.Extensions, composeResourceExtension)
			if volume.Extensions == nil {
				volume.Extensions = make(composetypes.Extensions)
			}
			volume.Extensions[ComposeVolumeSlugExtension] = identity.Slug
		}
		project.Volumes[name] = volume
	}
	return project, nil
}

func stripControllerServiceExtensions(extensions composetypes.Extensions) composetypes.Extensions {
	consumed := false
	for _, key := range []string{composeResourceExtension, "x-gp-release"} {
		if _, recognized := extensions[key]; recognized {
			delete(extensions, key)
			consumed = true
		}
	}
	if consumed && len(extensions) == 0 {
		return nil
	}
	return extensions
}

func stripControllerLabels(labels composetypes.Labels) {
	for key := range labels {
		if len(key) >= len("com.groundplane.") && key[:len("com.groundplane.")] == "com.groundplane." {
			delete(labels, key)
		}
	}
}

func ApplyProjectedServiceDependencyPhase(
	project *composetypes.Project,
	plans core.ServiceDependencyPlans,
	phase core.ServiceLifecyclePhase,
) error {
	if phase == "" || phase == core.ServiceLifecycleStart {
		return nil
	}
	plan := plans.DeployDependencyPlan
	if phase == core.ServiceLifecycleRollback {
		plan = plans.RollbackDependencyPlan
	} else if phase != core.ServiceLifecycleDeploy {
		return errs.New(errs.KindInternal, "Service dependency projection phase is invalid")
	}
	for _, edge := range plan.Edges {
		service, enabled := project.Services[edge.Service]
		if !enabled {
			service = project.DisabledServices[edge.Service]
		}
		if service.Name == "" {
			return errs.New(errs.KindInternal, "Service dependency projection target is absent")
		}
		if service.DependsOn == nil {
			service.DependsOn = make(map[string]composetypes.ServiceDependency)
		}
		dependency := composetypes.ServiceDependency{Condition: edge.Condition.String(), Required: true}
		if current, exists := service.DependsOn[edge.Dependency]; exists &&
			(current.Condition != dependency.Condition || current.Required != dependency.Required) {
			return errs.New(errs.KindValidationFailed, "Service dependency phase conflicts with native Compose")
		}
		service.DependsOn[edge.Dependency] = dependency
		if enabled {
			project.Services[edge.Service] = service
		} else {
			project.DisabledServices[edge.Service] = service
		}
	}
	return nil
}
