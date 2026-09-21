package taskplanning

import (
	"context"
	"crypto/sha256"
	composeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	composerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"maps"
	"sort"
	"strings"

	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type environmentComposeTransform func(
	*composetypes.Project,
	projectionrecord.EnvironmentComposeProjection,
) ([]composeidentity.Resource, error)

// ProjectAttachNetworks replaces the reserved Attach overlay on an owned
// Compose project with the supplied complete network-membership union.
func ProjectAttachNetworks(
	project *composetypes.Project,
	projection projectionrecord.EnvironmentComposeProjection,
	joins []etcd.AttachTaskNetworkJoin,
) ([]composeidentity.Resource, error) {
	if project == nil || ids.Validate(ids.KindEnvironment, projection.EnvironmentID) != nil {
		return nil, errs.New(errs.KindInternal, "Attach network projection input is invalid")
	}
	removeManagedAttachNetworks(project)
	identities, err := composerender.ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		return nil, err
	}
	serviceNames := make(map[string]string, len(identities.Services))
	for _, service := range identities.Services {
		serviceNames[service.ID] = service.Name
	}
	if project.Networks == nil {
		project.Networks = make(composetypes.Networks)
	}
	external := make([]composeidentity.Resource, 0, len(joins))
	for _, join := range joins {
		composeName := "gp_attach_" + strings.ToLower(join.NetworkID)
		if ids.Validate(ids.KindNetwork, join.NetworkID) != nil {
			return nil, errs.New(errs.KindInternal, "Attach network projection contains an invalid network")
		}
		if _, exists := project.Networks[composeName]; exists {
			return nil, errs.New(
				errs.KindValidationFailed,
				"managed Attach network conflicts with authored Compose",
			)
		}
		project.Networks[composeName] = composetypes.NetworkConfig{External: true}
		external = append(external, composeidentity.Resource{ID: join.NetworkID, Name: composeName})
		for _, serviceID := range join.ServiceIDs {
			serviceName, exists := serviceNames[serviceID]
			if !exists {
				return nil, errs.New(
					errs.KindInternal,
					"Attach network consumer is absent from the pinned projection",
				)
			}
			service, active := project.Services[serviceName]
			if !active {
				service, exists = project.DisabledServices[serviceName]
			}
			if !exists {
				return nil, errs.New(errs.KindInternal, "Attach network consumer is absent from the Blueprint")
			}
			if service.NetworkMode != "" {
				return nil, errs.New(
					errs.KindValidationFailed,
					"Attach network conflicts with service network_mode",
				)
			}
			if service.Networks == nil {
				service.Networks = make(map[string]*composetypes.ServiceNetworkConfig)
			}
			if _, exists := service.Networks[composeName]; exists {
				return nil, errs.New(errs.KindValidationFailed, "managed Attach network membership is duplicated")
			}
			service.Networks[composeName] = &composetypes.ServiceNetworkConfig{}
			if active {
				project.Services[serviceName] = service
			} else {
				project.DisabledServices[serviceName] = service
			}
		}
	}
	sort.Slice(external, func(i, j int) bool { return external[i].Name < external[j].Name })
	return external, nil
}

// MutateAttachNetworkArtifact applies a complete Attach union to an already
// captured runtime artifact. Native physical names and Entry bindings are kept
// byte-for-byte except for the managed network overlay. Historical serving
// ownership stays bound to the Release that created each native member.
func MutateAttachNetworkArtifact(
	ctx context.Context,
	current *agentpb.ComposeArtifact,
	projection projectionrecord.EnvironmentComposeProjection,
	joins []etcd.AttachTaskNetworkJoin,
	artifactID string,
) (*agentpb.ComposeArtifact, error) {
	if ctx == nil || current == nil ||
		current.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		current.GetOwnerId() != projection.EnvironmentID ||
		ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "Attach runtime artifact mutation input is invalid")
	}
	project, err := loader.LoadWithContext(ctx, composetypes.ConfigDetails{
		WorkingDir:  "/",
		ConfigFiles: []composetypes.ConfigFile{{Filename: "compose.yaml", Content: current.GetCanonicalYaml()}},
	}, func(options *loader.Options) {
		options.ResolvePaths = false
		options.SkipInterpolation = true
		options.SkipNormalization = true
		options.SkipResolveEnvironment = true
		options.SetProjectName(current.GetProjectName(), true)
	})
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if err := projectAttachRuntimeNetworks(project, projection, current, joins); err != nil {
		return nil, err
	}
	// Serialize the complete topology; profiles still control later execution.
	// The loader separates disabled Services, but MarshalYAML omits that map.
	maps.Copy(project.Services, project.DisabledServices)
	canonical, err := project.MarshalYAML()
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	owned := proto.CloneOf(current)
	owned.ArtifactId = artifactID
	owned.CanonicalYaml = canonical
	digest := sha256.Sum256(canonical)
	owned.YamlSha256 = digest[:]
	return owned, nil
}

func projectAttachRuntimeNetworks(
	project *composetypes.Project,
	projection projectionrecord.EnvironmentComposeProjection,
	artifact *agentpb.ComposeArtifact,
	joins []etcd.AttachTaskNetworkJoin,
) error {
	if project == nil || artifact == nil {
		return errs.New(errs.KindInternal, "Attach runtime network projection is invalid")
	}
	removeManagedAttachNetworks(project)
	allowed := make(map[string]bool, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		allowed[service.Desired.ID] = true
	}
	targets := make(map[string][]string, len(allowed))
	for _, service := range artifact.GetServices() {
		if service == nil || !allowed[service.GetServiceId()] ||
			service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			continue
		}
		name := service.GetComposeName()
		if _, exists := project.Services[name]; !exists {
			if _, disabled := project.DisabledServices[name]; !disabled {
				return errs.New(errs.KindInternal, "Attach runtime Service metadata is incomplete")
			}
		}
		targets[service.GetServiceId()] = append(targets[service.GetServiceId()], name)
	}
	if project.Networks == nil {
		project.Networks = make(composetypes.Networks)
	}
	for _, join := range joins {
		if ids.Validate(ids.KindNetwork, join.NetworkID) != nil {
			return errs.New(errs.KindInternal, "Attach runtime network identity is invalid")
		}
		composeName := "gp_attach_" + strings.ToLower(join.NetworkID)
		if _, exists := project.Networks[composeName]; exists {
			return errs.New(errs.KindValidationFailed, "managed Attach network conflicts with captured runtime")
		}
		dockerName, err := networkname.New(join.NetworkID)
		if err != nil {
			return err
		}
		project.Networks[composeName] = composetypes.NetworkConfig{Name: dockerName, External: true}
		for _, serviceID := range join.ServiceIDs {
			if !allowed[serviceID] || len(targets[serviceID]) == 0 {
				return errs.New(errs.KindInternal, "Attach runtime consumer is absent")
			}
			for _, name := range targets[serviceID] {
				service, active := project.Services[name]
				if !active {
					service = project.DisabledServices[name]
				}
				if service.NetworkMode != "" {
					return errs.New(errs.KindValidationFailed, "Attach network conflicts with service network_mode")
				}
				if service.Networks == nil {
					service.Networks = make(map[string]*composetypes.ServiceNetworkConfig)
				}
				service.Networks[composeName] = &composetypes.ServiceNetworkConfig{}
				if active {
					project.Services[name] = service
				} else {
					project.DisabledServices[name] = service
				}
			}
		}
	}
	return nil
}

func removeManagedAttachNetworks(project *composetypes.Project) {
	managed := make(map[string]struct{})
	for name := range project.Networks {
		if _, ok := managedAttachNetworkID(name); ok {
			managed[name] = struct{}{}
			delete(project.Networks, name)
		}
	}
	removeMemberships := func(services composetypes.Services) {
		for name, service := range services {
			if len(service.Networks) == 0 {
				continue
			}
			networks := make(map[string]*composetypes.ServiceNetworkConfig, len(service.Networks))
			for network, config := range service.Networks {
				if _, remove := managed[network]; !remove {
					networks[network] = config
				}
			}
			service.Networks = networks
			services[name] = service
		}
	}
	removeMemberships(project.Services)
	removeMemberships(project.DisabledServices)
}

func managedAttachExternalNetworks(project *composetypes.Project) ([]composeidentity.Resource, error) {
	external := make([]composeidentity.Resource, 0)
	for name, network := range project.Networks {
		networkID, managed := managedAttachNetworkID(name)
		if !managed {
			continue
		}
		if !network.External {
			return nil, errs.New(errs.KindInternal, "managed Attach network is not external")
		}
		external = append(external, composeidentity.Resource{ID: networkID, Name: name})
	}
	sort.Slice(external, func(left int, right int) bool { return external[left].Name < external[right].Name })
	return external, nil
}

func managedAttachNetworkID(composeName string) (string, bool) {
	const prefix = "gp_attach_net_"
	if !strings.HasPrefix(composeName, prefix) {
		return "", false
	}
	networkID := "net_" + strings.ToUpper(strings.TrimPrefix(composeName, prefix))
	return networkID, ids.Validate(ids.KindNetwork, networkID) == nil
}
