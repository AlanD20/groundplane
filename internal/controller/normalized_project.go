package controller

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/compose-spec/compose-go/v2/loader"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

func loadNormalizedEnvironmentProject(
	ctx context.Context,
	projection etcd.EnvironmentComposeProjection,
) (*composetypes.Project, error) {
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(projection.ComposeArtifact, artifact); err != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != projection.EnvironmentID || ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil {
		return nil, errs.New(errs.KindInternal, "Environment normalized Compose artifact is corrupt")
	}
	project, err := loader.LoadWithContext(ctx, composetypes.ConfigDetails{
		WorkingDir: "/",
		ConfigFiles: []composetypes.ConfigFile{{
			Filename: "compose.yaml", Content: append([]byte(nil), artifact.GetCanonicalYaml()...),
		}},
	}, func(options *loader.Options) {
		options.ResolvePaths = false
		options.SetProjectName(artifact.GetProjectName(), true)
	})
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Environment normalized Compose project is corrupt")
	}
	for name, service := range project.Services {
		stripControllerLabels(service.Labels)
		delete(service.Extensions, composeResourceExtension)
		project.Services[name] = service
	}
	for name, service := range project.DisabledServices {
		stripControllerLabels(service.Labels)
		delete(service.Extensions, composeResourceExtension)
		project.DisabledServices[name] = service
	}
	ownedNetworks := make(map[string]struct{}, len(artifact.GetNetworks()))
	for _, network := range artifact.GetNetworks() {
		ownedNetworks[network.GetComposeName()] = struct{}{}
	}
	for name, network := range project.Networks {
		if _, owned := ownedNetworks[name]; owned {
			network.Name = ""
			stripControllerLabels(network.Labels)
			delete(network.Extensions, composeResourceExtension)
		}
		project.Networks[name] = network
	}
	ownedVolumes := make(map[string]struct{}, len(artifact.GetVolumes()))
	for _, volume := range artifact.GetVolumes() {
		ownedVolumes[volume.GetComposeName()] = struct{}{}
	}
	for name, volume := range project.Volumes {
		if _, owned := ownedVolumes[name]; owned {
			volume.Name = ""
			volume.Driver = ""
			volume.DriverOpts = nil
			stripControllerLabels(volume.Labels)
			delete(volume.Extensions, composeResourceExtension)
		}
		project.Volumes[name] = volume
	}
	return project, nil
}

func stripControllerLabels(labels composetypes.Labels) {
	for key := range labels {
		if len(key) >= len("com.groundplane.") && key[:len("com.groundplane.")] == "com.groundplane." {
			delete(labels, key)
		}
	}
}

func applyProjectedServiceDependencyPhase(
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
