package controller

import (
	"path/filepath"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// EnvironmentComponentEnvironmentFile is one service-specific generated env
// file. Values contain immutable Entry identities only; value bytes are
// resolved into the transient materialization channel when a Task is sent.
type EnvironmentComponentEnvironmentFile struct {
	ComponentID string
	ServiceID   string
	ServiceName string
	Destination string
	Values      []components.GeneratedSecretEnvironment
}

// EnvironmentComponentComposeProjection is the complete deterministic
// addition made by enabled Environment components to one parsed Compose
// project. Project is an owned clone; the caller's parsed project is unchanged.
type EnvironmentComponentComposeProjection struct {
	Project          *composetypes.Project
	Services         []ComposeResourceIdentity
	PlainFiles       []GeneratedEnvironmentFile
	EnvironmentFiles []EnvironmentComponentEnvironmentFile
}

// ProjectEnvironmentComponents renders the compiled-in Environment component
// catalog and projects its services into the same Compose namespace as authored
// services. It does not resolve secrets or construct Task identities.
func ProjectEnvironmentComponents(
	project *composetypes.Project,
	environment core.Environment,
	catalog []components.Registration,
) (EnvironmentComponentComposeProjection, error) {
	if project == nil || ids.Validate(ids.KindEnvironment, environment.ID) != nil ||
		!filepath.IsAbs(environment.VolumeDir) || filepath.Clean(environment.VolumeDir) != environment.VolumeDir ||
		filepath.Base(environment.VolumeDir) != environment.ID {
		return EnvironmentComponentComposeProjection{}, errs.New(
			errs.KindInternal,
			"Environment Component Compose input is invalid",
		)
	}
	rendered, err := RenderEnvironmentComponents(environment, catalog)
	if err != nil {
		return EnvironmentComponentComposeProjection{}, err
	}
	projected := cloneComposeProjectServices(project)
	result := EnvironmentComponentComposeProjection{Project: projected}
	mountedFiles := make(map[string]map[string]struct{})

	for _, generated := range rendered.Services {
		if _, collision := projected.Services[generated.Name]; collision {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"Component generated Service conflicts with an active Compose service",
			)
		}
		if _, collision := projected.DisabledServices[generated.Name]; collision {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindNameConflict,
				"Component generated Service conflicts with a disabled Compose service",
			)
		}
		service, environmentFile, err := projectEnvironmentComponentService(environment, generated)
		if err != nil {
			return EnvironmentComponentComposeProjection{}, err
		}
		for zoneName := range service.Networks {
			if _, exists := projected.Networks[zoneName]; !exists {
				return EnvironmentComponentComposeProjection{}, errs.New(
					errs.KindInternal,
					"Component generated Service Zone is absent from the Compose project",
				)
			}
		}
		for _, mount := range generated.Definition.Mounts {
			owners := mountedFiles[mount.Source]
			if owners == nil {
				owners = make(map[string]struct{})
				mountedFiles[mount.Source] = owners
			}
			owners[generated.ComponentID] = struct{}{}
		}
		projected.Services[generated.Name] = service
		result.Services = append(result.Services, ComposeResourceIdentity{
			ID: generated.Definition.Service.ID, Name: generated.Name,
		})
		if environmentFile != nil {
			result.EnvironmentFiles = append(result.EnvironmentFiles, *environmentFile)
		}
	}
	for _, identity := range result.Services {
		for dependency := range projected.Services[identity.Name].DependsOn {
			if _, exists := projected.Services[dependency]; !exists {
				return EnvironmentComponentComposeProjection{}, errs.New(
					errs.KindInternal,
					"Component generated Service dependency is absent from the Compose project",
				)
			}
		}
	}

	for _, file := range rendered.Files {
		owners := mountedFiles[file.Path]
		if _, mounted := owners[file.ComponentID]; !mounted ||
			uint64(len(file.Content)) > entrymaterialization.MaximumContentBytes ||
			entrymaterialization.ValidateDesiredDestination(file.Path) != nil {
			return EnvironmentComponentComposeProjection{}, errs.New(
				errs.KindValidationFailed,
				"Component generated file is not a bounded mounted output",
			)
		}
		result.PlainFiles = append(result.PlainFiles, GeneratedEnvironmentFile{
			ComponentID: file.ComponentID,
			Path:        file.Path,
			Content:     append([]byte(nil), file.Content...),
		})
	}
	return result, nil
}

func cloneComposeProjectServices(project *composetypes.Project) *composetypes.Project {
	cloned := *project
	cloned.Services = make(composetypes.Services, len(project.Services))
	for name, service := range project.Services {
		cloned.Services[name] = service
	}
	cloned.DisabledServices = make(composetypes.Services, len(project.DisabledServices))
	for name, service := range project.DisabledServices {
		cloned.DisabledServices[name] = service
	}
	return &cloned
}

func projectEnvironmentComponentService(
	environment core.Environment,
	generated GeneratedEnvironmentService,
) (composetypes.ServiceConfig, *EnvironmentComponentEnvironmentFile, error) {
	definition := generated.Definition
	service := definition.Service
	if generatedServiceHasUnsupportedComposeFields(service) {
		return composetypes.ServiceConfig{}, nil, errs.New(
			errs.KindInternal,
			"Component renderer used a Service field outside the Environment component Compose contract",
		)
	}
	projected := composetypes.ServiceConfig{
		Name: service.Name, Image: service.Image,
		Command:  composetypes.ShellCommand(append([]string(nil), service.Command...)),
		Networks: make(map[string]*composetypes.ServiceNetworkConfig, len(service.Zones)),
		Expose:   composetypes.StringOrNumberList(append([]string(nil), service.Expose...)),
		Restart:  service.Restart,
	}
	joinedZones := make(map[string]struct{}, len(service.Zones))
	for _, zoneName := range service.Zones {
		if _, exists := environment.Zones[zoneName]; !exists {
			return composetypes.ServiceConfig{}, nil, errs.New(
				errs.KindInternal,
				"Component generated Service references a missing Environment Zone",
			)
		}
		projected.Networks[zoneName] = &composetypes.ServiceNetworkConfig{
			Aliases:     append([]string(nil), service.Aliases[zoneName]...),
			Ipv4Address: definition.StaticIPv4[zoneName],
		}
		joinedZones[zoneName] = struct{}{}
	}
	for zoneName := range service.Aliases {
		if _, joined := joinedZones[zoneName]; !joined {
			return composetypes.ServiceConfig{}, nil, errs.New(
				errs.KindInternal,
				"Component generated Service alias targets an unjoined Zone",
			)
		}
	}
	for zoneName := range definition.StaticIPv4 {
		if _, joined := joinedZones[zoneName]; !joined {
			return composetypes.ServiceConfig{}, nil, errs.New(
				errs.KindInternal,
				"Component generated Service address targets an unjoined Zone",
			)
		}
	}
	for _, mount := range definition.Mounts {
		projected.Volumes = append(projected.Volumes, composetypes.ServiceVolumeConfig{
			Type:   composetypes.VolumeTypeBind,
			Source: filepath.Join(environment.VolumeDir, filepath.FromSlash(mount.Source)),
			Target: mount.Target, ReadOnly: mount.ReadOnly,
			Bind: &composetypes.ServiceVolumeBind{CreateHostPath: true},
		})
	}
	if len(service.DependsOn) != 0 {
		projected.DependsOn = make(composetypes.DependsOnConfig, len(service.DependsOn))
		for name, dependency := range service.DependsOn {
			if len(dependency.Phases) != 0 {
				return composetypes.ServiceConfig{}, nil, errs.New(
					errs.KindInternal,
					"Component generated Service dependency contains unsupported lifecycle phases",
				)
			}
			projected.DependsOn[name] = composetypes.ServiceDependency{
				Condition: dependency.Condition, Required: true,
			}
		}
	}
	replicas := service.Replicas
	projected.Deploy = &composetypes.DeployConfig{Replicas: &replicas}

	if len(definition.SecretEnvironment) == 0 {
		return projected, nil, nil
	}
	destination := ServiceEnvFileName(environment.ID, service.Name)
	if entrymaterialization.ValidateDesiredDestination(destination) != nil {
		return composetypes.ServiceConfig{}, nil, errs.New(
			errs.KindInternal,
			"Component generated Environment destination is invalid",
		)
	}
	projected.EnvFiles = []composetypes.EnvFile{{
		Path: filepath.Join(environment.VolumeDir, filepath.FromSlash(destination)), Required: true,
	}}
	values := append([]components.GeneratedSecretEnvironment(nil), definition.SecretEnvironment...)
	sort.Slice(values, func(left, right int) bool { return values[left].Name < values[right].Name })
	return projected, &EnvironmentComponentEnvironmentFile{
		ComponentID: generated.ComponentID,
		ServiceID:   service.ID, ServiceName: service.Name, Destination: destination,
		Values: values,
	}, nil
}

func generatedServiceHasUnsupportedComposeFields(service core.Service) bool {
	return service.Strategy != "" || service.OnFailure != "" || service.Healthcheck != (core.Healthcheck{}) ||
		service.Resources != (core.Resources{}) || len(service.Mounts) != 0 || len(service.Environment) != 0 ||
		service.Logging.MaxSize != "" || service.Logging.MaxFile != 0 || service.Adapter != "" ||
		service.FactsPrefix != "" || service.Label != ""
}
