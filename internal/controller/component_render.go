package controller

import (
	"path"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/components"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GeneratedEnvironmentService ties one rendered Compose service to the stable
// Component that owns its runtime identity.
type GeneratedEnvironmentService struct {
	ComponentID string
	Name        string
	Definition  components.GeneratedService
}

// GeneratedEnvironmentFile is one Controller-rendered path beneath the
// Environment's authorized volume root.
type GeneratedEnvironmentFile struct {
	ComponentID string
	Path        string
	Content     []byte
}

// EnvironmentComponentRender is the deterministic output of every enabled
// EnvironmentRender registration in one Environment.
type EnvironmentComponentRender struct {
	Services []GeneratedEnvironmentService
	Files    []GeneratedEnvironmentFile
}

// RenderEnvironmentComponents invokes the compiled-in component catalog
// without branching on concrete kinds and rejects collisions before task
// construction or secret materialization.
func RenderEnvironmentComponents(
	environment core.Environment,
	catalog []components.Registration,
) (EnvironmentComponentRender, error) {
	if ids.Validate(ids.KindEnvironment, environment.ID) != nil {
		return EnvironmentComponentRender{}, errs.New(errs.KindInternal, "Component render Environment is invalid")
	}
	registrations, err := environmentComponentRegistrations(catalog)
	if err != nil {
		return EnvironmentComponentRender{}, err
	}
	entries, err := environmentSecretEntries(environment.Entries)
	if err != nil {
		return EnvironmentComponentRender{}, err
	}
	authoredServiceNames := make(map[string]struct{}, len(environment.Services))
	serviceIDs := make(map[string]struct{}, len(environment.Services))
	for name, service := range environment.Services {
		if name == "" || service.Name != name || ids.Validate(ids.KindService, service.ID) != nil {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Component render authored Service projection is invalid",
			)
		}
		authoredServiceNames[name] = struct{}{}
		serviceIDs[service.ID] = struct{}{}
	}

	componentsByKind := make(map[core.ComponentKind]core.Component, len(environment.Components))
	componentIDs := make(map[string]struct{}, len(environment.Components))
	for _, component := range environment.Components {
		registration, exists := registrations[component.Kind]
		if !exists || !registration.Allows(core.ComponentOwnerEnvironment) || registration.Environment == nil ||
			component.Owner != core.ComponentOwnerEnvironment || component.OwnerID != environment.ID ||
			ids.Validate(ids.KindComponent, component.ID) != nil || component.Validate() != nil {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindValidationFailed,
				"Environment Component is not supported by the compiled catalog",
			)
		}
		if _, duplicate := componentsByKind[component.Kind]; duplicate {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Environment Component projection repeats a kind",
			)
		}
		if _, duplicate := componentIDs[component.ID]; duplicate {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Environment Component projection repeats an id",
			)
		}
		componentsByKind[component.Kind] = component
		componentIDs[component.ID] = struct{}{}
	}

	result := EnvironmentComponentRender{}
	generatedServiceNames := make(map[string]struct{})
	generatedFilePaths := make(map[string]struct{})
	kinds := make([]core.ComponentKind, 0, len(componentsByKind))
	for kind := range componentsByKind {
		kinds = append(kinds, kind)
	}
	sort.Slice(kinds, func(left, right int) bool { return kinds[left] < kinds[right] })
	for _, kind := range kinds {
		component := componentsByKind[kind]
		services, files, renderErr := registrations[kind].Environment.Render(environment, component)
		if renderErr != nil {
			return EnvironmentComponentRender{}, renderErr
		}
		if !component.Enabled && (len(services) != 0 || len(files) != 0) {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"disabled Environment Component rendered runtime resources",
			)
		}
		generatedIDs := make(map[string]struct{}, len(component.GeneratedServices))
		for _, serviceID := range component.GeneratedServices {
			if ids.Validate(ids.KindService, serviceID) != nil {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component generated Service identity is invalid",
				)
			}
			generatedIDs[serviceID] = struct{}{}
		}
		for name, service := range services {
			if err := validateGeneratedEnvironmentService(
				environment,
				component,
				name,
				service,
				entries,
			); err != nil {
				return EnvironmentComponentRender{}, err
			}
			if _, exists := generatedIDs[service.Service.ID]; !exists {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component renderer emitted an unowned Service identity",
				)
			}
			if _, collision := authoredServiceNames[name]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component generated Service name conflicts with an authored Service",
				)
			}
			if _, collision := generatedServiceNames[name]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component render repeats a generated Service name",
				)
			}
			if _, collision := serviceIDs[service.Service.ID]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component generated Service id is already in use",
				)
			}
			generatedServiceNames[name] = struct{}{}
			serviceIDs[service.Service.ID] = struct{}{}
			result.Services = append(result.Services, GeneratedEnvironmentService{
				ComponentID: component.ID,
				Name:        name,
				Definition:  service,
			})
		}
		if component.Enabled && len(services) != len(generatedIDs) {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Component renderer did not cover every generated Service identity",
			)
		}
		for filePath, content := range files {
			if !validGeneratedRelativePath(filePath) {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindValidationFailed,
					"Component generated file path is invalid",
				)
			}
			if _, collision := generatedFilePaths[filePath]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component render repeats a generated file path",
				)
			}
			generatedFilePaths[filePath] = struct{}{}
			result.Files = append(result.Files, GeneratedEnvironmentFile{
				ComponentID: component.ID,
				Path:        filePath,
				Content:     append([]byte(nil), content...),
			})
		}
	}
	sort.Slice(result.Services, func(left, right int) bool {
		return result.Services[left].Name < result.Services[right].Name
	})
	sort.Slice(result.Files, func(left, right int) bool { return result.Files[left].Path < result.Files[right].Path })
	return result, nil
}

func environmentComponentRegistrations(
	catalog []components.Registration,
) (map[core.ComponentKind]components.Registration, error) {
	registrations := make(map[core.ComponentKind]components.Registration)
	for _, registration := range catalog {
		if registration.ApplyStrategy != components.EnvironmentRender {
			continue
		}
		if registration.Kind == "" || registration.Environment == nil ||
			!registration.Allows(core.ComponentOwnerEnvironment) {
			return nil, errs.New(errs.KindInternal, "Environment Component registration is invalid")
		}
		if _, duplicate := registrations[registration.Kind]; duplicate {
			return nil, errs.New(errs.KindInternal, "Environment Component catalog repeats a kind")
		}
		registrations[registration.Kind] = registration
	}
	return registrations, nil
}

func environmentSecretEntries(entries []core.EnvEntry) (map[string]core.EnvEntry, error) {
	indexed := make(map[string]core.EnvEntry, len(entries))
	for _, entry := range entries {
		if entry.Validate() != nil || ids.Validate(ids.KindEnvEntry, entry.ID) != nil {
			return nil, errs.New(errs.KindInternal, "Component render Entry projection is invalid")
		}
		if _, duplicate := indexed[entry.ID]; duplicate {
			return nil, errs.New(errs.KindInternal, "Component render Entry projection repeats an id")
		}
		indexed[entry.ID] = entry
	}
	return indexed, nil
}

func validateGeneratedEnvironmentService(
	environment core.Environment,
	component core.Component,
	name string,
	generated components.GeneratedService,
	entries map[string]core.EnvEntry,
) error {
	if name == "" || generated.Service.Name != name || ids.Validate(ids.KindService, generated.Service.ID) != nil ||
		generated.Service.Validate() != nil {
		return errs.New(errs.KindInternal, "Component renderer emitted an invalid Service")
	}
	for _, zoneName := range generated.Service.Zones {
		zone, exists := environment.Zones[zoneName]
		if !exists || zone.Name != zoneName {
			return errs.New(errs.KindInternal, "Component renderer referenced an unknown Zone")
		}
	}
	for zoneName, address := range generated.StaticIPv4 {
		if _, exists := environment.Zones[zoneName]; !exists || address == "" {
			return errs.New(errs.KindInternal, "Component renderer emitted an invalid static address binding")
		}
	}
	mountTargets := make(map[string]struct{}, len(generated.Mounts))
	for _, mount := range generated.Mounts {
		if !validGeneratedRelativePath(mount.Source) || !path.IsAbs(mount.Target) ||
			path.Clean(mount.Target) != mount.Target || mount.Target == "/" {
			return errs.New(errs.KindValidationFailed, "Component renderer emitted an unsafe mount")
		}
		if _, duplicate := mountTargets[mount.Target]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component renderer repeated a mount target")
		}
		mountTargets[mount.Target] = struct{}{}
	}
	secretNames := make(map[string]struct{}, len(generated.SecretEnvironment))
	for _, binding := range generated.SecretEnvironment {
		entry, exists := entries[binding.EntryID]
		if !validGeneratedEnvironmentVariable(binding.Name) || !exists || !entry.Secret {
			return errs.New(errs.KindValidationFailed, "Component renderer emitted an invalid secret binding")
		}
		if _, duplicate := secretNames[binding.Name]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component renderer repeated a secret binding")
		}
		secretNames[binding.Name] = struct{}{}
	}
	if component.Enabled && len(generated.Service.Zones) == 0 {
		return errs.New(errs.KindValidationFailed, "enabled Environment Component Service must join a Zone")
	}
	return nil
}

func validGeneratedRelativePath(value string) bool {
	return value != "" && value != "." && !path.IsAbs(value) && path.Clean(value) == value &&
		!strings.HasPrefix(value, "../")
}

func validGeneratedEnvironmentVariable(value string) bool {
	if value == "" || !asciiEnvironmentVariableStart(value[0]) {
		return false
	}
	for index := 1; index < len(value); index++ {
		if !asciiEnvironmentVariableStart(value[index]) && (value[index] < '0' || value[index] > '9') {
			return false
		}
	}
	return true
}

func asciiEnvironmentVariableStart(value byte) bool {
	return value == '_' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}
