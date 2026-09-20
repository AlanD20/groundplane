package componentrender

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
)

func RenderEnvironmentComponents(
	environment core.Environment,
	catalog []EnvironmentComponentRegistration,
) (EnvironmentComponentRender, error) {
	if ids.Validate(ids.KindEnvironment, environment.ID) != nil {
		return EnvironmentComponentRender{}, errs.New(errs.KindInternal, "Component render Environment is invalid")
	}
	if err := ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return EnvironmentComponentRender{}, err
	}
	registrations := make(map[core.ComponentKind]EnvironmentComponentRegistration, len(catalog))
	for _, registration := range catalog {
		registrations[registration.Kind] = registration
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
	for _, instance := range environment.Components {
		if _, exists := registrations[instance.Kind]; !exists ||
			instance.Owner != core.ComponentOwnerEnvironment || instance.OwnerID != environment.ID ||
			ids.Validate(ids.KindComponent, instance.ID) != nil || instance.Validate() != nil {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindValidationFailed,
				"Environment Component is not supported by the compiled catalog",
			)
		}
		if _, duplicate := componentsByKind[instance.Kind]; duplicate {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Environment Component projection repeats a kind",
			)
		}
		if _, duplicate := componentIDs[instance.ID]; duplicate {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Environment Component projection repeats an id",
			)
		}
		componentsByKind[instance.Kind] = instance
		componentIDs[instance.ID] = struct{}{}
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
		instance := componentsByKind[kind]
		plan, err := registrations[kind].Plan(environment, instance)
		if err != nil {
			return EnvironmentComponentRender{}, err
		}
		plan = componentsdk.CloneEnvironmentPlan(plan)
		if !instance.Enabled && (len(plan.Services) != 0 || len(plan.Files) != 0) {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"disabled Environment Component planned runtime resources",
			)
		}
		generatedIDs := make(map[string]struct{}, len(instance.GeneratedServices))
		for _, serviceID := range instance.GeneratedServices {
			if ids.Validate(ids.KindService, serviceID) != nil {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component generated Service identity is invalid",
				)
			}
			generatedIDs[serviceID] = struct{}{}
		}
		for _, service := range plan.Services {
			if err := validateGeneratedEnvironmentService(environment, instance, service); err != nil {
				return EnvironmentComponentRender{}, err
			}
			if _, exists := generatedIDs[service.ID]; !exists {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component planner emitted an unowned Service identity",
				)
			}
			if _, collision := authoredServiceNames[service.Name]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component generated Service name conflicts with an authored Service",
				)
			}
			if _, collision := generatedServiceNames[service.Name]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component plan repeats a generated Service name",
				)
			}
			if _, collision := serviceIDs[service.ID]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindInternal,
					"Component generated Service id is already in use",
				)
			}
			generatedServiceNames[service.Name] = struct{}{}
			serviceIDs[service.ID] = struct{}{}
			result.Services = append(result.Services, GeneratedEnvironmentService{
				ComponentID: instance.ID,
				Name:        service.Name,
				Definition:  service,
			})
		}
		if instance.Enabled && len(plan.Services) != len(generatedIDs) {
			return EnvironmentComponentRender{}, errs.New(
				errs.KindInternal,
				"Component planner did not cover every generated Service identity",
			)
		}
		for _, file := range plan.Files {
			if !ValidGeneratedRelativePath(file.Path) {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindValidationFailed,
					"Component generated file path is invalid",
				)
			}
			if _, collision := generatedFilePaths[file.Path]; collision {
				return EnvironmentComponentRender{}, errs.New(
					errs.KindNameConflict,
					"Component plan repeats a generated file path",
				)
			}
			generatedFilePaths[file.Path] = struct{}{}
			result.Files = append(result.Files, GeneratedEnvironmentFile{
				ComponentID: instance.ID,
				Path:        file.Path,
				Content:     append([]byte(nil), file.Content...),
			})
		}
	}
	sort.Slice(result.Services, func(left, right int) bool {
		return result.Services[left].Name < result.Services[right].Name
	})
	sort.Slice(result.Files, func(left, right int) bool { return result.Files[left].Path < result.Files[right].Path })
	return result, nil
}
