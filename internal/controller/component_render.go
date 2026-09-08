package controller

import (
	"crypto/sha256"
	"net/netip"
	"path"
	"sort"
	"strings"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EnvironmentComponentPlanFunc func(
	core.Environment,
	core.Component,
) (componentsdk.EnvironmentPlan, error)

type EnvironmentHTTPRouterProjectionFunc func(
	core.Environment,
	core.Component,
) (componentsdk.HTTPRouterInput, error)

type EnvironmentHTTPRouterPlanFunc func(
	componentsdk.HTTPRouterInput,
	core.Component,
) (componentsdk.EnvironmentPlan, error)

// EnvironmentManagedConfigurationRegistration binds a registered Component's
// generic managed-config action to the immutable file it activates.
type EnvironmentManagedConfigurationRegistration struct {
	SourcePath string
	ActionID   componentsdk.ActionID
}

// EnvironmentComponentRegistration is the composition-root seam between one
// build-time registered implementation and Controller-owned plan validation.
type EnvironmentComponentRegistration struct {
	Kind                 core.ComponentKind
	Definition           componentsdk.Definition
	CatalogDigest        [sha256.Size]byte
	Plan                 EnvironmentComponentPlanFunc
	ProjectHTTPRouter    EnvironmentHTTPRouterProjectionFunc
	PlanHTTPRouter       EnvironmentHTTPRouterPlanFunc
	ManagedConfiguration *EnvironmentManagedConfigurationRegistration
}

type GeneratedEnvironmentService struct {
	ComponentID string
	Name        string
	Definition  componentsdk.ManagedService
}

type GeneratedEnvironmentFile struct {
	ComponentID string
	Path        string
	Content     []byte
}

type EnvironmentComponentRender struct {
	Services []GeneratedEnvironmentService
	Files    []GeneratedEnvironmentFile
}

func ValidateEnvironmentComponentCatalog(catalog []EnvironmentComponentRegistration) error {
	seen := make(map[core.ComponentKind]struct{}, len(catalog))
	for _, registration := range catalog {
		if registration.Kind == "" || registration.Plan == nil ||
			registration.Definition.Validate() != nil ||
			registration.Definition.Implementation() != componentsdk.ImplementationKey(registration.Kind) ||
			zeroComponentDigest(registration.CatalogDigest) {
			return errs.New(errs.KindInternal, "Environment Component registration is invalid")
		}
		if managed := registration.ManagedConfiguration; managed != nil {
			action, found := registration.Definition.FindAction(managed.ActionID)
			if managed.SourcePath == "" || path.IsAbs(managed.SourcePath) ||
				path.Clean(managed.SourcePath) != managed.SourcePath ||
				!found ||
				action.Capability() != componentsdk.CapabilityManagedConfig ||
				action.Operation() != componentsdk.OperationActivate {
				return errs.New(errs.KindInternal, "Environment Component managed configuration is invalid")
			}
		}
		providesRouter := false
		for _, capability := range registration.Definition.Provides() {
			providesRouter = providesRouter || capability == componentsdk.CapabilityHTTPRouter
		}
		if registration.ProjectHTTPRouter != nil != (registration.PlanHTTPRouter != nil) ||
			registration.ProjectHTTPRouter != nil && (!providesRouter || registration.ManagedConfiguration == nil) {
			return errs.New(errs.KindInternal, "HTTP router Component registration is incomplete")
		}
		if _, duplicate := seen[registration.Kind]; duplicate {
			return errs.New(errs.KindInternal, "Environment Component catalog repeats a kind")
		}
		seen[registration.Kind] = struct{}{}
	}
	return nil
}

func BuildPinnedEnvironmentComponentAction(
	catalog []EnvironmentComponentRegistration,
	componentID string,
	definitionDigest [sha256.Size]byte,
	catalogDigest [sha256.Size]byte,
	actionID componentsdk.ActionID,
	artifactID string,
	artifactDigest [sha256.Size]byte,
	generation uint64,
) (*agentpb.ComponentApply, error) {
	if err := ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	for _, registration := range catalog {
		if registration.Definition.Digest() != definitionDigest || registration.CatalogDigest != catalogDigest {
			continue
		}
		return BuildEnvironmentComponentAction(
			catalog, registration.Kind, componentID, actionID, artifactID, artifactDigest, generation,
		)
	}
	return nil, errs.New(errs.KindStateConflict, "pinned Environment Component definition is not registered")
}

func managedConfigurationIdentity(
	registration EnvironmentComponentRegistration,
) (string, componentsdk.ActionID, bool) {
	if registration.ManagedConfiguration == nil {
		return "", "", false
	}
	return registration.ManagedConfiguration.SourcePath, registration.ManagedConfiguration.ActionID, true
}

func BuildEnvironmentComponentAction(
	catalog []EnvironmentComponentRegistration,
	kind core.ComponentKind,
	componentID string,
	actionID componentsdk.ActionID,
	artifactID string,
	artifactDigest [sha256.Size]byte,
	generation uint64,
) (*agentpb.ComponentApply, error) {
	if err := ValidateEnvironmentComponentCatalog(catalog); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindComponent, componentID) != nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil || generation == 0 ||
		zeroComponentDigest(artifactDigest) {
		return nil, errs.New(errs.KindInternal, "Environment Component action identity is invalid")
	}
	for _, registration := range catalog {
		if registration.Kind != kind {
			continue
		}
		action, found := registration.Definition.FindAction(actionID)
		if !found || action.Capability() != componentsdk.CapabilityManagedConfig ||
			action.Operation() != componentsdk.OperationActivate {
			return nil, errs.New(errs.KindInternal, "Environment Component action is not registered")
		}
		definitionDigest := registration.Definition.Digest()
		return &agentpb.ComponentApply{
			ComponentId:      componentID,
			DefinitionDigest: append([]byte(nil), definitionDigest[:]...),
			CatalogDigest:    append([]byte(nil), registration.CatalogDigest[:]...),
			ActionId:         string(actionID), ArtifactId: artifactID,
			ArtifactDigest: append([]byte(nil), artifactDigest[:]...),
			Generation:     generation,
		}, nil
	}
	return nil, errs.New(errs.KindInternal, "Environment Component action kind is not registered")
}

func zeroComponentDigest(value [sha256.Size]byte) bool {
	var combined byte
	for _, part := range value {
		combined |= part
	}
	return combined == 0
}

func CloneEnvironmentComponentCatalog(
	catalog []EnvironmentComponentRegistration,
) []EnvironmentComponentRegistration {
	return append([]EnvironmentComponentRegistration(nil), catalog...)
}

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
			if !validGeneratedRelativePath(file.Path) {
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

func validateGeneratedEnvironmentService(
	environment core.Environment,
	instance core.Component,
	service componentsdk.ManagedService,
) error {
	_, imageSelected := environmentComponentImageReference(service.Image)
	if ids.Validate(ids.KindService, service.ID) != nil || service.Name == "" || !imageSelected ||
		service.Replicas == 0 || !service.NetworkMode.Valid() ||
		service.NetworkMode == componentsdk.ManagedNetworkModeHost {
		return errs.New(errs.KindInternal, "Component planner emitted an invalid Service")
	}
	if service.Healthcheck != nil && service.Healthcheck.Validate() != nil {
		return errs.New(errs.KindValidationFailed, "Component planner emitted an invalid healthcheck")
	}
	networks := make(map[string]struct{}, len(service.Networks))
	gatewayPriorities := 0
	for _, network := range service.Networks {
		zone, exists := environment.Zones[network.Name]
		if !exists || zone.Name != network.Name {
			return errs.New(errs.KindInternal, "Component planner referenced an unknown Zone")
		}
		if _, duplicate := networks[network.Name]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component planner repeated a Zone")
		}
		networks[network.Name] = struct{}{}
		if network.StaticIPv4 != "" {
			address, err := netip.ParseAddr(network.StaticIPv4)
			if err != nil || !address.Is4() || address.String() != network.StaticIPv4 {
				return errs.New(errs.KindValidationFailed, "Component planner emitted an invalid static address")
			}
		}
		if network.GatewayPriority < 0 || network.GatewayPriority > 1 {
			return errs.New(errs.KindValidationFailed, "Component planner emitted an invalid gateway priority")
		}
		if network.GatewayPriority == 1 {
			gatewayPriorities++
		}
	}
	if gatewayPriorities > 1 {
		return errs.New(errs.KindValidationFailed, "Component planner emitted multiple gateway priorities")
	}
	mountTargets := make(map[string]struct{}, len(service.Mounts))
	for _, mount := range service.Mounts {
		if !validGeneratedRelativePath(mount.Source) || !path.IsAbs(mount.Target) ||
			path.Clean(mount.Target) != mount.Target || mount.Target == "/" {
			return errs.New(errs.KindValidationFailed, "Component planner emitted an unsafe mount")
		}
		if _, duplicate := mountTargets[mount.Target]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component planner repeated a mount target")
		}
		mountTargets[mount.Target] = struct{}{}
	}
	secretNames := make(map[string]struct{}, len(service.SecretEnvironment))
	for _, binding := range service.SecretEnvironment {
		if !validGeneratedEnvironmentVariable(binding.Name) ||
			ids.Validate(ids.KindSecret, binding.SecretID) != nil {
			return errs.New(errs.KindValidationFailed, "Component planner emitted an invalid Secret binding")
		}
		if _, duplicate := secretNames[binding.Name]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component planner repeated a Secret binding")
		}
		secretNames[binding.Name] = struct{}{}
	}
	for _, dependency := range service.Dependencies {
		if dependency.ServiceName == "" || dependency.Condition == "" {
			return errs.New(errs.KindValidationFailed, "Component planner emitted an invalid Service dependency")
		}
	}
	if (service.NetworkMode == componentsdk.ManagedNetworkModeZones && len(service.Networks) == 0) ||
		(service.NetworkMode == componentsdk.ManagedNetworkModeDefault && len(service.Networks) != 0) {
		return errs.New(errs.KindValidationFailed, "Environment Component Service network mode is invalid")
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
