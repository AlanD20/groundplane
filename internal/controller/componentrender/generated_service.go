package componentrender

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
	"path"
	"runtime"
	"strings"
)

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
		if !ValidGeneratedRelativePath(mount.Source) || !path.IsAbs(mount.Target) ||
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

func ValidGeneratedRelativePath(value string) bool {
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

func environmentComponentImageReference(image componentsdk.OCIImage) (string, bool) {
	_, reference, selected := image.Select(runtime.GOOS, runtime.GOARCH)
	return reference, selected
}
