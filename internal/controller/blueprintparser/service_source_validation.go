package blueprintparser

import (
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/core"
)

var authoredServiceGroundplaneExtensions = map[string]struct{}{
	"x-gp-depends_on": {}, "x-gp-release": {},
}

var authoredNetworkGroundplaneExtensions = map[string]struct{}{
	"x-gp-network": {},
}

var authoredVolumeGroundplaneExtensions = map[string]struct{}{
	"x-gp-slug": {},
}

func validateAuthoredGroundplaneSource(source string, model map[string]any) error {
	if err := validateGroundplaneExtensionNames(model); err != nil {
		return err
	}
	if err := validateGroundplaneExtensionScopes(source, model); err != nil {
		return err
	}
	return validateServiceExtensionSource(source, model)
}

func validateGroundplaneExtensionScopes(source string, model map[string]any) error {
	for name, value := range model {
		if strings.HasPrefix(name, "x-gp-") {
			return sourceExtensionError(source, "Groundplane extension %q is in the wrong scope", name)
		}
		switch name {
		case "services":
			if err := validateScopedDefinitions(source, value, authoredServiceGroundplaneExtensions); err != nil {
				return err
			}
		case "networks":
			if err := validateScopedDefinitions(source, value, authoredNetworkGroundplaneExtensions); err != nil {
				return err
			}
		case "volumes":
			if err := validateScopedDefinitions(source, value, authoredVolumeGroundplaneExtensions); err != nil {
				return err
			}
		default:
			if err := rejectNestedGroundplaneExtensions(value); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateScopedDefinitions(source string, value any, allowed map[string]struct{}) error {
	definitions, ok := stringMap(value)
	if !ok {
		return nil
	}
	for _, definitionValue := range definitions {
		definition, ok := stringMap(definitionValue)
		if !ok {
			continue
		}
		for name, child := range definition {
			if strings.HasPrefix(name, "x-gp-") {
				if _, accepted := allowed[name]; !accepted {
					if name == "x-gp-adapter" {
						return sourceExtensionError(
							source,
							"x-gp-adapter is allowed only on the sole Service of a backing Blueprint",
						)
					}
					return sourceExtensionError(source, "Groundplane extension %q is in the wrong scope", name)
				}
			}
			if err := rejectNestedGroundplaneExtensions(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func rejectNestedGroundplaneExtensions(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for name, child := range typed {
			if strings.HasPrefix(name, "x-gp-") {
				return validationError("blueprint Groundplane extension is in the wrong scope")
			}
			if err := rejectNestedGroundplaneExtensions(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectNestedGroundplaneExtensions(child); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateServiceExtensionSource(source string, model map[string]any) error {
	services, ok := stringMap(model["services"])
	if !ok {
		return nil
	}
	for serviceName, serviceValue := range services {
		service, ok := stringMap(serviceValue)
		if !ok {
			continue
		}
		if raw, exists := service["x-gp-release"]; exists {
			if err := validateServiceReleaseSource(source, raw); err != nil {
				return err
			}
		}
		if raw, exists := service["x-gp-depends_on"]; exists {
			dependencies, err := validateServiceDependenciesSource(source, raw)
			if err != nil {
				return err
			}
			if _, self := dependencies[serviceName]; self {
				return sourceExtensionError(source, "Service %q depends on itself", serviceName)
			}
		}
	}
	return nil
}

func validateServiceReleaseSource(source string, raw any) error {
	release, ok := stringMap(raw)
	if !ok {
		return sourceExtensionError(source, "Service x-gp-release must be a mapping")
	}
	for name, value := range release {
		if value == nil {
			return sourceExtensionError(source, "Service x-gp-release field %q must not be null", name)
		}
		text, ok := value.(string)
		if !ok {
			return sourceExtensionError(source, "Service x-gp-release field %q must be a string", name)
		}
		switch name {
		case "default_strategy":
			probe := core.Service{ID: "source", Name: "source", Image: "source", Strategy: core.Strategy(text)}
			if text == "" || probe.Validate() != nil {
				return sourceExtensionError(source, "Service x-gp-release default_strategy is invalid")
			}
		case "on_failure":
			probe := core.Service{
				ID: "source", Name: "source", Image: "source",
				Strategy: core.StrategyRecreate, OnFailure: core.OnFailure(text),
			}
			if text == "" || probe.Validate() != nil {
				return sourceExtensionError(source, "Service x-gp-release on_failure is invalid")
			}
		default:
			return sourceExtensionError(source, "Service x-gp-release field %q is not authored", name)
		}
	}
	return nil
}

func validateServiceDependenciesSource(
	source string,
	raw any,
) (map[string]any, error) {
	dependencies, ok := stringMap(raw)
	if !ok {
		return nil, sourceExtensionError(source, "Service x-gp-depends_on must be a mapping")
	}
	for name, rawDependency := range dependencies {
		if name == "" {
			return nil, sourceExtensionError(source, "Service dependency target is empty")
		}
		dependency, ok := stringMap(rawDependency)
		if !ok {
			return nil, sourceExtensionError(source, "Service dependency %q must be a mapping", name)
		}
		probe := core.ServiceDependency{Condition: core.ServiceDependencyStarted}
		for field, value := range dependency {
			if value == nil {
				return nil, sourceExtensionError(
					source,
					"Service dependency %q field %q must not be null",
					name,
					field,
				)
			}
			switch field {
			case "condition":
				condition, ok := value.(string)
				if !ok {
					return nil, sourceExtensionError(source, "Service dependency %q condition must be a string", name)
				}
				probe.Condition = core.ServiceDependencyCondition(condition)
			case "phases":
				values, ok := value.([]any)
				if !ok || len(values) == 0 {
					return nil, sourceExtensionError(
						source,
						"Service dependency %q phases must be a non-empty list",
						name,
					)
				}
				probe.Phases = make([]core.ServiceDependencyPhase, len(values))
				for index, phase := range values {
					text, ok := phase.(string)
					if !ok {
						return nil, sourceExtensionError(source, "Service dependency %q phase must be a string", name)
					}
					probe.Phases[index] = core.ServiceDependencyPhase(text)
				}
			default:
				return nil, sourceExtensionError(source, "Service dependency %q field %q is unknown", name, field)
			}
		}
		if err := probe.Validate(); err != nil {
			return nil, sourceExtensionError(source, "Service dependency %q is invalid: %v", name, err)
		}
	}
	return dependencies, nil
}

func validateAuthoredGroundplanePresence(source string, content []byte) error {
	document, err := decodeSingleDocument(content)
	if err != nil {
		return err
	}
	var model map[string]any
	if err := document.Decode(&model); err != nil {
		return sourceExtensionError(source, "cannot be decoded for extension validation")
	}
	services, ok := stringMap(model["services"])
	if !ok {
		return nil
	}
	for _, rawService := range services {
		service, ok := stringMap(rawService)
		if !ok {
			continue
		}
		if raw, exists := service["x-gp-release"]; exists && raw == nil {
			return sourceExtensionError(source, "Service x-gp-release must not be null")
		}
		raw, exists := service["x-gp-depends_on"]
		if !exists {
			continue
		}
		if raw == nil {
			return sourceExtensionError(source, "Service x-gp-depends_on must not be null")
		}
		dependencies, ok := stringMap(raw)
		if !ok {
			continue
		}
		for name, rawDependency := range dependencies {
			if rawDependency == nil {
				return sourceExtensionError(source, "Service dependency %q must not be null", name)
			}
			dependency, ok := stringMap(rawDependency)
			if !ok {
				continue
			}
			if phases, exists := dependency["phases"]; exists && phases == nil {
				return sourceExtensionError(source, "Service dependency %q phases must not be null", name)
			}
		}
	}
	return nil
}

func sourceExtensionError(source string, format string, arguments ...any) error {
	reason := fmt.Sprintf(format, arguments...)
	return validationError(fmt.Sprintf("blueprint Compose source %q: %s", source, reason))
}

func decodeServiceRelease(raw any) (core.ServiceReleaseSpec, error) {
	var release core.ServiceReleaseSpec
	if err := decodeExtension(raw, &release); err != nil || release.DefaultStrategy == "" {
		return core.ServiceReleaseSpec{}, validationError("blueprint Service x-gp-release is invalid")
	}
	probe := core.Service{
		ID: "authored", Name: "authored", Image: "authored",
		Strategy: release.DefaultStrategy, OnFailure: release.OnFailure,
	}
	if err := probe.Validate(); err != nil {
		return core.ServiceReleaseSpec{}, validationError("blueprint Service x-gp-release is invalid")
	}
	release.OnFailure = release.OnFailure.WithDefault()
	return release, nil
}

func decodeServiceDependencies(raw any) (map[string]core.ServiceDependency, error) {
	var dependencies map[string]core.ServiceDependency
	if err := decodeExtension(raw, &dependencies); err != nil || len(dependencies) == 0 {
		return nil, validationError("blueprint Service x-gp-depends_on is invalid")
	}
	for name, dependency := range dependencies {
		if name == "" {
			return nil, validationError("blueprint Service dependency target is invalid")
		}
		if err := dependency.Validate(); err != nil {
			return nil, validationError("blueprint Service x-gp-depends_on is invalid")
		}
	}
	return dependencies, nil
}
