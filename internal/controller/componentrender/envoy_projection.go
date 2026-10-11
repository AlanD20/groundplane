package componentrender

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ProjectEnvoyInput(
	environment core.Environment,
	instance core.Component,
	origin componentsdk.HTTPRouterOrigin,
) (componentsdk.HTTPRouterInput, core.EnvoyComponentConfig, error) {
	if instance.Kind != core.ComponentKindIngressEnvoy {
		return componentsdk.HTTPRouterInput{}, core.EnvoyComponentConfig{}, errs.New(
			errs.KindValidationFailed, "envoy: Component kind is invalid",
		)
	}
	input, err := ProjectHTTPRouterInput(environment, instance, instance.HTTPRouterZoneIDs(), origin)
	if err != nil {
		return componentsdk.HTTPRouterInput{}, core.EnvoyComponentConfig{}, err
	}
	config := core.EnvoyComponentConfig{}
	if instance.Config.Envoy != nil {
		config.Alias = instance.Config.Envoy.Alias
	}
	return input, config, nil
}
