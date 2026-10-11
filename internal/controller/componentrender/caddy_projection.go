package componentrender

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ProjectCaddyInput(
	environment core.Environment,
	instance core.Component,
	origin componentsdk.HTTPRouterOrigin,
) (componentsdk.HTTPRouterInput, core.CaddyComponentConfig, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindIngressCaddy || instance.Validate() != nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: component ownership or kind is invalid",
		)
	}
	if !instance.Enabled {
		return componentsdk.HTTPRouterInput{ComponentID: instance.ID}, core.CaddyComponentConfig{}, nil
	}
	if instance.Config.Caddy == nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, errs.New(
			errs.KindValidationFailed,
			"caddy: typed config is required",
		)
	}
	input, err := ProjectHTTPRouterInput(environment, instance, instance.Config.Caddy.ZoneIDs, origin)
	if err != nil {
		return componentsdk.HTTPRouterInput{}, core.CaddyComponentConfig{}, err
	}
	return input, core.CaddyComponentConfig{
		CaddyfileTemplate: instance.Config.Caddy.CaddyfileTemplate, Alias: instance.Config.Caddy.Alias,
	}, nil
}
