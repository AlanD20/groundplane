package componentrender

import (
	"github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func PlanCloudflareTunnel(
	environment core.Environment,
	instance core.Component,
	plan func(string, string, []component.NetworkInput) (component.EnvironmentPlan, error),
) (component.EnvironmentPlan, error) {
	if environment.ID == "" || instance.ID == "" || instance.Owner != core.ComponentOwnerEnvironment ||
		instance.OwnerID != environment.ID || instance.Kind != core.ComponentKindEdgeCloudflare || instance.Validate() != nil {
		return component.EnvironmentPlan{}, errs.New(
			errs.KindValidationFailed,
			"cloudflare tunnel: component ownership or kind is invalid",
		)
	}
	if !instance.Enabled {
		return component.EnvironmentPlan{}, nil
	}
	if instance.Config.CloudflareTunnel == nil ||
		ids.Validate(ids.KindSecret, instance.Config.CloudflareTunnel.SecretID) != nil {
		return component.EnvironmentPlan{}, errs.New(
			errs.KindValidationFailed,
			"cloudflare tunnel: secret_id is invalid",
		)
	}
	secretID := instance.Config.CloudflareTunnel.SecretID
	zones := make([]component.NetworkInput, len(instance.Config.CloudflareTunnel.ZoneIDs))
	for index, zoneID := range instance.Config.CloudflareTunnel.ZoneIDs {
		found := false
		for _, candidate := range environment.Zones {
			if candidate.ID != zoneID {
				continue
			}
			if candidate.OwnerKind != core.ZoneOwnerEnvironment || candidate.OwnerID != environment.ID {
				return component.EnvironmentPlan{}, errs.New(
					errs.KindScopeUnauthorized,
					"cloudflare tunnel: selected Zone is not owned by the Environment",
				)
			}
			zones[index] = component.NetworkInput{ID: candidate.ID, Name: candidate.Name, Internal: candidate.Internal}
			found = true
			break
		}
		if !found {
			return component.EnvironmentPlan{}, errs.New(
				errs.KindValidationFailed,
				"cloudflare tunnel: selected Zone is not in the Environment",
			)
		}
	}
	if len(instance.GeneratedServices) != 1 || ids.Validate(ids.KindService, instance.GeneratedServices[0]) != nil {
		return component.EnvironmentPlan{}, errs.New(
			errs.KindValidationFailed,
			"cloudflare tunnel: one stable generated Service id is required",
		)
	}
	planned, err := plan(instance.GeneratedServices[0], secretID, zones)
	if err != nil {
		return component.EnvironmentPlan{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	return planned, nil
}
