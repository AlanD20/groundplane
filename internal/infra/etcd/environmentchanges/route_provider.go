package environmentchanges

import (
	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RouteProviderPin struct {
	ComponentID      string                       `json:"component_id"`
	DefinitionDigest string                       `json:"definition_digest"`
	CatalogDigest    string                       `json:"catalog_digest"`
	InputRevision    int64                        `json:"input_revision"`
	InputGeneration  uint64                       `json:"input_generation"`
	Destination      string                       `json:"destination"`
	ActionID         string                       `json:"action_id"`
	ServiceID        string                       `json:"service_id"`
	Input            componentsdk.HTTPRouterInput `json:"input"`
	Runtime          *RouteProviderRuntime        `json:"runtime,omitempty"`
}

// A direct Route changes files and may replace the router container. Retain the
// exact acknowledged artifact and its storage revision before publishing that
// work. Empty artifact means no acknowledged running predecessor, not a live
// container-absence proof. Component lifecycle pins do not use this field.
type RouteProviderRuntime struct {
	EnvironmentID string `json:"environment_id"`
	Revision      int64  `json:"revision"`
	Artifact      []byte `json:"artifact,omitempty"`
}

func ValidateRouteProviderPin(provider *RouteProviderPin) error {
	if provider == nil {
		return errs.New(errs.KindValidationFailed, "Route provider pin is missing")
	}
	if routerecord.ValidateProviderObservation(&routerecord.ProviderObservation{
		ComponentID: provider.ComponentID, DefinitionDigest: provider.DefinitionDigest,
		CatalogDigest: provider.CatalogDigest, InputRevision: provider.InputRevision,
		InputGeneration: provider.InputGeneration,
	}) != nil || provider.Destination == "" || provider.ActionID == "" ||
		recordcodec.ValidateID(ids.KindService, provider.ServiceID) != nil ||
		provider.Input.ComponentID != provider.ComponentID ||
		provider.Input.GeneratedServiceID != provider.ServiceID ||
		componentsdk.ValidateHTTPRouterInput(provider.Input) != nil {
		return errs.New(errs.KindValidationFailed, "Route provider pin is invalid")
	}
	if provider.Runtime != nil {
		if _, err := DecodeRouteProviderRuntime(provider); err != nil {
			return err
		}
	}
	return nil
}
