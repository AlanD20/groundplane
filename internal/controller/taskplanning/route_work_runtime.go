package taskplanning

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CapturedRuntime retrieves applied metadata at the pin's fixed snapshot. It
// cannot adopt a newer runtime between rendering and native input publication.
func (capture *RouteProviderCapture) CapturedRuntime(ctx context.Context, pin environmentchanges.RouteProviderPin,
) (keyvalue.Versioned[componentplanning.Runtime], bool, error) {
	if capture == nil || ctx == nil || environmentchanges.ValidateRouteProviderPin(&pin) != nil || pin.Runtime == nil {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false, errs.New(
			errs.KindValidationFailed,
			"Route runtime capture requires its pinned provider",
		)
	}
	runtime, found, err := capture.state.GetRouteProviderRuntimeAtRevision(ctx, pin.ComponentID, pin.InputRevision)
	if err != nil {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false, err
	}
	if runtime.ReadRevision != pin.InputRevision || runtime.Revision != pin.Runtime.Revision ||
		found != (pin.Runtime.Revision != 0) {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false, errs.New(
			errs.KindStateConflict,
			"Route runtime snapshot differs from its captured provider",
		)
	}
	if found && (componentplanning.ValidateRuntime(runtime.Record) != nil ||
		runtime.Record.EnvironmentID != pin.Runtime.EnvironmentID || runtime.Record.Component.Desired.ID != pin.ComponentID ||
		!bytes.Equal(runtime.Record.Artifact, pin.Runtime.Artifact)) {
		return keyvalue.Versioned[componentplanning.Runtime]{}, false, errs.New(
			errs.KindStateConflict,
			"Route runtime metadata differs from its captured artifact",
		)
	}
	return runtime, found, nil
}
