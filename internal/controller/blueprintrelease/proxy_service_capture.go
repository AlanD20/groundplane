package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CaptureProxyServiceRollout freezes native work before the router can execute.
// It uses the ordinary preparation owner and publishes neither a Task nor a
// Release. File recovery is deliberately left for the actual child publication.
func (service *Service) CaptureProxyServiceRollout(ctx context.Context,
	input PrepareServiceUnitInput, candidate componentplanning.ProxyCandidateInputReference,
	preflight componentplanning.ProxyServicePreflight,
) (componentplanning.ProxyServiceRolloutReference, error) {
	if err := validateServiceUnitInput(ctx, service, input); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	if candidate.Validate() != nil || candidate.EnvironmentID != input.Projection.EnvironmentID ||
		candidate.ParentTaskID != input.Projection.RevisionID || preflight.EnvironmentID != candidate.EnvironmentID ||
		preflight.ParentTaskID != candidate.ParentTaskID || preflight.ReadRevision <= 0 {
		return componentplanning.ProxyServiceRolloutReference{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service inputs belong to another Apply",
		)
	}
	stored, err := service.ledger.ReadBlueprintProxyCandidateInput(ctx, candidate, 0)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	if err := exactServiceUnitProjection(stored.Record, input.Projection); err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	var replacement *componentplanning.ProxyReplacementImage
	for _, source := range preflight.Services {
		if source.ServiceID == input.ServiceID && source.ReplacementRequired {
			replacement = source.Replacement
		}
	}
	if replacement == nil {
		return componentplanning.ProxyServiceRolloutReference{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service has no frozen replacement images",
		)
	}
	scope, err := service.ledger.LoadPlanningScopeForCapturedInput(ctx, stored.Record, preflight.ReadRevision)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	// This is a rendering input only. The actual predecessor owner continues
	// reading acknowledged runtime at scope.ReadRevision, never this override.
	prepared, err := service.prepareServiceUnitInput(ctx, input, scope, replacement)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	proxies := make([]core.Service, len(prepared.ServiceChanges))
	for index, change := range prepared.ServiceChanges {
		proxies[index] = change.Record.Desired
		proxies[index].Expose = proxies[index].ProxyExposures()
	}
	addresses, err := taskplanning.PrepareServiceProxyAddresses(
		ctx,
		service.ledger.Planner,
		prepared.Projection,
		proxies,
	)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	defer addresses.Clear()
	native, err := service.stageSelectedRuntime(ctx, prepared, prepared.ServiceChanges, prepared.Task, addresses)
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	capture, err := native.runtime.Capture()
	if err != nil {
		return componentplanning.ProxyServiceRolloutReference{}, err
	}
	defer clear(capture.NativePlan)
	members := append([]releaserender.ReleaseTaskRenderMember(nil), native.members...)
	for index := range members {
		members[index].Render.Projection = projectionrecord.EnvironmentComposeProjection{}
	}
	hooks := make(map[string]scriptsourcequeries.ScriptExecutionSources, len(native.hooks.sources))
	for executionID, sources := range native.hooks.sources {
		sources.DesiredProjection.Record = projectionrecord.EnvironmentComposeProjection{}
		sources.RenderInput.Record.Projection = projectionrecord.EnvironmentComposeProjection{}
		hooks[executionID] = sources
	}
	return service.ledger.StageBlueprintProxyServiceRollout(ctx, etcd.BlueprintProxyServiceRollout{
		Input: candidate, Task: native.task, Manifest: native.manifest,
		NativePredecessors: nativePredecessorCaptures(prepared, native.members), Members: members, Hooks: hooks,
	}, capture)
}
