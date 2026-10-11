package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) prepareDisabledProxyRollouts(ctx context.Context,
	parent etcd.TaskRecord, snapshot blueprintunits.Snapshot, source authoredRuntimeProjectionSource,
	candidate componentrecord.Record, authority componentplanning.FrozenProjectionAuthority,
) (componentplanning.FrozenProjectionAuthority, error) {
	if len(authority.ProxyReplacementServices()) == 0 {
		return authority, nil
	}
	projection, err := service.compileAuthoredProxyCandidateInput(ctx, parent, source, snapshot,
		authoredPendingProxy{record: candidate})
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	operationID := ids.DeriveAt(ids.KindOperation, parent.CreatedAt, parent.ID, "disabled-proxy/"+candidate.Desired.ID)
	stored, err := service.repository.StageBlueprintDisabledProxyCandidateInput(ctx, parent, operationID,
		candidate, projection, source.desired.Revision)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	preflight, found := authority.ProxyServiceCapture()
	if !found {
		return componentplanning.FrozenProjectionAuthority{}, errs.New(
			errs.KindStateConflict,
			"disabled proxy lost its captured Service membership",
		)
	}
	rollouts, err := service.prepareAuthoredProxyServiceRollouts(ctx, parent,
		authoredProxyRolloutOwner{ID: operationID, CreatedAt: parent.CreatedAt, TimeoutSeconds: parent.TimeoutSeconds},
		projection, stored.Record, preflight)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	return service.blueprintReleases.PrepareDisabledProxySourceHold(ctx, parent, rollouts, authority, stored)
}
