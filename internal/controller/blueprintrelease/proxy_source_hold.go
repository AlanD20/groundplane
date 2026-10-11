package blueprintrelease

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProxySourceHold uses the visible Apply's source root. Preparation owns
// existing immutable values only; activation joins the router Task publication.
func (service *Service) PrepareProxySourceHold(ctx context.Context, parent etcd.TaskRecord,
	rollouts []componentplanning.ProxyServiceRolloutReference, preparation componentplanning.ComponentTaskPreparation,
) (componentplanning.ComponentTaskPreparation, error) {
	prepared, err := service.prepareProxySourceHold(ctx, parent, rollouts)
	if err != nil {
		return componentplanning.ComponentTaskPreparation{}, err
	}
	defer prepared.fragment.Clear()
	if prepared.reference == nil {
		return preparation, nil
	}
	result, err := preparation.WithProxySourceHold(*prepared.reference, prepared.fragment)
	if err != nil {
		return componentplanning.ComponentTaskPreparation{}, service.abandonProxySourceHold(
			ctx,
			parent.OperationID,
			prepared.members,
			err,
		)
	}
	return result, nil
}

func (service *Service) PrepareDisabledProxySourceHold(ctx context.Context, parent etcd.TaskRecord,
	rollouts []componentplanning.ProxyServiceRolloutReference, authority componentplanning.FrozenProjectionAuthority,
	input keyvalue.Versioned[componentplanning.ProxyCandidateInputReference],
) (componentplanning.FrozenProjectionAuthority, error) {
	prepared, err := service.prepareProxySourceHold(ctx, parent, rollouts)
	if err != nil {
		return componentplanning.FrozenProjectionAuthority{}, err
	}
	defer prepared.fragment.Clear()
	result, err := authority.WithDisabledProxyRollouts(input, rollouts, prepared.reference, prepared.fragment)
	if err != nil && prepared.reference != nil {
		return componentplanning.FrozenProjectionAuthority{}, service.abandonProxySourceHold(
			ctx,
			parent.OperationID,
			prepared.members,
			err,
		)
	}
	return result, err
}

type proxySourceHoldPreparation struct {
	reference *componentplanning.ProxySourceHold
	fragment  scriptsourcepublication.ScriptSourcePublicationFragment
	members   []scriptsourceevidence.ScriptSourcePreparationMember
}

func (service *Service) prepareProxySourceHold(ctx context.Context, parent etcd.TaskRecord,
	rollouts []componentplanning.ProxyServiceRolloutReference,
) (proxySourceHoldPreparation, error) {
	if service == nil || service.sources == nil || parent.Executor != taskjournal.TaskExecutorBlueprint ||
		parent.Status != taskjournal.TaskStatusRunning || parent.Owner.EnvironmentID == "" {
		return proxySourceHoldPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pending proxy sources require an active Apply",
		)
	}
	var members []scriptsourceevidence.ScriptSourcePreparationMember
	for _, rollout := range rollouts {
		captured, err := service.ledger.BlueprintProxyHookSourceMembers(ctx, parent, rollout)
		if err != nil {
			return proxySourceHoldPreparation{}, err
		}
		members = append(members, captured...)
	}
	if len(members) == 0 {
		return proxySourceHoldPreparation{}, nil
	}
	prepared, err := service.sources.Prepare(ctx, parent.OperationID, members)
	if err != nil {
		return proxySourceHoldPreparation{}, service.abandonProxySourceHold(ctx, parent.OperationID, members, err)
	}
	fragment, err := service.sources.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		return proxySourceHoldPreparation{}, service.abandonProxySourceHold(ctx, parent.OperationID, members, err)
	}
	return proxySourceHoldPreparation{reference: &componentplanning.ProxySourceHold{
		OperationID: parent.OperationID, MembershipCount: prepared.MembershipCount(), MembershipSHA256: prepared.MembershipSHA256(),
	}, fragment: fragment, members: members}, nil
}

func (service *Service) abandonProxySourceHold(ctx context.Context, operationID string,
	members []scriptsourceevidence.ScriptSourcePreparationMember, cause error,
) error {
	if blueprintPublicationOutcomeUnknown(cause) {
		return cause
	}
	if err := service.sources.Abandon(ctx, operationID, members); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}
