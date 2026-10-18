package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareProxySourceHold retains immutable hook inputs until dependent work
// owns them. Activation joins native Component input publication.
func (service *Service) PrepareProxySourceHold(ctx context.Context, work workqueue.View,
	rollouts []componentplanning.ProxyServiceRolloutReference, preflight componentplanning.ProxyServicePreflight,
) (*componentplanning.ProxyServicePreflight, workqueue.Change, error) {
	if work.Item.Item.Validate() != nil || work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.ReadRevision <= 0 ||
		work.Item.Item.TaskID != work.Task.Task.ID || work.Item.Item.Operation != workexecution.Component ||
		work.Item.Item.AttemptNumber != 0 || work.Item.Item.ActiveAttemptID != "" || work.Item.Item.ForwardRevoked ||
		(work.Item.Item.Phase != workexecution.Waiting && work.Item.Item.Phase != workexecution.Ready) ||
		preflight.ParentTaskID != work.Task.Task.ID || preflight.EnvironmentID != work.Task.Task.Scope.EnvironmentID {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Proxy source preparation does not match unstarted Component work")
	}
	prepared, err := service.prepareProxySourceHold(ctx, work, rollouts)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	defer prepared.fragment.Clear()
	if prepared.reference == nil {
		if preflight.SourceHold != nil {
			return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Proxy work retains a source hold absent from its captured rollouts")
		}
		return &preflight, workqueue.Change{}, nil
	}
	result, change, err := componentplanning.BindWorkProxySourceHold(work, preflight, *prepared.reference, prepared.fragment)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	return result, change, nil
}

type proxySourceHoldPreparation struct {
	reference *componentplanning.ProxySourceHold
	fragment  scriptsourcepublication.ScriptSourcePublicationFragment
}

func (service *Service) prepareProxySourceHold(ctx context.Context, work workqueue.View,
	rollouts []componentplanning.ProxyServiceRolloutReference,
) (proxySourceHoldPreparation, error) {
	parent := work.Task.Task
	if service == nil || service.sources == nil || ctx == nil || parent.Validate() != nil ||
		parent.Action != workexecution.ActionApply || parent.Stop != nil || parent.FinishedAt != nil || parent.CollectionStartedAt != nil || parent.Scope.EnvironmentID == "" {
		return proxySourceHoldPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pending proxy sources require an active Apply",
		)
	}
	operationID, err := componentplanning.ProxySourceOperationID(parent)
	if err != nil {
		return proxySourceHoldPreparation{}, err
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
	prepared, err := service.sources.PrepareOwned(ctx, operationID, members, []keyvalue.Condition{
		{Key: workqueue.TaskKey(parent.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision},
	})
	if err != nil {
		return proxySourceHoldPreparation{}, err
	}
	fragment, err := service.sources.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		return proxySourceHoldPreparation{}, err
	}
	return proxySourceHoldPreparation{reference: &componentplanning.ProxySourceHold{
		OperationID: operationID, MembershipCount: prepared.MembershipCount(), MembershipSHA256: prepared.MembershipSHA256(),
	}, fragment: fragment}, nil
}
