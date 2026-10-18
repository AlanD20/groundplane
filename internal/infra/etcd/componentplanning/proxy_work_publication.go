package componentplanning

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ValidateAgentProxyPublication checks staged dependencies before the same
// transaction publishes Component input and activates its source holds.
func (repository *Planner) ValidateAgentProxyPublication(ctx context.Context, work workqueue.View,
	preflight ProxyServicePreflight, change workqueue.Change,
) error {
	if err := ValidateAgentProxyCapture(work, preflight); err != nil {
		return err
	}
	operationID, err := ProxySourceOperationID(work.Task.Task)
	if err != nil {
		return err
	}
	if preflight.SourceHold != nil && preflight.SourceHold.OperationID != operationID {
		return errs.New(errs.KindStateConflict, "Component proxy source hold belongs to another Apply")
	}
	revision, err := proxyPublicationRevision(preflight, change, work.ReadRevision)
	if err != nil {
		return err
	}
	return validateProxyRolloutPublication(ctx, repository.store, &preflight, change.Mutations, revision)
}

// Staging happens after the desired-state snapshot. Read the newly staged
// immutable records at their own revisions while retaining the original source
// comparisons in publication; never refresh the desired/runtime capture.
func proxyPublicationRevision(preflight ProxyServicePreflight, change workqueue.Change, revision int64) (int64, error) {
	if preflight.CandidateInput == nil || preflight.validate() != nil || revision <= 0 {
		return 0, errs.New(errs.KindStateConflict, "Proxy publication has no complete captured input")
	}
	compared := func(key string, expected int64) (int64, bool) {
		for _, condition := range change.Conditions {
			if condition.Key == key && !condition.Prefix && condition.ModRevision > 0 &&
				(expected == 0 || condition.ModRevision == expected) {
				return condition.ModRevision, true
			}
		}
		return 0, false
	}
	inputRevision, found := compared(ProxyCandidateInputKey(*preflight.CandidateInput), 0)
	if !found {
		return 0, errs.New(errs.KindStateConflict, "Proxy publication must compare its staged input")
	}
	revision = max(revision, inputRevision)
	for _, source := range preflight.Services {
		if !source.ReplacementRequired {
			continue
		}
		if source.Rollout == nil {
			return 0, errs.New(errs.KindStateConflict, "Proxy publication is missing a Service rollout")
		}
		rollout := source.Rollout
		for _, required := range []keyvalue.Condition{
			{Key: ProxyServiceRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, rollout.WorkItemID), ModRevision: rollout.Revision},
			{Key: ProxyServiceNativeRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, rollout.WorkItemID), ModRevision: rollout.NativeRevision},
		} {
			if _, found := compared(required.Key, required.ModRevision); !found {
				return 0, errs.New(errs.KindStateConflict, "Proxy publication must compare each staged Service rollout")
			}
			revision = max(revision, required.ModRevision)
		}
	}
	return revision, nil
}
