package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Prerequisite waits happen before assignment. Skipping blocked queue entries
// leaves a single-worker Agent available for resolver activation/withdrawal.
func (repository *TaskRepository) prepareQueuedPrerequisites(ctx context.Context,
	current keyvalue.Versioned[TaskRecord], at time.Time,
) (conditions []keyvalue.Condition, ready, advanced bool, err error) {
	networkConditions, networkReady, err := repository.releaseNetworkClaimConditions(
		ctx,
		current.Record,
		current.ReadRevision,
		at,
	)
	if err != nil {
		return nil, false, false, err
	}
	if !networkReady {
		advanced, err := repository.finishPendingReleaseNetwork(ctx, current, at)
		return nil, false, advanced, err
	}
	dnsConditions, dnsReady, err := repository.removalDNSClaimConditions(ctx, current.Record, current.ReadRevision, at)
	if err != nil {
		return nil, false, false, err
	}
	if !dnsReady {
		advanced, err := repository.finishPendingRemovalDNSWithdrawal(ctx, current, at)
		return nil, false, advanced, err
	}
	return append(dnsConditions, networkConditions...), true, false, nil
}
