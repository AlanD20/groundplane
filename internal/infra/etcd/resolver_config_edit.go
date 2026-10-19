package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SaveDisabledResolverConfig changes desired input only. Saving a disabled
// resolver must not manufacture a host execution or start a container.
func (repository *TaskRepository) SaveDisabledResolverConfig(
	ctx context.Context,
	current keyvalue.Versioned[componentrecord.Record],
	desired core.Component,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if current.Revision <= 0 || current.Record.Desired.Enabled || desired.Enabled ||
		current.Record.Desired.Kind != core.ComponentKindCoreDNS || current.Record.Desired.Owner != core.ComponentOwnerPlatform ||
		marker.Kind != idempotency.IdempotencyMarkerDirect || marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform ||
		marker.Locator.ScopeID != "-" || marker.Locator.Method != "PUT" || marker.Locator.Route != "/components/{id}/config" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"disabled CoreDNS edit has invalid resource or replay ownership",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	value, err := componentrecord.EncodeRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	intentKey := resolverwork.IntentKey(host.Binding.HostID, desired.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{intentKey}, Revision: host.ReadRevision},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if read == nil || read.ReadRevision != host.ReadRevision || len(read.Values) != 1 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "CoreDNS current work read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := append(
		host.Conditions,
		keyvalue.Condition{Key: componentrecord.RecordKey(desired.ID), ModRevision: current.Revision},
		keyvalue.Condition{Key: intentKey, ModRevision: keyvalue.RevisionOf(read.Values[0])},
	)
	if intent := read.Values[0]; intent != nil {
		workID := string(intent.Value)
		if ids.Validate(ids.KindWorkItem, workID) != nil {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindInternal,
				"CoreDNS current work identity is corrupt",
			)
		}
		key := workqueue.ItemKey(workID)
		items, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{Keys: []string{key}, Revision: host.ReadRevision},
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		if items == nil || items.ReadRevision != host.ReadRevision || len(items.Values) != 1 {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindInternal,
				"CoreDNS current work snapshot is incomplete",
			)
		}
		defer keyvalue.ClearValues(items.Values)
		if item := items.Values[0]; item != nil {
			work, err := workqueue.DecodeItem(item.Value)
			if err != nil {
				return IdempotencyTransactionResult{}, err
			}
			if work.Operation != workexecution.Resolver || work.ID != workID || work.Phase != workexecution.Settled {
				return IdempotencyTransactionResult{}, errs.New(
					errs.KindStateConflict,
					"CoreDNS still has unfinished work; finish it before editing its disabled configuration",
				)
			}
		}
		conditions = append(conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(items.Values[0])})
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerDirect,
		conditions,
		[]keyvalue.Mutation{
			{Type: keyvalue.MutationPut, Key: componentrecord.RecordKey(desired.ID), Value: value},
			componentrecord.WriteFenceMutation(desired.ID),
		},
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(errs.KindStateConflict, "CoreDNS changed while saving; refresh and try again")
		},
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
