package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishResolverWork atomically publishes desired DNS configuration, its exact
// immutable host input, the native action and request replay. No legacy Task
// record, queue, event history or execution plan is written.
func (repository *TaskRepository) PublishResolverWork(ctx context.Context, input resolverexecution.Input,
	capture resolverwork.Capture, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := capture.Validate(input); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Locator.ScopeKind != idempotency.IdempotencyScopePlatform || marker.Locator.ScopeID != "-" {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"CoreDNS work requires platform request idempotency",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	task, item, err := resolverwork.NewWork(input, capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	native, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(native.Mutations)
	publication, err := resolverwork.PreparePublication(input, capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	host, err := hostbindings.ReadLocal(ctx, repository.store, capture.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if host.Binding.HostID != input.HostID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS selected host changed before publication",
		)
	}
	keys := []string{
		componentrecord.RecordKey(input.ComponentID),
		platformcomponents.PlatformComponentOwnerKey(input.ComponentID),
		platformcomponents.PlatformComponentKindKey(core.ComponentKindCoreDNS),
	}
	state, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: capture.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if state == nil || state.ReadRevision != capture.ReadRevision || len(state.Values) != len(keys) ||
		state.Values[0] == nil || state.Values[1] == nil || state.Values[2] == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS Component or singleton ownership is unavailable",
		)
	}
	defer keyvalue.ClearValues(state.Values)
	for index, value := range state.Values {
		if value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > capture.ReadRevision ||
			index > 0 && string(value.Value) != input.ComponentID {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindStateConflict,
				"CoreDNS Component ownership changed before publication",
			)
		}
	}
	if state.Values[0].ModRevision != capture.ComponentRevision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"CoreDNS desired configuration changed during preparation; submit a new action",
		)
	}
	current, err := componentrecord.DecodeRecord(state.Values[0].Value)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := componentrecord.ProjectRecord(current)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired.Enabled, desired.Config = capture.Desired.Enabled, core.CloneComponentConfig(capture.Desired.Config)
	replacement, err := componentrecord.ReplaceDesired(current, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	expected, err := resolverwork.DesiredDigest(capture.Desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	actual, err := resolverwork.DesiredDigest(replacement.Desired)
	if err != nil || expected != actual {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"CoreDNS desired capture changed its stable resource identity",
		)
	}
	value, err := componentrecord.EncodeRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := append(append(publication.Conditions, native.Conditions...), host.Conditions...)
	conditions = append(conditions, keyvalue.Condition{Key: keys[1], ModRevision: state.Values[1].ModRevision},
		keyvalue.Condition{Key: keys[2], ModRevision: state.Values[2].ModRevision})
	mutations := append(publication.Mutations, native.Mutations...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: keys[0], Value: value},
		componentrecord.WriteFenceMutation(input.ComponentID))
	if capture.ProjectionRevision == 0 {
		if len(capture.Projection.Routes) != 0 {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"initial CoreDNS projection cannot invent existing Routes",
			)
		}
		projection, err := hostresolution.EncodeHostResolutionProjectionRecord(capture.Projection)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(projection)
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: hostresolution.StorageKey, Value: projection},
		)
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations,
		func(_ int64, _ []*keyvalue.KeyValue) error {
			return errs.New(
				errs.KindStateConflict,
				"CoreDNS, its host or referenced DNS resources changed before acceptance; refresh and submit a new action",
			)
		})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.Apply(ctx, marker, plan)
}
