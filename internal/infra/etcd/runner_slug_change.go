package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func (repository *RunnerRepository) ReplaceRunnerSlugIdempotent(
	ctx context.Context,
	runnerID string,
	slug string,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if ids.Validate(ids.KindRunner, runnerID) != nil || runnerrecord.ValidateRunnerSlug(slug) != nil ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.Method != http.MethodPatch || marker.Locator.Route != "/runners/{id}" ||
		marker.ReplayTarget == nil || marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetRunner ||
		marker.ReplayTarget.ID != runnerID || idempotencyrecord.ValidateIdempotencyMarker(marker) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "runner slug mutation is invalid")
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	existing, err := idempotency.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing != nil {
		return IdempotencyTransactionResult{
			kind: idempotencyTransactionExisting, revision: existing.modRevision,
			marker: idempotencyrecord.CloneIdempotencyMarker(existing.marker),
		}, nil
	}
	current, err := repository.GetRunner(ctx, runnerID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateRunnerMarkerScope(current.Record.Desired, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secondaryKeys := []string{
		runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
		deletionTombstoneKey(string(DeletionTargetRunner), runnerID),
	}
	renaming := current.Record.Desired.Slug != slug
	if renaming {
		secondaryKeys = append(secondaryKeys, runnerTenantSlugKey(current.Record.Desired.TenantID, slug))
	}
	secondary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: secondaryKeys, Revision: current.ReadRevision})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secondary == nil || len(secondary.Values) != len(secondaryKeys) || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != runnerID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "runner slug index is missing or mismatched")
	}
	if secondary.Values[1] != nil || current.Record.ProvisioningState == runnerrecord.RunnerProvisioningProvisioning {
		return IdempotencyTransactionResult{}, errs.New(errs.KindResourceInUse, "runner mutation is in progress")
	}
	if renaming && secondary.Values[2] != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindRunnerSlugConflict, "runner slug is already in use")
	}
	replacement := runnerrecord.CloneRunnerRecord(current.Record)
	replacement.Desired.Slug = slug
	value, err := runnerrecord.EncodeRunnerDesiredRecord(replacement.Desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	conditions := []etcdstore.Condition{
		{Key: runnerKey(runnerID), ModRevision: current.Revision},
		{Key: runnerLifecycleKey(runnerID), ModRevision: current.Record.LifecycleRevision},
		{
			Key:         runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{Key: deletionTombstoneKey(string(DeletionTargetRunner), runnerID)},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: runnerKey(runnerID), Value: value}}
	if renaming {
		conditions = append(conditions, etcdstore.Condition{Key: runnerTenantSlugKey(current.Record.Desired.TenantID, slug)})
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key:  runnerTenantSlugKey(current.Record.Desired.TenantID, current.Record.Desired.Slug),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   runnerTenantSlugKey(current.Record.Desired.TenantID, slug),
				Value: []byte(runnerID),
			},
		)
	}
	plan, err := newIdempotencyMutationPlan(conditions, mutations, func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "runner slug mutation compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindRunnerNotFound, "Runner was not found")
		}
		if values[3] != nil {
			return errs.New(errs.KindResourceInUse, "runner mutation is in progress")
		}
		if renaming && values[4] != nil {
			return errs.New(errs.KindRunnerSlugConflict, "runner slug is already in use")
		}
		return stateConflict("runner", runnerID)
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
