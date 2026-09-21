package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplaceEnvironmentPoolIdempotent atomically replaces one Environment pool,
// its global allocation, the mutation epoch, and the protected direct replay
// marker. The Zone registry is compare-only: any concurrent Zone mutation
// invalidates the replacement transaction.
func (repository *HierarchyRepository) ReplaceEnvironmentPoolIdempotent(
	ctx context.Context,
	root netip.Prefix,
	current etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	replacement hierarchyrecord.EnvironmentRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool root must be a canonical IPv4 CIDR",
		)
	}
	if current.Record.ID != replacement.ID ||
		current.Record.ProjectID != replacement.ProjectID ||
		current.Record.Name != replacement.Name ||
		current.Record.VolumeDir != replacement.VolumeDir ||
		current.Record.ProvisioningState != replacement.ProvisioningState ||
		current.Record.CreateTaskID != replacement.CreateTaskID ||
		!current.Record.CreatedAt.Equal(replacement.CreatedAt) ||
		current.Revision <= 0 || current.ReadRevision < current.Revision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool replacement identity is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	fence, err := loadOrdinaryEnvironmentMutationFence(
		ctx,
		repository.store,
		current.Record.ID,
		current.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	fenceConditions := fence.transactionConditions()
	foundCurrentRevision := false
	for _, condition := range fenceConditions {
		if condition.Key == hierarchyrecord.EnvironmentKey(current.Record.ID) {
			foundCurrentRevision = true
			if condition.ModRevision != current.Revision {
				return IdempotencyTransactionResult{}, stateConflict("environment", current.Record.ID)
			}
			break
		}
	}
	if !foundCurrentRevision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Environment mutation fence omitted its Environment authority",
		)
	}

	registryKeys := []string{
		environmentPoolRegistryKey,
		zonePoolRegistryKey(current.Record.ID),
	}
	registries, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: registryKeys, Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if registries == nil || len(registries.Values) != len(registryKeys) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Environment pool fixed-revision evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(registries.Values)
	for index, value := range registries.Values {
		if value != nil && value.Key != registryKeys[index] {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindInternal,
				"Environment pool fixed-revision evidence is corrupt",
			)
		}
	}
	if registries.Values[0] == nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Environment pool reservation registry is missing",
		)
	}
	global, err := recordcodec.Decode[EnvironmentPoolRegistry](
		registries.Values[0].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(global) != nil {
		return IdempotencyTransactionResult{}, corruptEnvironmentPoolRegistry()
	}
	nextGlobal, canonical, err := global.Replace(
		root,
		current.Record.ID,
		current.Record.NetworkPool,
		replacement.NetworkPool,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if canonical != replacement.NetworkPool {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"environment network_pool must be a canonical IPv4 CIDR",
		)
	}

	zones := zonePoolRegistry{Reservations: map[string]string{}}
	zoneRevision := int64(0)
	if registries.Values[1] != nil {
		zones, err = recordcodec.Decode[zonePoolRegistry](
			registries.Values[1].Value,
			"zone_pool_registry",
		)
		if err != nil || validateZonePoolRegistry(zones) != nil {
			return IdempotencyTransactionResult{}, corruptZonePoolRegistry()
		}
		zoneRevision = registries.Values[1].ModRevision
	}
	candidate, err := ipam.ParseIPv4Prefix(canonical)
	if err != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"environment network_pool must be a canonical IPv4 CIDR",
		)
	}
	zonePrefixes, err := zones.prefixes()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	for _, subnet := range zonePrefixes {
		if err := ipam.ValidateChild(candidate, subnet, nil); err != nil {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"environment network_pool must contain every Zone subnet",
			)
		}
	}

	environmentValue, err := hierarchyrecord.EncodeEnvironment(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	globalValue, err := recordcodec.Encode("environment_pool_registry", nextGlobal)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(globalValue)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)

	conditions := append([]etcdstore.Condition(nil), fenceConditions...)
	conditions = append(conditions,
		etcdstore.Condition{Key: environmentPoolRegistryKey, ModRevision: registries.Values[0].ModRevision},
		etcdstore.Condition{Key: zonePoolRegistryKey(current.Record.ID), ModRevision: zoneRevision},
	)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(current.Record.ID), Value: environmentValue},
		{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: globalValue},
		epochMutation,
	}
	fenceCount := len(fenceConditions)
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != fenceCount+2 {
			return errs.New(
				errs.KindInternal,
				"Environment pool compare evidence is incomplete",
			)
		}
		if err := fence.classifyCAS(values[:fenceCount]); err != nil {
			return err
		}
		if values[fenceCount] == nil ||
			values[fenceCount].ModRevision != registries.Values[0].ModRevision {
			return stateConflict("environment pool registry", current.Record.ID)
		}
		if values[fenceCount+1] == nil {
			if zoneRevision != 0 {
				return stateConflict("Zone pool registry", current.Record.ID)
			}
		} else if values[fenceCount+1].ModRevision != zoneRevision {
			return stateConflict("Zone pool registry", current.Record.ID)
		}
		return stateConflict("environment", current.Record.ID)
	}
	plan, err := NewIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
