package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/netip"
)

type preparedEnvironmentBlueprintPoolChange struct {
	environment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	registryRevision int64
	environmentValue []byte
	registryValue    []byte
}

func (change preparedEnvironmentBlueprintPoolChange) changed() bool {
	return len(change.environmentValue) != 0
}
func clearPreparedEnvironmentBlueprintPoolChange(change preparedEnvironmentBlueprintPoolChange) {
	clear(change.environmentValue)
	clear(change.registryValue)
}
func (repository *HierarchyRepository) prepareEnvironmentBlueprintPoolChangeAtRevision(
	ctx context.Context,
	root netip.Prefix,
	current etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	desiredNetworkPool string,
	revision int64,
) (preparedEnvironmentBlueprintPoolChange, error) {
	prepared := preparedEnvironmentBlueprintPoolChange{environment: current}
	if desiredNetworkPool == current.Record.NetworkPool {
		return prepared, nil
	}
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool root must be a canonical IPv4 CIDR",
		)
	}
	prepared.environment.Record.NetworkPool = desiredNetworkPool
	if err := hierarchyrecord.ValidateEnvironment(prepared.environment.Record); err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	registries, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentPoolRegistryKey}, Revision: revision,
	})
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if registries == nil || len(registries.Values) != 1 || registries.Values[0] == nil ||
		registries.Values[0].Key != environmentPoolRegistryKey {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindInternal,
			"Environment pool reservation registry is missing",
		)
	}
	defer etcdstore.ClearValues(registries.Values)
	global, err := recordcodec.Decode[EnvironmentPoolRegistry](
		registries.Values[0].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(global) != nil {
		return preparedEnvironmentBlueprintPoolChange{}, corruptEnvironmentPoolRegistry()
	}
	nextGlobal, canonical, err := global.Replace(
		root,
		current.Record.ID,
		current.Record.NetworkPool,
		desiredNetworkPool,
	)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if canonical != desiredNetworkPool {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"x-gp-network-pool must be a canonical IPv4 CIDR",
		)
	}
	prepared.environmentValue, err = hierarchyrecord.EncodeEnvironment(prepared.environment.Record)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryValue, err = recordcodec.Encode("environment_pool_registry", nextGlobal)
	if err != nil {
		clear(prepared.environmentValue)
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryRevision = registries.Values[0].ModRevision
	return prepared, nil
}
