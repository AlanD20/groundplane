package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentPoolRegistryKey = "/v1/indexes/environments/by-network-pool/global/-"

// EnvironmentPoolRegistry is the CAS-protected uniqueness index for durable
// Environment pool reservations. Environment records remain the public source
// of truth; this single record makes overlap checks atomic.
type EnvironmentPoolRegistry struct {
	Reservations map[string]string `json:"reservations"`
}

func (registry EnvironmentPoolRegistry) Reserve(
	root netip.Prefix,
	environmentID string,
	value string,
) (EnvironmentPoolRegistry, string, error) {
	if err := ids.Validate(ids.KindEnvironment, environmentID); err != nil {
		return EnvironmentPoolRegistry{}, "", errs.New(errs.KindValidationFailed, "environment pool owner is invalid")
	}
	candidate, err := ipam.ParseIPv4Prefix(value)
	if err != nil || candidate.String() != value {
		return EnvironmentPoolRegistry{}, "", errs.New(
			errs.KindValidationFailed,
			"environment network_pool must be a canonical IPv4 CIDR",
		)
	}
	reserved, err := registry.prefixes("")
	if err != nil {
		return EnvironmentPoolRegistry{}, "", err
	}
	if err := ipam.ValidateChild(root, candidate, reserved); err != nil {
		return EnvironmentPoolRegistry{}, "", err
	}
	next := EnvironmentPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)+1)}
	for id, pool := range registry.Reservations {
		next.Reservations[id] = pool
	}
	next.Reservations[environmentID] = candidate.String()
	return next, candidate.String(), nil
}

// Replace validates and replaces one existing Environment allocation without
// considering that Environment's current allocation as an overlap.
func (registry EnvironmentPoolRegistry) Replace(
	root netip.Prefix,
	environmentID string,
	current string,
	value string,
) (EnvironmentPoolRegistry, string, error) {
	if err := ids.Validate(ids.KindEnvironment, environmentID); err != nil {
		return EnvironmentPoolRegistry{}, "", errs.New(errs.KindValidationFailed, "environment pool owner is invalid")
	}
	if registry.Reservations[environmentID] != current {
		return EnvironmentPoolRegistry{}, "", errs.New(
			errs.KindStateConflict,
			"Environment pool reservation does not match its owner",
		)
	}
	candidate, err := ipam.ParseIPv4Prefix(value)
	if err != nil || candidate.String() != value {
		return EnvironmentPoolRegistry{}, "", errs.New(
			errs.KindValidationFailed,
			"environment network_pool must be a canonical IPv4 CIDR",
		)
	}
	reserved, err := registry.prefixes(environmentID)
	if err != nil {
		return EnvironmentPoolRegistry{}, "", err
	}
	if err := ipam.ValidateChild(root, candidate, reserved); err != nil {
		return EnvironmentPoolRegistry{}, "", err
	}
	next := EnvironmentPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations))}
	for id, pool := range registry.Reservations {
		next.Reservations[id] = pool
	}
	next.Reservations[environmentID] = candidate.String()
	return next, candidate.String(), nil
}

func (registry EnvironmentPoolRegistry) Release(
	environmentID string,
	value string,
) (EnvironmentPoolRegistry, error) {
	if registry.Reservations[environmentID] != value {
		return EnvironmentPoolRegistry{}, errs.New(
			errs.KindStateConflict,
			"Environment pool reservation does not match its owner",
		)
	}
	next := EnvironmentPoolRegistry{Reservations: make(map[string]string, len(registry.Reservations)-1)}
	for id, pool := range registry.Reservations {
		if id != environmentID {
			next.Reservations[id] = pool
		}
	}
	return next, nil
}

func (registry EnvironmentPoolRegistry) prefixes(excludeEnvironmentID string) ([]netip.Prefix, error) {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for environmentID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindEnvironment, environmentID); err != nil {
			return nil, corruptEnvironmentPoolRegistry()
		}
		pool, err := ipam.ParseIPv4Prefix(value)
		if err != nil || pool.String() != value {
			return nil, corruptEnvironmentPoolRegistry()
		}
		if environmentID != excludeEnvironmentID {
			reserved = append(reserved, pool)
		}
	}
	return reserved, nil
}

func validateEnvironmentPoolRegistry(registry EnvironmentPoolRegistry) error {
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	root := netip.MustParsePrefix("0.0.0.0/0")
	for environmentID, value := range registry.Reservations {
		if err := ids.Validate(ids.KindEnvironment, environmentID); err != nil {
			return corruptEnvironmentPoolRegistry()
		}
		pool, err := ipam.ParseIPv4Prefix(value)
		if err != nil || pool.String() != value || ipam.ValidateChild(root, pool, reserved) != nil {
			return corruptEnvironmentPoolRegistry()
		}
		reserved = append(reserved, pool)
	}
	return nil
}

func (repository *HierarchyRepository) GetEnvironmentPoolRegistry(
	ctx context.Context,
) (Versioned[EnvironmentPoolRegistry], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentPoolRegistry]{}, err
	}
	result, err := repository.store.Get(ctx, environmentPoolRegistryKey)
	if err != nil {
		return Versioned[EnvironmentPoolRegistry]{}, err
	}
	if result.Entry == nil {
		return Versioned[EnvironmentPoolRegistry]{
			Record:       EnvironmentPoolRegistry{Reservations: map[string]string{}},
			ReadRevision: result.ReadRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[EnvironmentPoolRegistry](result.Entry.Value, "environment_pool_registry")
	if err != nil || validateEnvironmentPoolRegistry(registry) != nil {
		return Versioned[EnvironmentPoolRegistry]{}, corruptEnvironmentPoolRegistry()
	}
	return Versioned[EnvironmentPoolRegistry]{
		Record: registry, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func corruptEnvironmentPoolRegistry() error {
	return errs.New(errs.KindInternal, "Environment pool registry is corrupt")
}
