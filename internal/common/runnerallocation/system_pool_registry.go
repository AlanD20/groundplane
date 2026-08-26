package runnerallocation

import (
	"net/netip"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type SystemPoolRegistry struct {
	RunnerNetworkPool string            `json:"runner_network_pool"`
	Reservations      map[string]string `json:"reservations"`
}

func (registry SystemPoolRegistry) ReserveRunner(
	root netip.Prefix,
	runnerID string,
) (SystemPoolRegistry, netip.Prefix, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	owner := RunnerReservationOwner(runnerID)
	if registry.RunnerNetworkPool != root.String() {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(
			errs.KindStateConflict,
			"runner network pool is not reserved",
		)
	}
	reserved, err := registry.runnerPrefixes(root)
	if err != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, err
	}
	if value, exists := registry.Reservations[owner]; exists {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if parseErr != nil {
			return SystemPoolRegistry{}, netip.Prefix{}, corruptSystemPoolRegistry()
		}
		next := SystemPoolRegistry{
			RunnerNetworkPool: registry.RunnerNetworkPool,
			Reservations:      make(map[string]string, len(registry.Reservations)),
		}
		for key, reservation := range registry.Reservations {
			next.Reservations[key] = reservation
		}
		return next, prefix, nil
	}
	candidate, err := ipam.FirstAvailableChild(root, RunnerSubnetBits, reserved)
	if err != nil {
		return SystemPoolRegistry{}, netip.Prefix{}, errs.New(
			errs.KindResourceInUse,
			"runner network pool is exhausted",
		)
	}
	next := SystemPoolRegistry{
		RunnerNetworkPool: registry.RunnerNetworkPool,
		Reservations:      make(map[string]string, len(registry.Reservations)+1),
	}
	for key, value := range registry.Reservations {
		next.Reservations[key] = value
	}
	next.Reservations[owner] = candidate.String()
	return next, candidate, nil
}

func (registry SystemPoolRegistry) ReleaseRunner(runnerID string, subnet string) (SystemPoolRegistry, error) {
	if err := registry.ValidateReservations(); err != nil {
		return SystemPoolRegistry{}, err
	}
	owner := RunnerReservationOwner(runnerID)
	if registry.Reservations[owner] != subnet {
		return SystemPoolRegistry{}, errs.New(
			errs.KindStateConflict,
			"runner network allocation does not match its owner",
		)
	}
	next := SystemPoolRegistry{
		RunnerNetworkPool: registry.RunnerNetworkPool,
		Reservations:      make(map[string]string, len(registry.Reservations)-1),
	}
	for key, value := range registry.Reservations {
		if key != owner {
			next.Reservations[key] = value
		}
	}
	return next, nil
}

func (registry SystemPoolRegistry) ValidateReservations() error {
	runnerPool, err := ipam.ParseIPv4Prefix(registry.RunnerNetworkPool)
	if err != nil || runnerPool.String() != registry.RunnerNetworkPool || runnerPool.Bits() > RunnerSubnetBits ||
		registry.Reservations == nil {
		return corruptSystemPoolRegistry()
	}
	for owner, value := range registry.Reservations {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if strings.TrimSpace(owner) == "" || parseErr != nil || prefix.String() != value {
			return corruptSystemPoolRegistry()
		}
		if strings.HasPrefix(owner, "runner/") {
			if ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
				prefix.Bits() != RunnerSubnetBits || !runnerPool.Contains(prefix.Addr()) {
				return corruptSystemPoolRegistry()
			}
			continue
		}
		if prefix.Overlaps(runnerPool) {
			return corruptSystemPoolRegistry()
		}
	}
	return nil
}

func (registry SystemPoolRegistry) Validate(root netip.Prefix) error {
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() || registry.ValidateReservations() != nil {
		return corruptSystemPoolRegistry()
	}
	runnerPool, err := ipam.ParseIPv4Prefix(registry.RunnerNetworkPool)
	if err != nil || runnerPool.String() != registry.RunnerNetworkPool || runnerPool.Bits() <= root.Bits() ||
		runnerPool.Bits() > RunnerSubnetBits || !root.Contains(runnerPool.Addr()) {
		return corruptSystemPoolRegistry()
	}
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for owner, value := range registry.Reservations {
		prefix, parseErr := ipam.ParseIPv4Prefix(value)
		if strings.TrimSpace(owner) == "" || parseErr != nil || prefix.String() != value ||
			ipam.ValidateChild(root, prefix, reserved) != nil {
			return corruptSystemPoolRegistry()
		}
		runnerOwner := strings.HasPrefix(owner, "runner/")
		if runnerOwner {
			if ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
				prefix.Bits() != RunnerSubnetBits || !runnerPool.Contains(prefix.Addr()) {
				return corruptSystemPoolRegistry()
			}
		} else if prefix.Overlaps(runnerPool) {
			return corruptSystemPoolRegistry()
		}
		reserved = append(reserved, prefix)
	}
	return nil
}

func (registry SystemPoolRegistry) runnerPrefixes(root netip.Prefix) ([]netip.Prefix, error) {
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return nil, errs.New(errs.KindValidationFailed, "runner pool must be a canonical IPv4 pool")
	}
	reserved := make([]netip.Prefix, 0, len(registry.Reservations))
	for owner, value := range registry.Reservations {
		if !strings.HasPrefix(owner, "runner/") {
			continue
		}
		prefix, err := ipam.ParseIPv4Prefix(value)
		if err != nil || ids.Validate(ids.KindRunner, strings.TrimPrefix(owner, "runner/")) != nil ||
			prefix.String() != value || prefix.Bits() != RunnerSubnetBits ||
			ipam.ValidateChild(root, prefix, reserved) != nil {
			return nil, corruptSystemPoolRegistry()
		}
		reserved = append(reserved, prefix)
	}
	return reserved, nil
}

func RunnerReservationOwner(runnerID string) string { return "runner/" + runnerID }

func corruptSystemPoolRegistry() error {
	return errs.New(errs.KindInternal, "system pool registry is corrupt")
}
