// Package ipam owns Groundplane's pure IPv4 allocation rules.
package ipam

import (
	"encoding/binary"
	"net/netip"
	"sort"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const ipv4Bits = 32

// ParseIPv4Prefix parses an IPv4 CIDR and returns its canonical network prefix.
func ParseIPv4Prefix(value string) (netip.Prefix, error) {
	prefix, err := netip.ParsePrefix(value)
	if err != nil {
		return netip.Prefix{}, errs.Newf(errs.KindValidationFailed, "invalid IPv4 CIDR %q", value)
	}
	if !prefix.Addr().Is4() {
		return netip.Prefix{}, errs.Newf(errs.KindValidationFailed, "CIDR %q is not IPv4", value)
	}
	return prefix.Masked(), nil
}

// ValidateRootPair verifies that the machine allocation roots are valid and disjoint.
func ValidateRootPair(environmentPool, systemPool netip.Prefix) error {
	environmentPool, err := canonicalIPv4(environmentPool)
	if err != nil {
		return err
	}
	systemPool, err = canonicalIPv4(systemPool)
	if err != nil {
		return err
	}
	if environmentPool.Overlaps(systemPool) {
		return errs.Newf(
			errs.KindValidationFailed,
			"environment pool %s overlaps system pool %s",
			environmentPool,
			systemPool,
		)
	}
	return nil
}

// ValidateChild verifies containment and collision rules for one reservation.
func ValidateChild(parent, candidate netip.Prefix, reserved []netip.Prefix) error {
	parent, err := canonicalIPv4(parent)
	if err != nil {
		return err
	}
	candidate, err = canonicalIPv4(candidate)
	if err != nil {
		return err
	}
	if !containsPrefix(parent, candidate) {
		return errs.Newf(errs.KindValidationFailed, "CIDR %s is outside allocation pool %s", candidate, parent)
	}
	for _, value := range reserved {
		value, canonicalErr := canonicalIPv4(value)
		if canonicalErr != nil {
			return canonicalErr
		}
		if candidate.Overlaps(value) {
			return errs.Newf(errs.KindStateConflict, "CIDR %s overlaps reservation %s", candidate, value)
		}
	}
	return nil
}

// FirstAvailableChild returns the lowest free child prefix of the requested size.
func FirstAvailableChild(parent netip.Prefix, bits int, reserved []netip.Prefix) (netip.Prefix, error) {
	parent, err := canonicalIPv4(parent)
	if err != nil {
		return netip.Prefix{}, err
	}
	if bits < parent.Bits() || bits > ipv4Bits {
		return netip.Prefix{}, errs.Newf(
			errs.KindValidationFailed,
			"child prefix length %d is outside %s",
			bits,
			parent,
		)
	}

	intervals := make([]addressInterval, 0, len(reserved))
	for _, value := range reserved {
		value, canonicalErr := canonicalIPv4(value)
		if canonicalErr != nil {
			return netip.Prefix{}, canonicalErr
		}
		if !containsPrefix(parent, value) {
			return netip.Prefix{}, errs.Newf(
				errs.KindValidationFailed,
				"reservation %s is outside allocation pool %s",
				value,
				parent,
			)
		}
		intervals = append(intervals, intervalOf(value))
	}
	sort.Slice(intervals, func(left, right int) bool {
		if intervals[left].start == intervals[right].start {
			return intervals[left].end < intervals[right].end
		}
		return intervals[left].start < intervals[right].start
	})

	parentRange := intervalOf(parent)
	blockSize := uint64(1) << (ipv4Bits - bits)
	cursor := parentRange.start
	for _, interval := range intervals {
		if interval.end < cursor {
			continue
		}
		candidateEnd := cursor + blockSize - 1
		if candidateEnd < interval.start {
			return prefixFrom(cursor, bits), nil
		}
		cursor = alignUp(interval.end+1, parentRange.start, blockSize)
		if cursor > parentRange.end {
			break
		}
	}
	if cursor+blockSize-1 <= parentRange.end {
		return prefixFrom(cursor, bits), nil
	}
	return netip.Prefix{}, errs.Newf(
		errs.KindStateConflict,
		"allocation pool %s has no free /%d reservation",
		parent,
		bits,
	)
}

type addressInterval struct {
	start uint64
	end   uint64
}

func canonicalIPv4(prefix netip.Prefix) (netip.Prefix, error) {
	if !prefix.IsValid() || !prefix.Addr().Is4() {
		return netip.Prefix{}, errs.New(errs.KindValidationFailed, "invalid IPv4 prefix")
	}
	return prefix.Masked(), nil
}

func containsPrefix(parent, child netip.Prefix) bool {
	return child.Bits() >= parent.Bits() && parent.Contains(child.Addr())
}

func intervalOf(prefix netip.Prefix) addressInterval {
	start := uint64(binary.BigEndian.Uint32(prefix.Addr().AsSlice()))
	size := uint64(1) << (ipv4Bits - prefix.Bits())
	return addressInterval{start: start, end: start + size - 1}
}

func alignUp(value, base, size uint64) uint64 {
	offset := value - base
	if remainder := offset % size; remainder != 0 {
		offset += size - remainder
	}
	return base + offset
}

func prefixFrom(start uint64, bits int) netip.Prefix {
	var raw [4]byte
	binary.BigEndian.PutUint32(raw[:], uint32(start))
	return netip.PrefixFrom(netip.AddrFrom4(raw), bits)
}
