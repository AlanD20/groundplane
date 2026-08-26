// Package releasegroup owns the pure desired-state model for explicit,
// environment-scoped multi-Service release coordination.
package releasegroup

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// OnFailure is the closed group-level failure policy.
type OnFailure string

const (
	OnFailureSwitchBack  OnFailure = "switch_back"
	OnFailureLeaveActive OnFailure = "leave_active"
	MinimumMembers                 = 2
	MaximumMembers                 = 32
)

// Group is one durable Release Group. ServiceIDs is the exact membership set;
// Order is the complete operation order and contains each member exactly once.
type Group struct {
	ID            string
	EnvironmentID string
	Name          string
	ServiceIDs    []string
	Order         []string
	DefaultTag    string
	OnFailure     OnFailure
}

// Input contains stable identity plus operator-authored desired state.
type Input struct {
	ID            string
	EnvironmentID string
	Name          string
	ServiceIDs    []string
	Order         []string
	DefaultTag    string
	OnFailure     OnFailure
}

// Desired contains every mutable field.
type Desired struct {
	Name       string
	ServiceIDs []string
	Order      []string
	DefaultTag string
	OnFailure  OnFailure
}

// New validates and normalizes one Release Group.
func New(input Input) (Group, error) {
	if ids.Validate(ids.KindReleaseGroup, input.ID) != nil {
		return Group{}, validation("release group id is invalid")
	}
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return Group{}, validation("release group environment id is invalid")
	}
	return build(input.ID, input.EnvironmentID, Desired{
		Name: input.Name, ServiceIDs: input.ServiceIDs, Order: input.Order,
		DefaultTag: input.DefaultTag, OnFailure: input.OnFailure,
	})
}

// Replace applies complete desired state without changing stable identity or
// Environment ownership.
func Replace(current Group, desired Desired) (Group, error) {
	if err := Validate(current); err != nil {
		return Group{}, err
	}
	return build(current.ID, current.EnvironmentID, desired)
}

// Validate rejects any value that cannot be stored as a durable Release Group.
func Validate(group Group) error {
	normalized, err := New(Input{
		ID: group.ID, EnvironmentID: group.EnvironmentID, Name: group.Name,
		ServiceIDs: group.ServiceIDs, Order: group.Order, DefaultTag: group.DefaultTag,
		OnFailure: group.OnFailure,
	})
	if err != nil {
		return err
	}
	if !Equal(group, normalized) {
		return validation("release group value is not normalized")
	}
	return nil
}

// Clone returns a value whose membership slices do not alias group.
func Clone(group Group) Group {
	group.ServiceIDs = slices.Clone(group.ServiceIDs)
	group.Order = slices.Clone(group.Order)
	return group
}

// Equal reports equality of the complete durable desired value.
func Equal(left Group, right Group) bool {
	return left.ID == right.ID && left.EnvironmentID == right.EnvironmentID &&
		left.Name == right.Name && left.DefaultTag == right.DefaultTag &&
		left.OnFailure == right.OnFailure && slices.Equal(left.ServiceIDs, right.ServiceIDs) &&
		slices.Equal(left.Order, right.Order)
}

func build(id string, environmentID string, desired Desired) (Group, error) {
	name := desired.Name
	if name == "" || name != strings.TrimSpace(name) || !utf8.ValidString(name) ||
		strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return Group{}, validation("release group name must be canonical valid UTF-8 without surrounding whitespace or control characters")
	}
	if desired.DefaultTag != "" && strings.TrimSpace(desired.DefaultTag) == "" {
		return Group{}, validation("release group default tag cannot be blank")
	}
	if len(desired.ServiceIDs) < MinimumMembers || len(desired.ServiceIDs) > MaximumMembers {
		return Group{}, validation("release group requires between 2 and 32 services")
	}

	members := make(map[string]struct{}, len(desired.ServiceIDs))
	for _, serviceID := range desired.ServiceIDs {
		if ids.Validate(ids.KindService, serviceID) != nil {
			return Group{}, validation("release group member service id is invalid")
		}
		if _, exists := members[serviceID]; exists {
			return Group{}, validation("release group member service ids must be unique")
		}
		members[serviceID] = struct{}{}
	}

	order := desired.Order
	if len(order) == 0 {
		order = desired.ServiceIDs
	}
	if len(order) != len(desired.ServiceIDs) {
		return Group{}, validation("release group order must contain every member exactly once")
	}
	ordered := make(map[string]struct{}, len(order))
	for _, serviceID := range order {
		if _, member := members[serviceID]; !member {
			return Group{}, validation("release group order contains a non-member service")
		}
		if _, duplicate := ordered[serviceID]; duplicate {
			return Group{}, validation("release group order contains a duplicate service")
		}
		ordered[serviceID] = struct{}{}
	}

	policy := desired.OnFailure
	if policy == "" {
		policy = OnFailureSwitchBack
	}
	if policy != OnFailureSwitchBack && policy != OnFailureLeaveActive {
		return Group{}, validation("release group on_failure must be switch_back or leave_active")
	}

	return Group{
		ID: id, EnvironmentID: environmentID, Name: name,
		ServiceIDs: slices.Clone(desired.ServiceIDs), Order: slices.Clone(order),
		DefaultTag: desired.DefaultTag, OnFailure: policy,
	}, nil
}

func validation(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}
