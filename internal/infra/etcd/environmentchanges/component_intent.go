package environmentchanges

import (
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/netip"
	"reflect"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const componentTaskIntentPrefix = "/v1/records/component-task-intents/"

// ComponentTaskCandidate pins the exact active Component revision and the
// replacement that one Agent Task is allowed to promote.
type ComponentTaskCandidate struct {
	CurrentRevision int64                  `json:"current_revision"`
	Current         componentrecord.Record `json:"current"`
	Candidate       componentrecord.Record `json:"candidate"`
}

// ComponentTaskIntent is the private, task-owned staging record for one
// Environment Component reconciliation. Components remain publicly active at
// Current until the owning Agent Task completes successfully.
type ComponentTaskIntent struct {
	TaskID          string                        `json:"task_id"`
	EnvironmentID   string                        `json:"environment_id"`
	Status          taskjournal.TaskStatus        `json:"status"`
	Candidates      []ComponentTaskCandidate      `json:"candidates"`
	RouteProjection *ComponentTaskRouteProjection `json:"route_projection,omitempty"`
	CreatedAt       time.Time                     `json:"created_at"`
	TerminalAt      *time.Time                    `json:"terminal_at,omitempty"`
}

func NewComponentTaskIntent(
	taskID string,
	environmentID string,
	candidates []ComponentTaskCandidate,
	createdAt time.Time,
) (ComponentTaskIntent, error) {
	intent := ComponentTaskIntent{
		TaskID:        taskID,
		EnvironmentID: environmentID,
		Status:        taskjournal.TaskStatusPending,
		Candidates:    cloneComponentTaskCandidates(candidates),
		CreatedAt:     createdAt,
	}
	sort.Slice(intent.Candidates, func(left int, right int) bool {
		return intent.Candidates[left].Current.Desired.ID < intent.Candidates[right].Current.Desired.ID
	})
	if err := ValidateComponentTaskIntent(intent); err != nil {
		return ComponentTaskIntent{}, err
	}
	return intent, nil
}

func ComponentTaskIntentKey(taskID string) string {
	return componentTaskIntentPrefix + taskID
}

func ComponentTaskActiveEnvironmentKey(environmentID string) string {
	return "/v1/indexes/component-task-intents/by-environment/" + environmentID
}

func TerminalComponentTaskIntent(
	intent ComponentTaskIntent,
	status taskjournal.TaskStatus,
	terminalAt time.Time,
) (ComponentTaskIntent, error) {
	if intent.Status != taskjournal.TaskStatusPending || !taskjournal.IsTerminalTaskStatus(status) {
		return ComponentTaskIntent{}, errs.New(errs.KindStateConflict, "Component candidate is not pending")
	}
	terminal := CloneComponentTaskIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(terminalAt)
	if err := ValidateComponentTaskIntent(terminal); err != nil {
		return ComponentTaskIntent{}, err
	}
	return terminal, nil
}

func EncodeComponentTaskIntent(intent ComponentTaskIntent) ([]byte, error) {
	if err := ValidateComponentTaskIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("component_task_intent", intent)
}

func DecodeComponentTaskIntent(value []byte) (ComponentTaskIntent, error) {
	intent, err := recordcodec.Decode[ComponentTaskIntent](value, "component_task_intent")
	if err != nil {
		return ComponentTaskIntent{}, err
	}
	if err := ValidateComponentTaskIntent(intent); err != nil {
		return ComponentTaskIntent{}, corruptComponentTaskIntent()
	}
	return intent, nil
}

func ValidateComponentTaskIntent(intent ComponentTaskIntent) error {
	if ids.Validate(ids.KindTask, intent.TaskID) != nil ||
		ids.Validate(ids.KindEnvironment, intent.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Component candidate identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("component candidate created_at", intent.CreatedAt); err != nil {
		return err
	}
	if intent.Status == taskjournal.TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Component candidate has a terminal timestamp")
		}
	} else {
		if !taskjournal.IsTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil {
			return errs.New(errs.KindValidationFailed, "Component candidate status is invalid")
		}
		if err := recordcodec.ValidateTimestamp("component candidate terminal_at", *intent.TerminalAt); err != nil {
			return err
		}
		if intent.TerminalAt.Before(intent.CreatedAt) {
			return errs.New(errs.KindValidationFailed, "Component candidate terminal timestamp is invalid")
		}
	}
	if len(intent.Candidates) == 0 || len(intent.Candidates) > 2 {
		return errs.New(errs.KindValidationFailed, "Component candidate count is invalid")
	}
	seenKinds := make(map[core.ComponentKind]struct{}, len(intent.Candidates))
	previousID := ""
	for _, candidate := range intent.Candidates {
		if candidate.CurrentRevision <= 0 || componentrecord.ValidateRecord(candidate.Current) != nil ||
			componentrecord.ValidateRecord(candidate.Candidate) != nil {
			return errs.New(errs.KindValidationFailed, "Component candidate record is invalid")
		}
		current := candidate.Current.Desired
		next := candidate.Candidate.Desired
		if current.ID != next.ID || current.Owner != next.Owner || current.OwnerID != next.OwnerID ||
			current.Kind != next.Kind || current.Owner != core.ComponentOwnerEnvironment ||
			current.OwnerID != intent.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "Component candidate changed stable ownership")
		}
		if current.Kind != core.ComponentKindIngressCaddy && current.Kind != core.ComponentKindEdgeCloudflare {
			return errs.New(errs.KindValidationFailed, "Component candidate kind is not Environment-owned")
		}
		if previousID != "" && current.ID <= previousID {
			return errs.New(errs.KindValidationFailed, "Component candidates are not uniquely sorted")
		}
		previousID = current.ID
		if _, duplicate := seenKinds[current.Kind]; duplicate {
			return errs.New(errs.KindValidationFailed, "Component candidate kind is duplicated")
		}
		seenKinds[current.Kind] = struct{}{}
		if reflect.DeepEqual(candidate.Current, candidate.Candidate) &&
			(!candidate.Current.Desired.Enabled || candidate.Current.Runtime.Healthy) {
			return errs.New(errs.KindValidationFailed, "Component candidate does not change active state")
		}
		if _, _, err := ComponentTaskAddress(candidate.Current); err != nil {
			return err
		}
		if _, _, err := ComponentTaskAddress(candidate.Candidate); err != nil {
			return err
		}
	}
	if err := validateComponentTaskRouteProjection(intent); err != nil {
		return err
	}
	return nil
}

type componentTaskAddressBinding struct {
	zoneID  string
	address string
}

func (binding componentTaskAddressBinding) ZoneID() string { return binding.zoneID }

func (binding componentTaskAddressBinding) Address() string { return binding.address }

func ComponentTaskAddress(record componentrecord.Record) (componentTaskAddressBinding, bool, error) {
	if record.Desired.Kind != core.ComponentKindIngressCaddy {
		if record.Runtime.PinnedIPv4 != "" {
			return componentTaskAddressBinding{}, false, errs.New(
				errs.KindValidationFailed,
				"non-Caddy Component candidate has a pinned address",
			)
		}
		return componentTaskAddressBinding{}, false, nil
	}
	if !record.Desired.Enabled {
		if record.Runtime.PinnedIPv4 != "" {
			return componentTaskAddressBinding{}, false, errs.New(
				errs.KindValidationFailed,
				"disabled Caddy Component candidate has a pinned address",
			)
		}
		return componentTaskAddressBinding{}, false, nil
	}
	if record.Desired.Config.Caddy == nil {
		return componentTaskAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has no typed config",
		)
	}
	if len(record.Desired.Config.Caddy.ZoneIDs) == 0 {
		return componentTaskAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has no primary Zone",
		)
	}
	zoneID := record.Desired.Config.Caddy.ZoneIDs[0]
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return componentTaskAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has an invalid Zone",
		)
	}
	address, err := netip.ParseAddr(record.Runtime.PinnedIPv4)
	if err != nil || !address.Is4() || address.String() != record.Runtime.PinnedIPv4 {
		return componentTaskAddressBinding{}, false, errs.New(
			errs.KindValidationFailed,
			"enabled Caddy Component candidate has an invalid pinned address",
		)
	}
	return componentTaskAddressBinding{zoneID: zoneID, address: address.String()}, true, nil
}

func ComponentTaskBindingsEqual(
	left componentTaskAddressBinding,
	leftPresent bool,
	right componentTaskAddressBinding,
	rightPresent bool,
) bool {
	return leftPresent == rightPresent && (!leftPresent || left == right)
}

func CloneComponentTaskIntent(intent ComponentTaskIntent) ComponentTaskIntent {
	clone := intent
	clone.Candidates = cloneComponentTaskCandidates(intent.Candidates)
	clone.RouteProjection = CloneComponentTaskRouteProjection(intent.RouteProjection)
	clone.TerminalAt = cloneTimePointer(intent.TerminalAt)
	return clone
}

func cloneComponentTaskCandidates(source []ComponentTaskCandidate) []ComponentTaskCandidate {
	clone := make([]ComponentTaskCandidate, len(source))
	for index, candidate := range source {
		clone[index] = ComponentTaskCandidate{
			CurrentRevision: candidate.CurrentRevision,
			Current:         componentrecord.CloneRecord(candidate.Current),
			Candidate:       componentrecord.CloneRecord(candidate.Candidate),
		}
	}
	return clone
}

func corruptComponentTaskIntent() error {
	return errs.New(errs.KindInternal, "Component candidate intent is corrupt")
}
