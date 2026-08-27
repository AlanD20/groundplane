package releasegroup

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestNewDefaultsFailurePolicyAndOrder(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	group, err := New(Input{
		ID: ids.NewAt(ids.KindReleaseGroup, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		Name: "realtime", ServiceIDs: []string{
			ids.NewAt(ids.KindService, now, 3), ids.NewAt(ids.KindService, now, 4),
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if group.OnFailure != OnFailureSwitchBack {
		t.Fatalf("OnFailure = %q, want %q", group.OnFailure, OnFailureSwitchBack)
	}
	if len(group.Order) != 2 || group.Order[0] != group.ServiceIDs[0] || group.Order[1] != group.ServiceIDs[1] {
		t.Fatalf("Order = %v, want member order %v", group.Order, group.ServiceIDs)
	}
}

func TestNewRejectsInvalidIdentityMembershipOrderAndPolicy(t *testing.T) {
	// Rationale: release-group execution and compensation are explicitly bounded;
	// accepting a 33rd member would bypass the task plan size contract.
	t.Parallel()

	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	groupID := ids.NewAt(ids.KindReleaseGroup, now, 1)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2)
	serviceA := ids.NewAt(ids.KindService, now, 3)
	serviceB := ids.NewAt(ids.KindService, now, 4)

	tests := map[string]Input{
		"wrong group id kind":    {ID: environmentID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, serviceB}},
		"blank name":             {ID: groupID, EnvironmentID: environmentID, Name: " \t", ServiceIDs: []string{serviceA, serviceB}},
		"one member":             {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA}},
		"duplicate member":       {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, serviceA}},
		"wrong member id kind":   {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, groupID}},
		"incomplete order":       {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, serviceB}, Order: []string{serviceA}},
		"foreign order member":   {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, serviceB}, Order: []string{serviceA, ids.NewAt(ids.KindService, now, 5)}},
		"unknown failure policy": {ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: []string{serviceA, serviceB}, OnFailure: "continue"},
	}
	overflow := make([]string, MaximumMembers+1)
	for index := range overflow {
		overflow[index] = ids.NewAt(ids.KindService, now, int64(100+index))
	}
	tests["too many members"] = Input{ID: groupID, EnvironmentID: environmentID, Name: "realtime", ServiceIDs: overflow}
	for name, input := range tests {
		input := input
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := New(input)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("New() error = %v, want validation.failed", err)
			}
		})
	}
}

func TestReplacePreservesStableIdentityAndCopiesMembership(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	current, err := New(Input{
		ID: ids.NewAt(ids.KindReleaseGroup, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		Name: "realtime", ServiceIDs: []string{
			ids.NewAt(ids.KindService, now, 3), ids.NewAt(ids.KindService, now, 4),
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	replacementMembers := []string{current.ServiceIDs[1], current.ServiceIDs[0]}
	replacement, err := Replace(current, Desired{
		Name: "workers", ServiceIDs: replacementMembers, Order: replacementMembers,
		OnFailure: OnFailureLeaveActive,
	})
	if err != nil {
		t.Fatalf("Replace() error = %v", err)
	}
	replacementMembers[0] = "mutated"
	if replacement.ID != current.ID || replacement.EnvironmentID != current.EnvironmentID {
		t.Fatalf("Replace() changed stable identity: %+v", replacement)
	}
	if replacement.Name != "workers" || replacement.ServiceIDs[0] != current.ServiceIDs[1] || replacement.OnFailure != OnFailureLeaveActive {
		t.Fatalf("Replace() = %+v", replacement)
	}
}

func TestValidateRejectsNonNormalizedDurableValue(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	group := Group{
		ID: ids.NewAt(ids.KindReleaseGroup, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		Name: " realtime ", ServiceIDs: []string{
			ids.NewAt(ids.KindService, now, 3), ids.NewAt(ids.KindService, now, 4),
		}, Order: []string{
			ids.NewAt(ids.KindService, now, 3), ids.NewAt(ids.KindService, now, 4),
		}, OnFailure: "",
	}
	if err := Validate(group); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Validate() error = %v, want validation.failed", err)
	}
}

func TestNewPreservesBlueprintNameWhitespace(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	group, err := New(Input{
		ID: ids.NewAt(ids.KindReleaseGroup, now, 1), EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 2),
		Name: " realtime ", ServiceIDs: []string{
			ids.NewAt(ids.KindService, now, 3), ids.NewAt(ids.KindService, now, 4),
		},
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if group.Name != " realtime " {
		t.Fatalf("Name = %q, want exact Blueprint key", group.Name)
	}
}
