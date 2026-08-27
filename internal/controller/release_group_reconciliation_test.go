package controller

import (
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Blueprint replay must preserve stable Release Group IDs and exact authored name whitespace while reporting only omitted live identities.
func TestReconcileBlueprintReleaseGroupsPreservesIDsAndReportsRemovals(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	serviceAPI := core.Service{ID: ids.NewAt(ids.KindService, now, 2), Name: "api"}
	serviceDB := core.Service{ID: ids.NewAt(ids.KindService, now, 3), Name: "db"}
	existingID := ids.NewAt(ids.KindReleaseGroup, now, 4)
	removedID := ids.NewAt(ids.KindReleaseGroup, now, 5)

	result, err := ReconcileBlueprintReleaseGroups(
		environmentID,
		map[string]core.ReleaseGroupSpec{
			" release ": {
				Services: []string{"api", "db"}, Order: []string{"db", "api"}, Tag: "stable",
			},
		},
		[]core.Service{serviceAPI, serviceDB},
		[]ReleaseGroupIdentity{{ID: existingID, Name: " release "}, {ID: removedID, Name: "old"}},
		func() string { return ids.NewAt(ids.KindReleaseGroup, now, 6) },
	)
	if err != nil {
		t.Fatalf("ReconcileBlueprintReleaseGroups() error = %v", err)
	}
	if len(result.Desired) != 1 || result.Desired[0].ID != existingID || result.Desired[0].Name != " release " {
		t.Fatalf("Desired = %+v, want existing identity and exact name", result.Desired)
	}
	group := result.Desired[0]
	if group.ServiceIDs[0] != serviceAPI.ID || group.ServiceIDs[1] != serviceDB.ID ||
		group.Order[0] != serviceDB.ID || group.Order[1] != serviceAPI.ID ||
		group.DefaultTag != "stable" || group.OnFailure != domain.OnFailureSwitchBack {
		t.Fatalf("resolved group = %+v", group)
	}
	if len(result.Removed) != 1 || result.Removed[0].ID != removedID || result.Removed[0].Name != "old" {
		t.Fatalf("Removed = %+v, want old group", result.Removed)
	}
}

// Rationale: an unresolved authored member is a typed Service lookup failure, never an unclassified string error.
func TestReconcileBlueprintReleaseGroupsRejectsUnknownService(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1)
	service := core.Service{ID: ids.NewAt(ids.KindService, now, 2), Name: "api"}
	_, err := ReconcileBlueprintReleaseGroups(
		environmentID,
		map[string]core.ReleaseGroupSpec{"release": {Services: []string{"api", "missing"}}},
		[]core.Service{service}, nil, func() string { return ids.NewAt(ids.KindReleaseGroup, now, 3) },
	)
	if !errors.Is(err, errs.New(errs.KindServiceNotFound, "")) {
		t.Fatalf("error = %v, want service.not_found", err)
	}
}

// Rationale: every invalid authored Release Group shape must cross the controller boundary as the repository's validation.failed error type.
func TestReconcileBlueprintReleaseGroupsClassifiesInvalidSpec(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 11)
	service := core.Service{ID: ids.NewAt(ids.KindService, now, 12), Name: "api"}
	_, err := ReconcileBlueprintReleaseGroups(
		environmentID,
		map[string]core.ReleaseGroupSpec{"release": {Services: []string{"api"}}},
		[]core.Service{service}, nil, func() string {
			return ids.NewAt(ids.KindReleaseGroup, now, 13)
		},
	)
	kind, ok := errs.KindOf(err)
	if !ok || kind != errs.KindValidationFailed {
		t.Fatalf("error kind = %q/%t for %v, want validation.failed", kind, ok, err)
	}
}
