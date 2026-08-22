package controller

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: Blueprint replay must preserve stable ids for unchanged Compose keys while additions and removals
// remain explicit.
func TestReconcileOwnedComposeIdentitiesPreservesCreatesAndRemoves(t *testing.T) {
	serviceAPI := composeIdentityTestID(ids.KindService, 1)
	serviceRemoved := composeIdentityTestID(ids.KindService, 2)
	serviceWorker := composeIdentityTestID(ids.KindService, 3)
	networkFrontend := composeIdentityTestID(ids.KindNetwork, 4)
	networkRemoved := composeIdentityTestID(ids.KindNetwork, 5)
	networkBackend := composeIdentityTestID(ids.KindNetwork, 6)
	volumeData := composeIdentityTestID(ids.KindVolume, 7)
	volumeRemoved := composeIdentityTestID(ids.KindVolume, 8)
	volumeCache := composeIdentityTestID(ids.KindVolume, 9)

	project := &composetypes.Project{
		Services: composetypes.Services{"api": composetypes.ServiceConfig{}},
		DisabledServices: composetypes.Services{
			"worker": composetypes.ServiceConfig{},
		},
		Networks: composetypes.Networks{
			"backend":  composetypes.NetworkConfig{},
			"frontend": composetypes.NetworkConfig{},
		},
		Volumes: composetypes.Volumes{
			"cache": composetypes.VolumeConfig{},
			"data":  composetypes.VolumeConfig{},
		},
	}
	previous := ComposeIdentitySnapshot{
		Services: []ComposeResourceIdentity{{ID: serviceAPI, Name: "api"}, {ID: serviceRemoved, Name: "old"}},
		Networks: []ComposeResourceIdentity{
			{ID: networkFrontend, Name: "frontend"},
			{ID: networkRemoved, Name: "old"},
		},
		Volumes: []ComposeResourceIdentity{{ID: volumeData, Name: "data"}, {ID: volumeRemoved, Name: "old"}},
	}
	generated := map[ids.Kind][]string{
		ids.KindService: {serviceWorker},
		ids.KindNetwork: {networkBackend},
		ids.KindVolume:  {volumeCache},
	}

	changes, err := ReconcileOwnedComposeIdentities(project, previous, func(kind ids.Kind) string {
		value := generated[kind][0]
		generated[kind] = generated[kind][1:]
		return value
	})
	if err != nil {
		t.Fatalf("ReconcileOwnedComposeIdentities() error = %v", err)
	}
	want := ComposeIdentityChanges{
		Current: ComposeIdentitySnapshot{
			Services: []ComposeResourceIdentity{{ID: serviceAPI, Name: "api"}, {ID: serviceWorker, Name: "worker"}},
			Networks: []ComposeResourceIdentity{
				{ID: networkBackend, Name: "backend"},
				{ID: networkFrontend, Name: "frontend"},
			},
			Volumes: []ComposeResourceIdentity{{ID: volumeCache, Name: "cache"}, {ID: volumeData, Name: "data"}},
		},
		RemovedServiceIDs: []string{serviceRemoved},
		RemovedNetworkIDs: []string{networkRemoved},
		RemovedVolumeIDs:  []string{volumeRemoved},
	}
	if !reflect.DeepEqual(changes, want) {
		t.Fatalf("ReconcileOwnedComposeIdentities() = %#v, want %#v", changes, want)
	}
}

// Rationale: changing an authored key is an add plus remove and must never infer a rename that reuses the old id.
func TestReconcileOwnedComposeIdentitiesDoesNotInferRename(t *testing.T) {
	oldID := composeIdentityTestID(ids.KindService, 10)
	newID := composeIdentityTestID(ids.KindService, 11)
	project := &composetypes.Project{Services: composetypes.Services{"web": composetypes.ServiceConfig{}}}
	previous := ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{{ID: oldID, Name: "api"}}}

	changes, err := ReconcileOwnedComposeIdentities(project, previous, func(ids.Kind) string { return newID })
	if err != nil {
		t.Fatalf("ReconcileOwnedComposeIdentities() error = %v", err)
	}
	if got := changes.Current.Services; !reflect.DeepEqual(got, []ComposeResourceIdentity{{ID: newID, Name: "web"}}) {
		t.Fatalf("current services = %#v, want new identity", got)
	}
	if !reflect.DeepEqual(changes.RemovedServiceIDs, []string{oldID}) {
		t.Fatalf("removed service ids = %#v, want %#v", changes.RemovedServiceIDs, []string{oldID})
	}
}

// Rationale: duplicate or malformed durable identity snapshots are Controller corruption and must fail closed.
func TestReconcileOwnedComposeIdentitiesRejectsCorruptSnapshot(t *testing.T) {
	serviceA := composeIdentityTestID(ids.KindService, 12)
	serviceB := composeIdentityTestID(ids.KindService, 13)
	cases := map[string]ComposeIdentitySnapshot{
		"duplicate name": {
			Services: []ComposeResourceIdentity{{ID: serviceA, Name: "api"}, {ID: serviceB, Name: "api"}},
		},
		"duplicate id": {
			Services: []ComposeResourceIdentity{{ID: serviceA, Name: "api"}, {ID: serviceA, Name: "worker"}},
		},
		"wrong id kind": {
			Services: []ComposeResourceIdentity{{ID: composeIdentityTestID(ids.KindNetwork, 14), Name: "api"}},
		},
	}
	for name, previous := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReconcileOwnedComposeIdentities(&composetypes.Project{}, previous, ids.New)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("ReconcileOwnedComposeIdentities() error = %v, want %q", err, errs.CodeInternal)
			}
		})
	}
}

// Rationale: compose-go may retain the same profile-gated service in both maps, which cannot map to two durable
// identities.
func TestReconcileOwnedComposeIdentitiesRejectsDuplicateActiveAndDisabledService(t *testing.T) {
	project := &composetypes.Project{
		Services:         composetypes.Services{"api": composetypes.ServiceConfig{}},
		DisabledServices: composetypes.Services{"api": composetypes.ServiceConfig{}},
	}

	_, err := ReconcileOwnedComposeIdentities(project, ComposeIdentitySnapshot{}, ids.New)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("ReconcileOwnedComposeIdentities() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// Rationale: an external network belongs to another owner and must not receive a new identity in the consumer.
func TestReconcileOwnedComposeIdentitiesExcludesExternalNetworks(t *testing.T) {
	project := &composetypes.Project{
		Networks: composetypes.Networks{"shared": composetypes.NetworkConfig{External: true}},
	}
	allocatorCalled := false

	changes, err := ReconcileOwnedComposeIdentities(project, ComposeIdentitySnapshot{}, func(ids.Kind) string {
		allocatorCalled = true
		return ""
	})
	if err != nil {
		t.Fatalf("ReconcileOwnedComposeIdentities() error = %v", err)
	}
	if allocatorCalled || len(changes.Current.Networks) != 0 {
		t.Fatalf("external network was treated as owned: %#v", changes.Current.Networks)
	}
}

// Rationale: external volume ownership is not specified and must not be mistaken for a newly owned volume.
func TestReconcileOwnedComposeIdentitiesRejectsExternalVolume(t *testing.T) {
	project := &composetypes.Project{
		Volumes: composetypes.Volumes{"shared": composetypes.VolumeConfig{External: true}},
	}

	_, err := ReconcileOwnedComposeIdentities(project, ComposeIdentitySnapshot{}, ids.New)
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("ReconcileOwnedComposeIdentities() error = %v, want %q", err, errs.CodeInternal)
	}
}

// Rationale: an allocator defect must not introduce a wrong-kind or reused stable id into desired state.
func TestReconcileOwnedComposeIdentitiesRejectsInvalidGeneratedID(t *testing.T) {
	oldID := composeIdentityTestID(ids.KindService, 15)
	project := &composetypes.Project{Services: composetypes.Services{"web": composetypes.ServiceConfig{}}}
	previous := ComposeIdentitySnapshot{Services: []ComposeResourceIdentity{{ID: oldID, Name: "old"}}}
	cases := map[string]string{
		"wrong kind": composeIdentityTestID(ids.KindNetwork, 16),
		"reused id":  oldID,
	}
	for name, generated := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ReconcileOwnedComposeIdentities(project, previous, func(ids.Kind) string { return generated })
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("ReconcileOwnedComposeIdentities() error = %v, want %q", err, errs.CodeInternal)
			}
		})
	}
}

func composeIdentityTestID(kind ids.Kind, seed int64) string {
	return ids.NewAt(kind, time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC), seed)
}
