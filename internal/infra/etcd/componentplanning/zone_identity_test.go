package componentplanning

import (
	core "github.com/AlanD20/groundplane/internal/core"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	testing "testing"
)

func TestComponentCandidateSelectedZoneIdentityFence(t *testing.T) {

	t.Parallel()
	current := testzones.Record{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Desired:       core.Zone{ID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}
	matching := current
	wrongEnvironment := current
	wrongEnvironment.EnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	wrongZone := current
	wrongZone.Desired.ID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	for _, test := range []struct {
		name            string
		selected        testzones.Record
		selectedPresent bool
		want            bool
	}{
		{name: "absent selected Zone", selectedPresent: false, want: true},
		{name: "matching selected Zone", selected: matching, selectedPresent: true, want: true},
		{name: "wrong selected Environment", selected: wrongEnvironment, selectedPresent: true, want: false},
		{name: "wrong selected Zone", selected: wrongZone, selectedPresent: true, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := componentCandidateSelectedZoneIdentityMatches(current, test.selected, test.selectedPresent); got != test.want {
				t.Fatalf("componentCandidateSelectedZoneIdentityMatches() = %t, want %t", got, test.want)
			}
		})
	}
}
