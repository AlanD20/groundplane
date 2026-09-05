package app

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: a reapply must not advertise existing native resources as new;
// omitted native resources receive the same preservation preview as extensions.
func TestBlueprintDiffRecognizesExistingNativeResources(t *testing.T) {
	projection := etcd.EnvironmentComposeProjection{
		DesiredServices: []etcd.EnvironmentServiceProjection{{Desired: core.Service{Name: "api"}}},
		DesiredZones:    []etcd.EnvironmentZoneProjection{{Desired: core.Zone{Name: "private"}}},
		Volumes:         []etcd.EnvironmentVolumeIdentity{{Key: "storage", Slug: "renamed-storage"}},
	}
	for _, included := range []bool{false, true} {
		project := &composetypes.Project{}
		want := apiTypes.BlueprintChangeRetain
		if included {
			project.DisabledServices = composetypes.Services{"api": {Name: "api"}}
			project.Networks = composetypes.Networks{"private": {}}
			project.Volumes = composetypes.Volumes{"storage": {}}
			want = apiTypes.BlueprintChangeUpdate
		}
		changes := environmentBlueprintChanges(blueprintparser.AuthoringDocument{},
			blueprintparser.Result{Project: project}, true, projection)
		found := 0
		for _, change := range changes {
			if change.Resource == "compose" {
				continue
			}
			found++
			if change.Action != want {
				t.Fatalf("included=%v: %s %s action=%s, want %s",
					included, change.Resource, change.Key, change.Action, want)
			}
		}
		if found != 3 {
			t.Fatalf("included=%v: found %d native resources, want 3", included, found)
		}
	}
}
