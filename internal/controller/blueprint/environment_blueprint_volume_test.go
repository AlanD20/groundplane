package blueprint

import (
	"reflect"
	"testing"

	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
)

// Rationale: Blueprint retries must heal missing bind directories for every
// declared managed Volume, even when the Volume already exists in desired state.
func TestManagedEnvironmentVolumeIDsIncludesAllDeclaredVolumes(t *testing.T) {
	got := managedEnvironmentVolumeIDs([]testcomposeidentity.Resource{
		{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
		{ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	})
	want := []string{
		"vol_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"vol_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("managedEnvironmentVolumeIDs() = %v, want %v", got, want)
	}
}
