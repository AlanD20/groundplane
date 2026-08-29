package etcdcontainer

import (
	"testing"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
)

// Rationale: Docker may report mount options that are invisible in the basic
// source/target fields; those options must trigger drift reconciliation.
func TestMatchesDesiredRejectsTypedMountDrift(t *testing.T) {
	desired := createOptions()
	inspect := container.InspectResponse{Config: desired.Config, HostConfig: desired.HostConfig}
	inspect.HostConfig.Mounts = append([]mount.Mount(nil), desired.HostConfig.Mounts...)
	inspect.HostConfig.Mounts[0].BindOptions = &mount.BindOptions{}
	if matchesDesired(inspect) {
		t.Fatal("mount option drift was accepted")
	}

	inspect.HostConfig.Mounts[0] = desired.HostConfig.Mounts[0]
	inspect.HostConfig.Mounts[0].Consistency = mount.ConsistencyCached
	if matchesDesired(inspect) {
		t.Fatal("mount consistency drift was accepted")
	}
}

// Rationale: the Docker SDK distinguishes absent and explicitly empty slices
// and maps; matching must retain that distinction when comparing inspection
// results with the requested container.
func TestEtcdTypedEqualityPreservesPresence(t *testing.T) {
	if equalEtcdStrings(nil, []string{}) || equalEtcdStringMap(nil, map[string]string{}) ||
		equalEtcdStringMatrix(nil, [][]string{}) || equalEtcdMounts(nil, []mount.Mount{}) {
		t.Fatal("nil and empty Docker values were treated as equal")
	}
	if !equalEtcdStrings(nil, nil) || !equalEtcdStringMap(nil, nil) ||
		!equalEtcdStringMatrix(nil, nil) || !equalEtcdMounts(nil, nil) {
		t.Fatal("identical nil Docker values were rejected")
	}
}
