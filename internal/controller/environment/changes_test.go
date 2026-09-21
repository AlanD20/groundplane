package environment

import (
	"testing"

	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
)

// Rationale: a successful rename returns the complete Environment projection,
// so immutable allocation and filesystem identity must not disappear from the
// mutation response while remaining present in a later detail read.
func TestEnvironmentAPIProjectionPreservesImmutableIdentity(t *testing.T) {
	t.Parallel()

	record := testhierarchy.EnvironmentRecord{
		ID:                "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectID:         "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Name:              "renamed-production",
		NetworkPool:       "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
		CreateTaskID:      "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
	}

	projected := environmentAPI(record, NetworkCapacity{
		TotalAddresses: 65536, AvailableAddresses: 65536,
	})
	if projected.ID != record.ID || projected.ProjectID != record.ProjectID ||
		projected.Name != record.Name || projected.NetworkPool != record.NetworkPool ||
		projected.VolumeDir != record.VolumeDir || projected.CreateTaskID != nil ||
		projected.NetworkCapacity.TotalAddresses != 65536 {
		t.Fatalf("environmentAPI() = %#v", projected)
	}
}
