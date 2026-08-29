package controller

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func mustComposeIdentitySnapshotFromProjection(
	t *testing.T,
	projection etcd.EnvironmentComposeProjection,
) ComposeIdentitySnapshot {
	t.Helper()
	snapshot, err := ComposeIdentitySnapshotFromProjection(projection)
	if err != nil {
		t.Fatalf("ComposeIdentitySnapshotFromProjection() error = %v", err)
	}
	return snapshot
}

func TestComposeIdentitySnapshotRejectsDuplicateComponentOwnership(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	projection := etcd.EnvironmentComposeProjection{Components: []etcd.ComponentRecord{
		{
			Desired: etcd.ComponentDesiredRecord{ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			Runtime: etcd.ComponentRuntimeRecord{GeneratedServices: []string{serviceID}},
		},
		{
			Desired: etcd.ComponentDesiredRecord{ID: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
			Runtime: etcd.ComponentRuntimeRecord{GeneratedServices: []string{serviceID}},
		},
	}}
	if _, err := ComposeIdentitySnapshotFromProjection(projection); err == nil {
		t.Fatal("ComposeIdentitySnapshotFromProjection(duplicate owner) error = nil")
	}
}
