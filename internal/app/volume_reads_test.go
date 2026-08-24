package app

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Rationale: volume.list must delegate one fixed-revision Environment and
// owner-index projection rather than composing two independent reads.
func TestVolumeReadServiceScopesListAfterEnvironmentRead(t *testing.T) {
	repository := &volumeReadTestRepository{}
	service, err := newVolumeReadService(repository)
	if err != nil {
		t.Fatal(err)
	}
	want := etcd.PageRequest{Limit: 7, Cursor: "cursor"}
	if _, err := service.ListVolumes(context.Background(), "env_01AAAAAAAAAAAAAAAAAAAAAAAA", want); err != nil {
		t.Fatal(err)
	}
	if repository.environmentID != "env_01AAAAAAAAAAAAAAAAAAAAAAAA" || repository.request != want {
		t.Fatalf("scope/request = %q, %#v", repository.environmentID, repository.request)
	}
}

type volumeReadTestRepository struct {
	environmentID string
	request       etcd.PageRequest
}

func (repository *volumeReadTestRepository) ListEnvironmentVolumes(
	_ context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.VolumeRecord], error) {
	repository.environmentID = environmentID
	repository.request = request
	return etcd.Page[etcd.VolumeRecord]{Items: []etcd.Versioned[etcd.VolumeRecord]{}}, nil
}
