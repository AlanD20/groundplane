package volume

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func TestVolumeReadServicePagesPinnedProjection(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	repository := &volumeReadTestRepository{projection: etcd.Versioned[etcd.EnvironmentComposeProjection]{
		Record: etcd.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
			Volumes: []etcd.EnvironmentVolumeIdentity{
				{ID: ids.NewAt(ids.KindVolume, at, 3), Slug: "data-a", Key: "data-a"},
				{ID: ids.NewAt(ids.KindVolume, at, 4), Slug: "data-b", Key: "data-b"},
			},
		}, Revision: 7, ReadRevision: 7,
	}}
	service, err := NewReadService(repository)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListVolumes(context.Background(), environmentID, etcd.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := service.ListVolumes(context.Background(), environmentID, etcd.PageRequest{
		Limit: 1, Cursor: first.NextCursor,
	})
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.Slug != "data-b" || second.NextCursor != "" {
		t.Fatalf("second page = %#v, %v", second, err)
	}
}

type volumeReadTestRepository struct {
	projection etcd.Versioned[etcd.EnvironmentComposeProjection]
}

func (repository *volumeReadTestRepository) GetEnvironment(
	context.Context, string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{}, nil
}

func (repository *volumeReadTestRepository) GetEnvironmentComposeProjection(
	context.Context, string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *volumeReadTestRepository) GetEnvironmentComposeProjectionRevision(
	context.Context, string, string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *volumeReadTestRepository) FindEnvironmentVolume(
	context.Context, string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], etcd.EnvironmentVolumeIdentity, error) {
	return repository.projection, repository.projection.Record.Volumes[0], nil
}

func (repository *volumeReadTestRepository) ResolveEnvironmentVolumeAtRevision(
	context.Context, string, string, string, int64,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], etcd.EnvironmentVolumeIdentity, error) {
	return repository.projection, repository.projection.Record.Volumes[0], nil
}

func (repository *volumeReadTestRepository) ResolveVolumeRemovalImpactAtRevision(
	context.Context, string, string, int64, time.Time,
) (etcd.BackupVolumeRemovalImpact, error) {
	return etcd.BackupVolumeRemovalImpact{}, nil
}
