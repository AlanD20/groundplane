package volume

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func TestVolumeReadServicePagesPinnedProjection(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 26, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	repository := &volumeReadTestRepository{
		projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
			Record: testenvironmentprojection.EnvironmentComposeProjection{
				EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
				Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{
					{ID: ids.NewAt(ids.KindVolume, at, 3), Slug: "data-a", Key: "data-a"},
					{ID: ids.NewAt(ids.KindVolume, at, 4), Slug: "data-b", Key: "data-b"},
				},
			}, Revision: 7, ReadRevision: 7,
		},
	}
	service, err := NewReadService(repository)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.ListVolumes(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first page = %#v, %v", first, err)
	}
	second, err := service.ListVolumes(context.Background(), environmentID, testkeyvalue.PageRequest{
		Limit: 1, Cursor: first.NextCursor,
	})
	if err != nil || len(second.Items) != 1 || second.Items[0].Record.Slug != "data-b" || second.NextCursor != "" {
		t.Fatalf("second page = %#v, %v", second, err)
	}
}

func TestVolumeConsumerServiceNamesUsesDesiredAndAddressedGeneratedServices(t *testing.T) {
	t.Parallel()
	const (
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		volumeID      = "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		desiredID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		generatedID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		unmountedID   = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		componentID   = "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	artifact, err := proto.Marshal(&agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{
		{ServiceId: generatedID, ComposeName: "router", OwnerComponentId: componentID},
		{ServiceId: unmountedID, ComposeName: "unused", OwnerComponentId: componentID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	projection := testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID: environmentID, ComposeArtifact: artifact,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: environmentID, Desired: core.Service{ID: desiredID, Name: "api"},
		}},
		Components: []testcomponents.Record{{
			Desired: testcomponents.DesiredRecord{ID: componentID},
			Runtime: testcomponents.RuntimeRecord{GeneratedServices: []string{generatedID, unmountedID}},
		}},
		VolumeMounts: []testenvironmentprojection.EnvironmentServiceVolumeMount{
			{ServiceID: desiredID, VolumeID: volumeID, Target: "/data"},
			{ServiceID: generatedID, VolumeID: volumeID, Target: "/config"},
		},
	}
	names := volumeConsumerServiceNames(projection, volumeID)
	if len(names) != 2 || names[desiredID] != "api" || names[generatedID] != "router" {
		t.Fatalf("Volume consumer names = %#v", names)
	}
}

// Rationale: Volume collection reads follow the global public pagination
// contract, so the inclusive upper bound is 200 rather than a private
// capability-specific limit.
func TestVolumeReadServiceUsesGlobalPaginationBounds(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 29, 0, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	service, err := NewReadService(
		&volumeReadTestRepository{
			projection: testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
				Record: testenvironmentprojection.EnvironmentComposeProjection{
					EnvironmentID: environmentID,
					RevisionID:    revisionID,
				},
			},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.ListVolumes(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 200}); err != nil {
		t.Fatalf("ListVolumes(limit 200) error = %v", err)
	}
	if _, err := service.ListVolumes(context.Background(), environmentID, testkeyvalue.PageRequest{Limit: 201}); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("ListVolumes(limit 201) error = %v, want validation failure", err)
	}
}

type volumeReadTestRepository struct {
	projection testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]
}

func (repository *volumeReadTestRepository) GetEnvironment(
	context.Context, string,
) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{}, nil
}

func (repository *volumeReadTestRepository) GetEnvironmentComposeProjection(
	context.Context, string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *volumeReadTestRepository) GetEnvironmentComposeProjectionRevision(
	context.Context, string, string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return repository.projection, true, nil
}

func (repository *volumeReadTestRepository) FindEnvironmentVolume(
	context.Context,
	string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], testenvironmentprojection.EnvironmentVolumeIdentity, error) {
	return repository.projection, repository.projection.Record.Volumes[0], nil
}

func (repository *volumeReadTestRepository) ResolveEnvironmentVolumeAtRevision(
	context.Context,
	string,
	string,
	string,
	int64,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], testenvironmentprojection.EnvironmentVolumeIdentity, error) {
	return repository.projection, repository.projection.Record.Volumes[0], nil
}

func (repository *volumeReadTestRepository) ResolveVolumeRemovalImpactAtRevision(
	context.Context, string, string, int64, time.Time,
) (testbackupruntime.BackupVolumeRemovalImpact, error) {
	return testbackupruntime.BackupVolumeRemovalImpact{}, nil
}
