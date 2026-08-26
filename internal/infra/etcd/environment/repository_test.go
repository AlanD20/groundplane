package environment

import (
	"context"
	"errors"
	"reflect"
	"testing"

	etcdinfra "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

type hierarchyFake struct {
	get       etcdinfra.Versioned[etcdinfra.EnvironmentRecord]
	page      etcdinfra.Page[etcdinfra.EnvironmentRecord]
	getErr    error
	listErr   error
	projectID string
	request   etcdinfra.PageRequest
}

func (fake *hierarchyFake) GetEnvironment(
	context.Context,
	string,
) (etcdinfra.Versioned[etcdinfra.EnvironmentRecord], error) {
	return fake.get, fake.getErr
}

func (fake *hierarchyFake) ListEnvironments(
	_ context.Context,
	projectID string,
	request etcdinfra.PageRequest,
) (etcdinfra.Page[etcdinfra.EnvironmentRecord], error) {
	fake.projectID, fake.request = projectID, request
	return fake.page, fake.listErr
}

type reservationsFake struct {
	values    map[string][]string
	err       error
	revisions []int64
}

func (fake *reservationsFake) ListZoneSubnetReservationsAtRevision(
	_ context.Context,
	id string,
	revision int64,
) ([]string, error) {
	fake.revisions = append(fake.revisions, revision)
	return fake.values[id], fake.err
}

func storedEnvironment(id string) etcdinfra.EnvironmentRecord {
	return etcdinfra.EnvironmentRecord{
		ID: id, ProjectID: testProjectID, Name: "production", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/tenant/" + testProjectID + "/" + id,
		ProvisioningState: etcdinfra.EnvironmentProvisioningReady,
	}
}

// Rationale: Get must preserve stable identity, allocation, and filesystem
// identity while deriving capacity from the same durable revision.
func TestRepositoryGetProjectsIdentityPoolVolumeAndCapacity(t *testing.T) {
	deletionTaskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	record := storedEnvironment(testEnvironmentID)
	record.DeletionTaskID = deletionTaskID
	hierarchy := &hierarchyFake{get: etcdinfra.Versioned[etcdinfra.EnvironmentRecord]{
		Record: record, ReadRevision: 41,
	}}
	zones := &reservationsFake{values: map[string][]string{testEnvironmentID: {"10.40.1.0/24", "10.40.2.0/24"}}}
	got, err := NewRepository(hierarchy, zones).Get(context.Background(), testEnvironmentID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	if got.ID != testEnvironmentID || got.ProjectID != testProjectID || got.NetworkPool != "10.40.0.0/16" ||
		got.VolumeDir == "" || !reflect.DeepEqual(got.ZoneSubnets, []string{"10.40.1.0/24", "10.40.2.0/24"}) ||
		got.DeletionTaskID == nil || *got.DeletionTaskID != deletionTaskID ||
		!reflect.DeepEqual(zones.revisions, []int64{41}) {
		t.Fatalf("GetEnvironment() = %#v, revisions %v", got, zones.revisions)
	}
}

// Rationale: List must forward the operator's pagination request unchanged,
// preserve the opaque cursor, and use the page's fixed revision for every item.
func TestRepositoryListForwardsPaginationCursorAndFixedRevision(t *testing.T) {
	secondID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	hierarchy := &hierarchyFake{page: etcdinfra.Page[etcdinfra.EnvironmentRecord]{
		Items: []etcdinfra.Versioned[etcdinfra.EnvironmentRecord]{
			{Record: storedEnvironment(testEnvironmentID)}, {Record: storedEnvironment(secondID)},
		}, NextCursor: "opaque-next", Revision: 73,
	}}
	zones := &reservationsFake{values: map[string][]string{testEnvironmentID: {}, secondID: {}}}
	got, err := NewRepository(hierarchy, zones).List(
		context.Background(),
		testProjectID,
		PageRequest{Limit: 17, Cursor: "opaque-current"},
	)
	if err != nil {
		t.Fatalf("ListEnvironments() error = %v", err)
	}
	if hierarchy.projectID != testProjectID || hierarchy.request.Limit != 17 ||
		hierarchy.request.Cursor != "opaque-current" || got.NextCursor != "opaque-next" || len(got.Items) != 2 ||
		!reflect.DeepEqual(zones.revisions, []int64{73, 73}) {
		t.Fatalf("ListEnvironments() = %#v, request %#v, revisions %v", got, hierarchy.request, zones.revisions)
	}
}

// Rationale: repository and reservation failures retain their classified
// error identity instead of being flattened at the adapter boundary.
func TestRepositoryPropagatesReadFailures(t *testing.T) {
	want := errs.New(errs.KindStorageUnavailable, "etcd unavailable")
	if _, err := NewRepository(&hierarchyFake{getErr: want}, &reservationsFake{}).
		Get(context.Background(), testEnvironmentID); !errors.Is(err, want) {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	hierarchy := &hierarchyFake{page: etcdinfra.Page[etcdinfra.EnvironmentRecord]{
		Items: []etcdinfra.Versioned[etcdinfra.EnvironmentRecord]{
			{Record: storedEnvironment(testEnvironmentID)},
		}, Revision: 9,
	}}
	if _, err := NewRepository(hierarchy, &reservationsFake{err: want}).List(
		context.Background(), testProjectID, PageRequest{},
	); !errors.Is(err, want) {
		t.Fatalf("ListEnvironments() error = %v", err)
	}
}
