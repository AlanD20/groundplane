package serviceremovalreferences

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPrepareServiceRemovalReferenceGuards(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	serviceID := ids.NewAt(ids.KindService, at, 2)
	otherServiceID := ids.NewAt(ids.KindService, at, 3)
	thirdServiceID := ids.NewAt(ids.KindService, at, 4)
	groupID := ids.NewAt(ids.KindReleaseGroup, at, 5)
	current := servicerecord.ServiceRecord{
		EnvironmentID: environmentID,
		Desired:       core.Service{ID: serviceID, Name: "api"},
	}
	projection := etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record:   projectionrecord.EnvironmentComposeProjection{EnvironmentID: environmentID},
		Revision: 7, ReadRevision: 7,
	}

	t.Run("explicit Entry exposure blocks removal", func(t *testing.T) {
		candidate := projection
		candidate.Record.Entries = []entryrecord.Record{{
			EnvironmentID: environmentID,
			Entry:         core.EnvEntry{Exposure: []string{"api"}},
		}}
		_, err := Prepare(context.Background(), newFakeReferenceStore(7), current, candidate)
		assertReferenceErrorKind(t, err, errs.KindResourceInUse)
	})

	t.Run("Release Group membership blocks removal", func(t *testing.T) {
		store := newFakeReferenceStore(7)
		store.putGroup(t, groupID, environmentID, []string{serviceID, otherServiceID})
		_, err := Prepare(context.Background(), store, current, projection)
		assertReferenceErrorKind(t, err, errs.KindResourceInUse)
	})

	t.Run("unreferenced Service is allowed", func(t *testing.T) {
		store := newFakeReferenceStore(7)
		store.putGroup(t, groupID, environmentID, []string{otherServiceID, thirdServiceID})
		candidate := projection
		candidate.Record.Entries = []entryrecord.Record{{
			EnvironmentID: environmentID,
			Entry:         core.EnvEntry{Exposure: []string{"all"}},
		}}
		guards, err := Prepare(context.Background(), store, current, candidate)
		if err != nil {
			t.Fatal(err)
		}
		conditions := guards.Conditions()
		if len(conditions) != 1 || conditions[0] != (etcdstore.Condition{
			Key: groupstore.ReleaseGroupCollectionEpochKey(environmentID),
		}) {
			t.Fatalf("reference guards = %#v", conditions)
		}
	})
}

func assertReferenceErrorKind(t *testing.T, err error, want errs.Kind) {
	t.Helper()
	got, ok := errs.KindOf(err)
	if !ok || got != want {
		t.Fatalf("error = %v, kind = %v, want %v", err, got, want)
	}
}

type fakeReferenceStore struct {
	revision int64
	values   map[string]etcdstore.KeyValue
}

func newFakeReferenceStore(revision int64) *fakeReferenceStore {
	return &fakeReferenceStore{revision: revision, values: make(map[string]etcdstore.KeyValue)}
}

func (store *fakeReferenceStore) putGroup(
	t *testing.T,
	groupID string,
	environmentID string,
	serviceIDs []string,
) {
	t.Helper()
	value, err := recordcodec.Encode("release_group", struct {
		ID            string           `json:"id"`
		EnvironmentID string           `json:"environment_id"`
		Name          string           `json:"name"`
		ServiceIDs    []string         `json:"service_ids"`
		Order         []string         `json:"order"`
		DefaultTag    string           `json:"default_tag,omitempty"`
		OnFailure     domain.OnFailure `json:"on_failure"`
	}{
		ID: groupID, EnvironmentID: environmentID, Name: "production",
		ServiceIDs: serviceIDs, Order: serviceIDs, OnFailure: domain.OnFailureSwitchBack,
	})
	if err != nil {
		t.Fatal(err)
	}
	store.values[groupstore.ReleaseGroupRecordKey(groupID)] = etcdstore.KeyValue{
		Key: groupstore.ReleaseGroupRecordKey(groupID), Value: value,
		Version: 1, ModRevision: store.revision,
	}
	ownerKey := groupstore.ReleaseGroupOwnerKey(environmentID, groupID)
	store.values[ownerKey] = etcdstore.KeyValue{
		Key: ownerKey, Value: []byte(groupID), Version: 1, ModRevision: store.revision,
	}
}

func (store *fakeReferenceStore) Range(
	_ context.Context,
	request etcdstore.RangeRequest,
) (*etcdstore.RangeResult, error) {
	keys := make([]string, 0)
	for key := range store.values {
		if strings.HasPrefix(key, request.Prefix) && key > request.StartExclusive {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make([]etcdstore.KeyValue, 0, len(keys))
	for _, key := range keys {
		value := store.values[key]
		value.Value = append([]byte(nil), value.Value...)
		values = append(values, value)
	}
	return &etcdstore.RangeResult{Values: values, ReadRevision: request.Revision}, nil
}

func (store *fakeReferenceStore) GetMany(
	_ context.Context,
	request etcdstore.GetManyRequest,
) (*etcdstore.GetManyResult, error) {
	values := make([]*etcdstore.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		if value, found := store.values[key]; found {
			copy := value
			copy.Value = append([]byte(nil), value.Value...)
			values[index] = &copy
		}
	}
	return &etcdstore.GetManyResult{Values: values, ReadRevision: request.Revision}, nil
}
