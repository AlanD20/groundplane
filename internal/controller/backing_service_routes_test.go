package controller

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeBackingServiceReader struct {
	detail      etcd.Versioned[etcd.BackingServiceRecord]
	page        etcd.Page[etcd.BackingServiceRecord]
	wantRequest etcd.PageRequest
	listed      bool
}

func (fake *fakeBackingServiceReader) GetBackingService(
	_ context.Context,
	projectID string,
) (etcd.Versioned[etcd.BackingServiceRecord], error) {
	if projectID != fake.detail.Record.ProjectID {
		return etcd.Versioned[etcd.BackingServiceRecord]{}, nil
	}
	return fake.detail, nil
}

func (fake *fakeBackingServiceReader) ListBackingServices(
	_ context.Context,
	request etcd.PageRequest,
) (etcd.Page[etcd.BackingServiceRecord], error) {
	fake.listed = request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the REST facade must expose exactly the accepted Project, main Environment, and
// adapter Service ids while preserving the repository's cursor unchanged.
func TestBackingServiceRoutesProjectExactFacade(t *testing.T) {
	t.Parallel()
	record := etcd.BackingServiceRecord{
		ProjectID: ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
		ServiceID: ids.New(ids.KindService), BackingNetworkID: ids.New(ids.KindNetwork),
	}
	request := etcd.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeBackingServiceReader{
		detail: etcd.Versioned[etcd.BackingServiceRecord]{Record: record},
		page: etcd.Page[etcd.BackingServiceRecord]{
			Items:      []etcd.Versioned[etcd.BackingServiceRecord]{{Record: record}},
			NextCursor: "next",
		},
		wantRequest: request,
	}
	server := &Server{backingServices: reader}
	page, err := server.listBackingServices(context.Background(), &backingServiceListInput{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil || !reader.listed || len(page.Body.Items) != 1 || page.Body.NextCursor != "next" {
		t.Fatalf("listBackingServices() = %#v, %v, listed %t", page, err, reader.listed)
	}
	want := backingServiceResponse(record)
	if !reflect.DeepEqual(page.Body.Items[0], want) {
		t.Fatalf("backing-service page item = %#v, want %#v", page.Body.Items[0], want)
	}
	detail, err := server.showBackingService(
		context.Background(), &backingServiceShowInput{ProjectID: record.ProjectID},
	)
	if err != nil || !reflect.DeepEqual(detail.Body, want) {
		t.Fatalf("showBackingService() = %#v, %v, want %#v", detail, err, want)
	}
}
