package handlers

import (
	"context"
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testbackingservices "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type fakeBackingServiceReader struct {
	detail      testkeyvalue.Versioned[testbackingservices.Record]
	page        testkeyvalue.Page[testbackingservices.Record]
	wantRequest testkeyvalue.PageRequest
	listed      bool
}

func (fake *fakeBackingServiceReader) GetBackingService(
	_ context.Context,
	projectID string,
) (testkeyvalue.Versioned[testbackingservices.Record], error) {
	if projectID != fake.detail.Record.ProjectID {
		return testkeyvalue.Versioned[testbackingservices.Record]{}, nil
	}
	return fake.detail, nil
}

func (fake *fakeBackingServiceReader) ListBackingServices(
	_ context.Context,
	request testkeyvalue.PageRequest,
) (testkeyvalue.Page[testbackingservices.Record], error) {
	fake.listed = request == fake.wantRequest
	return fake.page, nil
}

// Rationale: the REST facade must expose exactly the accepted Project, main Environment, and
// adapter Service ids while preserving the repository's cursor unchanged.
func TestBackingServiceRoutesProjectExactFacade(t *testing.T) {
	t.Parallel()
	record := testbackingservices.Record{
		Authentication: core.BackingAuthenticationPassword,
		ProjectID:      ids.New(ids.KindProject), EnvironmentID: ids.New(ids.KindEnvironment),
		ServiceID: ids.New(ids.KindService), BackingNetworkID: ids.New(ids.KindNetwork),
	}
	request := testkeyvalue.PageRequest{Limit: 3, Cursor: "opaque"}
	reader := &fakeBackingServiceReader{
		detail: testkeyvalue.Versioned[testbackingservices.Record]{Record: record},
		page: testkeyvalue.Page[testbackingservices.Record]{
			Items:      []testkeyvalue.Versioned[testbackingservices.Record]{{Record: record}},
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
	if detail.Body.Authentication != "password" {
		t.Fatalf("backing-service authentication = %q", detail.Body.Authentication)
	}
}
