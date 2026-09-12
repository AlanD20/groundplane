package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestRouteProviderPageRequestPinsInitialAndContinuedRevision(t *testing.T) {
	request := PageRequest{Limit: 20, Revision: 41}
	_, revision, _, query, err := normalizePageRequest(
		request, "routes", "environment", ids.New(ids.KindEnvironment), routePrefix, ids.KindRoute,
	)
	if err != nil || revision != request.Revision {
		t.Fatalf("normalizePageRequest(initial) revision/error = %d/%v", revision, err)
	}
	cursor, err := encodeCursor(cursorPayload{
		Version: cursorVersion, Revision: request.Revision,
		LastID: ids.New(ids.KindRoute), Query: query,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = cursor
	_, _, _, _, err = normalizePageRequest(
		request, "routes", "environment", ids.New(ids.KindEnvironment), routePrefix, ids.KindRoute,
	)
	if err == nil {
		t.Fatal("normalizePageRequest accepted a cursor built for a different Environment query")
	}
	environmentID := ids.New(ids.KindEnvironment)
	_, _, _, query, err = normalizePageRequest(
		PageRequest{Limit: 20, Revision: 41}, "routes", "environment", environmentID, routePrefix, ids.KindRoute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = encodeCursor(cursorPayload{
		Version: cursorVersion, Revision: 41, LastID: ids.New(ids.KindRoute), Query: query,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, revision, _, _, err = normalizePageRequest(
		PageRequest{Limit: 20, Cursor: cursor, Revision: 41},
		"routes", "environment", environmentID, routePrefix, ids.KindRoute,
	)
	if err != nil || revision != 41 {
		t.Fatalf("normalizePageRequest(continued) revision/error = %d/%v", revision, err)
	}
	_, _, _, _, err = normalizePageRequest(
		PageRequest{Limit: 20, Cursor: cursor, Revision: 42},
		"routes", "environment", environmentID, routePrefix, ids.KindRoute,
	)
	if err == nil {
		t.Fatal("normalizePageRequest accepted a cursor at a different fixed revision")
	}
}
