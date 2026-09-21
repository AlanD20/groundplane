package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testrecordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	testroutes "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
)

func TestRouteProviderPageRequestPinsInitialAndContinuedRevision(t *testing.T) {
	request := testkeyvalue.PageRequest{Limit: 20, Revision: 41}
	_, revision, _, query, err := testrecordquery.NormalizePageRequest(
		request, "routes", "environment", ids.New(ids.KindEnvironment), testroutes.RecordPrefix, ids.KindRoute,
	)
	if err != nil || revision != request.Revision {
		t.Fatalf("normalizePageRequest(initial) revision/error = %d/%v", revision, err)
	}
	cursor, err := testrecordcodec.EncodeCursor(testrecordcodec.Cursor{
		Version: testrecordcodec.CursorVersion, Revision: request.Revision,
		LastID: ids.New(ids.KindRoute), Query: query,
	})
	if err != nil {
		t.Fatal(err)
	}
	request.Cursor = cursor
	_, _, _, _, err = testrecordquery.NormalizePageRequest(
		request, "routes", "environment", ids.New(ids.KindEnvironment), testroutes.RecordPrefix, ids.KindRoute,
	)
	if err == nil {
		t.Fatal("normalizePageRequest accepted a cursor built for a different Environment query")
	}
	environmentID := ids.New(ids.KindEnvironment)
	_, _, _, query, err = testrecordquery.NormalizePageRequest(
		testkeyvalue.PageRequest{Limit: 20, Revision: 41},
		"routes",
		"environment",
		environmentID,
		testroutes.RecordPrefix,
		ids.KindRoute,
	)
	if err != nil {
		t.Fatal(err)
	}
	cursor, err = testrecordcodec.EncodeCursor(testrecordcodec.Cursor{
		Version: testrecordcodec.CursorVersion, Revision: 41, LastID: ids.New(ids.KindRoute), Query: query,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, revision, _, _, err = testrecordquery.NormalizePageRequest(
		testkeyvalue.PageRequest{Limit: 20, Cursor: cursor, Revision: 41},
		"routes",
		"environment",
		environmentID,
		testroutes.RecordPrefix,
		ids.KindRoute,
	)
	if err != nil || revision != 41 {
		t.Fatalf("normalizePageRequest(continued) revision/error = %d/%v", revision, err)
	}
	_, _, _, _, err = testrecordquery.NormalizePageRequest(
		testkeyvalue.PageRequest{Limit: 20, Cursor: cursor, Revision: 42},
		"routes",
		"environment",
		environmentID,
		testroutes.RecordPrefix,
		ids.KindRoute,
	)
	if err == nil {
		t.Fatal("normalizePageRequest accepted a cursor at a different fixed revision")
	}
}
