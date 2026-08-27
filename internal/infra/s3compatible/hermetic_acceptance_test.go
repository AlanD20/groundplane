package s3compatible

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Connector CRUD performs no probe, while the execution adapter
// must independently prove signed conditional Put, exact Head/Get metadata,
// immutable conflict, and conditional Delete against a hermetic endpoint.
func TestC15HermeticS3AdapterConformance(t *testing.T) {
	body := []byte("c15 hermetic connector artifact")
	artifact := testArtifact(body, backupobject.EncryptionNone)
	etag := `"c15-hermetic-etag"`
	var mutex sync.Mutex
	stored := false
	storedBody := []byte(nil)
	requests := 0
	server := newIPv4Server(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		requests++
		if request.Header.Get("Authorization") == "" {
			t.Errorf("%s request omitted AWS authorization", request.Method)
		}
		for key, value := range artifact.Metadata() {
			if request.Method == http.MethodPut && request.Header.Get("X-Amz-Meta-"+key) != value {
				t.Errorf("PutObject metadata %s = %q, want %q", key, request.Header.Get("X-Amz-Meta-"+key), value)
			}
		}
		switch request.Method {
		case http.MethodPut:
			if request.Header.Get("If-None-Match") != "*" {
				t.Errorf("PutObject If-None-Match = %q", request.Header.Get("If-None-Match"))
			}
			if stored {
				writeProviderError(response, http.StatusPreconditionFailed, "PreconditionFailed")
				return
			}
			value, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read PutObject body: %v", err)
			}
			storedBody = append([]byte(nil), value...)
			stored = true
			response.Header().Set("ETag", etag)
		case http.MethodHead:
			if !stored {
				response.WriteHeader(http.StatusNotFound)
				return
			}
			writeObjectHeaders(response, artifact, etag)
		case http.MethodGet:
			if !stored {
				response.WriteHeader(http.StatusNotFound)
				return
			}
			writeObjectHeaders(response, artifact, etag)
			_, _ = response.Write(storedBody)
		case http.MethodDelete:
			if request.Header.Get("If-Match") != etag {
				t.Errorf("DeleteObject If-Match = %q, want %q", request.Header.Get("If-Match"), etag)
			}
			stored = false
			clear(storedBody)
			storedBody = nil
			response.WriteHeader(http.StatusNoContent)
		default:
			response.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()
	adapter := newWireAdapter(t, server, true)
	if requests != 0 {
		t.Fatalf("adapter construction performed %d endpoint requests", requests)
	}

	object, err := adapter.PutExact(context.Background(), artifact, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("PutExact() error = %v", err)
	}
	head, err := adapter.HeadExact(context.Background(), artifact, &object.Discriminator)
	if err != nil || !head.Present || head.Object != object {
		t.Fatalf("HeadExact() = %#v, %v", head, err)
	}
	var restored bytes.Buffer
	if err := adapter.GetExact(context.Background(), object, &restored); err != nil ||
		!bytes.Equal(restored.Bytes(), body) {
		t.Fatalf("GetExact() body/error = %q/%v", restored.Bytes(), err)
	}
	if _, err := adapter.PutExact(
		context.Background(),
		artifact,
		bytes.NewReader(body),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("PutExact(existing) error = %v", err)
	}
	if err := adapter.DeleteExact(context.Background(), object); err != nil {
		t.Fatalf("DeleteExact() error = %v", err)
	}
	absent, err := adapter.HeadExact(context.Background(), artifact, &object.Discriminator)
	if err != nil || absent.Present {
		t.Fatalf("HeadExact(after delete) = %#v, %v", absent, err)
	}
	if requests < 8 {
		t.Fatalf("hermetic lifecycle request count = %d, want complete lifecycle", requests)
	}
}

func isKind(err error, want errs.Kind) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == want
}
