package apiclient

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the sole raw response boundary must reject an oversized private
// identity, clear its temporary allocation, and never add an idempotency key.
func TestClientBoundsRawExportAndOmitsIdempotency(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if got := request.Header.Get(idempotencyKeyHeader); got != "" {
			t.Errorf("Idempotency-Key = %q, want empty", got)
		}
		if got := request.Header.Get("Accept"); got != "text/plain" {
			t.Errorf("Accept = %q, want text/plain", got)
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write(bytes.Repeat([]byte("x"), maximumRawResponseByteSize+1))
	}))
	defer server.Close()
	client := New(server.URL)
	request := client.NewRequest(
		http.MethodPost,
		"/api/v1/environments/env_01J00000000000000000000000/export-key",
		nil,
		nil,
		http.StatusOK,
	)
	var body []byte
	if err := client.Do(context.Background(), request, &body); err == nil {
		t.Fatal("Do() error = nil, want oversized raw response error")
	}
	if len(body) != 0 {
		t.Fatalf("body retained %d bytes", len(body))
	}
}

type partialPrivateReader struct {
	failure error
	read    bool
}

func (reader *partialPrivateReader) Read(target []byte) (int, error) {
	if reader.read {
		return 0, reader.failure
	}
	reader.read = true
	return copy(target, []byte("AGE-SECRET-KEY-1PARTIAL")), reader.failure
}

// Rationale: a transport that returns private identity bytes together with an
// error must leave no partial secret in the raw-response scratch buffer and
// must expose only the repository's single errs.Error type.
func TestReadBoundedRawResponseClearsPartialPrivateIdentityOnError(t *testing.T) {
	failure := errors.New("truncated response")
	scratch := bytes.Repeat([]byte{0x7f}, maximumRawResponseByteSize+1)
	_, err := readBoundedRawResponseInto(
		&partialPrivateReader{failure: failure},
		scratch,
		http.MethodPost,
		"/api/v1/environments/env_01J00000000000000000000000/export-key",
	)
	var domainError *errs.Error
	if !errors.Is(err, failure) || !errors.As(err, &domainError) {
		t.Fatalf("read error = %v", err)
	}
	if !bytes.Equal(scratch, make([]byte, len(scratch))) {
		t.Fatal("partial private response scratch was not cleared")
	}
}
