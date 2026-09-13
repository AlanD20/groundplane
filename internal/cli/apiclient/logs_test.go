package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the CLI must pass the exact public LogEvent to presentation code
// without weakening stable ids, closed variants, timestamps, or text fields.
// QA: LOG-01/02; local stream decoding only, not container log collection.
func TestStreamLogsAcceptsExactLogEvent(t *testing.T) {
	t.Parallel()

	want := apiTypes.LogEvent{
		Sequence:  1,
		ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		ContainerID: "container-1", ContainerName: "api-1",
		ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Slot:      "singleton", Stream: "stderr",
		Timestamp: time.Date(2026, time.August, 29, 12, 34, 56, 123456789, time.UTC),
		Line:      "ready", Truncated: true,
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	server := logEventServer(t, string(encoded))

	var got []apiTypes.LogEvent
	err = New(server.URL).StreamLogs(context.Background(), "/logs", nil, func(event apiTypes.LogEvent) error {
		got = append(got, event)
		return nil
	})
	if err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("StreamLogs() events = %#v, want %#v", got, []apiTypes.LogEvent{want})
	}
}

// Rationale: a stream frame is one closed JSON document; malformed syntax,
// unknown fields, or concatenated documents must never reach CLI presentation.
// QA: LOG-01/02; local stream decoding only, not container log collection.
func TestStreamLogsRejectsMalformedUnknownAndMultipleJSONDocuments(t *testing.T) {
	t.Parallel()

	valid := `{"sequence":1,"service_id":"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",` +
		`"service_name":"api","container_id":"container-1","container_name":"api-1",` +
		`"release_id":"dep_01ARZ3NDEKTSV4RRFFQ69G5FAV","slot":"blue","stream":"stdout",` +
		`"timestamp":"2026-08-29T12:34:56.123456789Z","line":"ready","truncated":false}`
	tests := []struct {
		name string
		data string
	}{
		{name: "malformed", data: `{"sequence":`},
		{name: "unknown field", data: valid[:len(valid)-1] + `,"unexpected":true}`},
		{name: "multiple documents", data: valid + " " + valid},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := logEventServer(t, test.data)
			called := false
			err := New(server.URL).StreamLogs(
				context.Background(),
				"/logs",
				nil,
				func(apiTypes.LogEvent) error {
					called = true
					return nil
				},
			)
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("StreamLogs() error = %v, want internal", err)
			}
			if called {
				t.Fatal("StreamLogs() invoked callback for rejected JSON")
			}
		})
	}
}

func logEventServer(t *testing.T, data string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Accept") != "text/event-stream" {
			t.Errorf("Accept = %q, want text/event-stream", request.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		if _, err := io.WriteString(w, "event: log\ndata: "+data+"\n\n"); err != nil {
			t.Errorf("write SSE fixture: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}
