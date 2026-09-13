package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the human client must preserve both absent metadata and exact
// digest, compatibility and durable Task fields through the generated model.
// QA: UP-01/05; update metadata projection only, not activation/recovery.
func TestShowHostPreservesControllerUpdateMetadata(t *testing.T) {
	t.Parallel()
	for _, update := range []apiTypes.ControllerUpdateState{
		{RunningSHA256: "running", Error: "unavailable"},
		{RunningSHA256: "running", Available: true,
			Candidate: &apiTypes.ControllerRelease{Release: "release", ControllerSHA256: "binary",
				ControllerVersion: "version", AgentImage: "image", StorageEpoch: 1, ChannelSchema: 2},
			LastUpdate: &apiTypes.ControllerUpdateSummary{TaskID: "task_update", Release: "release",
				Status: "failed", Phase: "recovered", CreatedAt: time.Date(2026, 9, 10, 1, 2, 3, 0, time.UTC)}},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(writer).Encode(apiTypes.Host{Controller: apiTypes.HostController{Update: update}})
		}))
		got, err := New(server.URL).ShowHost(context.Background())
		server.Close()
		if err != nil || !reflect.DeepEqual(got.Controller.Update, update) {
			t.Fatalf("update = %#v, want %#v, error = %v", got.Controller.Update, update, err)
		}
	}
}
