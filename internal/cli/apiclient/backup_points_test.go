package apiclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: the human CLI client must use the generated BackupPointsList
// operation and preserve its cursor plus exact verified public projection.
func TestListRecoveryPointsUsesGeneratedOperation(t *testing.T) {
	t.Parallel()
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet ||
			request.URL.Path != "/api/v1/environments/"+environmentID+"/recovery-points" ||
			request.URL.Query().Get("cursor") != "opaque-current" || len(request.URL.Query()) != 1 {
			t.Errorf("request = %s %s", request.Method, request.URL.RequestURI())
		}
		writer.Header().Set("Content-Type", "application/json")
		_, err := io.WriteString(writer, `{"items":[{"id":"rp_01ARZ3NDEKTSV4RRFFQ69G5FAV",`+
			`"source_id":"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV","source_kind":"volume",`+
			`"target_id":"vol_01ARZ3NDEKTSV4RRFFQ69G5FAV","created_at":"2026-08-27T12:00:00Z",`+
			`"size_bytes":123,"encrypted":true,"key_era":3,"status":"verified"}],`+
			`"next_cursor":"opaque-next"}`)
		if err != nil {
			t.Errorf("write response: %v", err)
		}
	}))
	defer server.Close()

	page, err := New(server.URL).ListRecoveryPoints(
		context.Background(), environmentID, "opaque-current",
	)
	if err != nil {
		t.Fatalf("list Recovery Points: %v", err)
	}
	if len(page.Items) != 1 || page.NextCursor != "opaque-next" {
		t.Fatalf("page = %#v", page)
	}
	point := page.Items[0]
	if point.ID != "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		point.SourceID != "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		point.SourceKind != apiTypes.BackupSourceVolume ||
		point.TargetID != "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV" ||
		point.CreatedAt != "2026-08-27T12:00:00Z" || point.SizeBytes != 123 ||
		!point.Encrypted || point.KeyEra != 3 || point.Status != apiTypes.RecoveryPointVerified {
		t.Fatalf("recovery point = %#v", point)
	}
}
