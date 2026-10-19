package etcd

import (
	"net/http"

	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

func backupPolicyReplacementMarker(environmentID string, key string) testidempotency.IdempotencyMarker {
	marker := testDirectMarker()
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
		Method:    http.MethodPut,
		Route:     "/environments/{id}/backup-policy",
		Key:       key,
	}
	marker.Response = testidempotency.IdempotencyResponse{
		Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"status":"saved"}`),
	}
	return marker
}
