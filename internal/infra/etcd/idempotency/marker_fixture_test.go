package idempotency

import (
	sha256 "crypto/sha256"
	hex "encoding/hex"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	http "net/http"
	time "time"
)

func testDirectMarker() IdempotencyMarker {
	createdAt := testMarkerTime()
	ciphertext := []byte("protected-intent")
	digest := sha256.Sum256(ciphertext)
	return IdempotencyMarker{
		Kind: IdempotencyMarkerDirect, State: IdempotencyMarkerCompleted,
		Locator: IdempotencyLocator{
			ScopeKind: IdempotencyScopeTenant,
			ScopeID:   ids.NewAt(ids.KindTenant, createdAt, 1),
			Method:    http.MethodPatch, Route: "/projects/{id}", Key: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		Intent: ProtectedIntentRecord{
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
		},
		Response: IdempotencyResponse{
			Status: http.StatusOK, ContentKind: "application/json", Body: []byte(`{"id":"prj"}`),
		},
		CreatedAt: createdAt, UpdatedAt: createdAt, TerminalAt: createdAt,
		RetainUntil: createdAt.Add(MarkerRetention),
	}
}

func testMarkerTime() time.Time {
	return time.Date(2026, time.August, 20, 12, 0, 0, 123456789, time.UTC)
}
