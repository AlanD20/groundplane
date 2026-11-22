package operations

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"net/http"
)

func bulkCanonicalIntent(input entryBulkUpsertInput) requestidempotency.CanonicalIntentV1 {
	items := make([]requestidempotency.Value, len(input.entries))
	for index, entry := range input.entries {
		digest := sha256.Sum256([]byte(entry.value))
		items[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(entry.key)},
			requestidempotency.Field{
				Name:  "value_sha256",
				Value: requestidempotency.String(hex.EncodeToString(digest[:])),
			},
		)
	}
	fields := []requestidempotency.Field{
		{Name: "environment_id", Value: requestidempotency.String(input.environmentID)},
		{Name: "entries", Value: requestidempotency.List(items...)},
		{Name: "exposure", Value: canonicalEntryExposure(input.exposure)},
		{Name: "secret", Value: requestidempotency.Bool(input.secret)},
		{Name: "after", Value: acceptedaction.AfterValue(input.after)},
	}
	if input.encryption != "" {
		fields = append(fields, requestidempotency.Field{
			Name: "encryption", Value: requestidempotency.String(input.encryption),
		})
	}
	return requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  entryBulkUpsertRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: input.environmentID},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.JSONBody(requestidempotency.Object(fields...)),
	}
}
