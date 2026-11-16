package acceptedaction

import requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"

// AfterValue participates in each action's protected request fingerprint.
// Reusing a request key with different predecessor Tasks is a content mismatch.
func AfterValue(after []string) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(after))
	for index, id := range after {
		values[index] = requestidempotency.String(id)
	}
	return requestidempotency.List(values...)
}
