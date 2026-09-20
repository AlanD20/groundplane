package idempotency

import idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

// CloneResponse gives the caller ownership of replay bytes before retained
// evidence is cleared by the operation that produced them.
func CloneResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
