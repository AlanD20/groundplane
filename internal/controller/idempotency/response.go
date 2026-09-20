package idempotency

import "github.com/AlanD20/groundplane/internal/infra/etcd"

// CloneResponse gives the caller ownership of replay bytes before retained
// evidence is cleared by the operation that produced them.
func CloneResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
