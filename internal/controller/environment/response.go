package environment

import idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

func cloneIdempotencyResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	cloned := response
	cloned.Body = append([]byte(nil), response.Body...)
	return cloned
}
