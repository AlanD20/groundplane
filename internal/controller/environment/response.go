package environment

import "github.com/AlanD20/groundplane/internal/infra/etcd"

func cloneIdempotencyResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	cloned := response
	cloned.Body = append([]byte(nil), response.Body...)
	return cloned
}
