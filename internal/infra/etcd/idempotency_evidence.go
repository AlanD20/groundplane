package etcd

import idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

// IdempotencyEvidence is minted by repository reads, not from caller-supplied records.
// Its private marker and revision remain together with replay and pruning authority.
type IdempotencyEvidence struct {
	marker      idempotencyrecord.IdempotencyMarker
	modRevision int64
}
