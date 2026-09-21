package backuppolicymutations

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
)

// PreparePublication binds the prepared replacement to its exact response marker.
// The caller must Clear the returned write values after publication.
func (prepared PreparedBackupPolicyReplacement) PreparePublication(
	ctx context.Context,
	marker idempotencyrecord.IdempotencyMarker,
) (backupPolicyReplacementPlan, error) {
	if err := ValidateBackupPolicyReplacement(ctx, prepared.candidate, marker); err != nil {
		return backupPolicyReplacementPlan{}, err
	}
	return PrepareBackupPolicyReplacement(prepared.candidate)
}
