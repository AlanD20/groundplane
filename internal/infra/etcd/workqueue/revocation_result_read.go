package workqueue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

func (repository *Repository) validateRevocationResult(ctx context.Context, task TaskSnapshot,
	snapshot ItemSnapshot, revision int64,
) error {
	item := snapshot.Item
	if item.Result == nil || item.Result.ID != revocationResultID(item) {
		return nil // Operation-owned completion evidence is validated by its owner.
	}
	read, err := repository.read(ctx, []string{revocationResultKey(item.ID)}, revision)
	if err != nil {
		return err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || len(value.Value) > 4096 || item.Input == nil || !item.ForwardRevoked ||
		item.Phase != workexecution.Settled || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone || item.CleanupResult != nil || item.FirstFailure != nil ||
		item.Outcome != workexecution.Cancelled && item.Outcome != workexecution.Superseded {
		return corrupt()
	}
	proof, err := recordcodec.Decode[revocationResult](value.Value, "execution_revocation_result")
	if err != nil || proof.TaskID != task.Task.ID || proof.WorkID != item.ID || proof.Input != *item.Input ||
		proof.Outcome != item.Outcome || !recordcodec.IsCanonicalUTC(proof.RevokedAt) ||
		!recordcodec.IsCanonicalUTC(proof.RecordedAt) || proof.RevokedAt.Before(item.CreatedAt) ||
		proof.RecordedAt.Before(proof.RevokedAt) || !proof.RecordedAt.Equal(item.UpdatedAt) {
		return corrupt()
	}
	digest := sha256.Sum256(value.Value)
	if hex.EncodeToString(digest[:]) != item.Result.SHA256 {
		return corrupt()
	}
	attempts, err := repository.store.Range(ctx, keyvalue.RangeRequest{
		Prefix: AttemptIndexPrefix(item.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return err
	}
	if attempts == nil || attempts.ReadRevision != revision {
		return corrupt()
	}
	defer keyvalue.ClearRangeValues(attempts.Values)
	if len(attempts.Values) != 0 || attempts.More {
		return corrupt()
	}
	return nil
}
