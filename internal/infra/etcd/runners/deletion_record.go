package runners

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) error {
	if record.TargetKind != deletionrecord.DeletionTargetRunner ||
		ids.Validate(ids.KindRunner, record.TargetID) != nil ||
		record.TargetRevision <= 0 ||
		ids.Validate(ids.KindTask, record.TaskID) != nil ||
		record.Phase != deletionrecord.DeletionPhaseFinalizing ||
		record.Checkpoint != (deletionrecord.DeletionCheckpoint{}) ||
		!recordcodec.IsCanonicalUTC(record.CreatedAt) ||
		!recordcodec.IsCanonicalUTC(record.UpdatedAt) ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "runner deletion tombstone is invalid")
	}
	return nil
}

func EncodeRunnerDeletionTombstone(record deletionrecord.DeletionTombstoneRecord) ([]byte, error) {
	if err := ValidateRunnerDeletionTombstone(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("deletion-tombstone", record)
}

func DecodeRunnerDeletionTombstone(value []byte) (deletionrecord.DeletionTombstoneRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(
			errs.KindInternal,
			"runner deletion tombstone is corrupt",
		)
	}
	record, err := recordcodec.Decode[deletionrecord.DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil || ValidateRunnerDeletionTombstone(record) != nil {
		return deletionrecord.DeletionTombstoneRecord{}, errs.New(
			errs.KindInternal,
			"runner deletion tombstone is corrupt",
		)
	}
	return record, nil
}
