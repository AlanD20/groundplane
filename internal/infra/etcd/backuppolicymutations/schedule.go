package backuppolicymutations

import (
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func SealBackupPolicyCandidateSchedule(
	candidate *ReplacementCandidate,
	now time.Time,
) error {
	if candidate == nil || !coordinationrecord.ValidInstant(now.UTC()) {
		return errs.New(errs.KindValidationFailed, "backup policy schedule boundary is invalid")
	}
	candidate.Replacement.UpdatedAt = now.UTC()
	next, nextRunAt, err := coordinationrecord.ReplaceSchedule(
		candidate.Coordination.Record, candidate.Replacement, now.UTC(),
	)
	if err != nil {
		return err
	}
	candidate.NextCoordination = next
	candidate.NextRunAt = nextRunAt
	candidate.ScheduleSealed = true
	return nil
}

func BackupPolicyNextRunAt(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}
