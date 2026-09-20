package etcd

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func sealBackupPolicyCandidateSchedule(
	candidate *backupPolicyReplacementCandidate,
	now time.Time,
) error {
	if candidate == nil || !validEnvironmentCoordinationInstant(now.UTC()) {
		return errs.New(errs.KindValidationFailed, "backup policy schedule boundary is invalid")
	}
	candidate.Replacement.UpdatedAt = now.UTC()
	next, nextRunAt, err := replaceEnvironmentCoordinationSchedule(
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

func backupPolicyNextRunAt(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	value = value.UTC()
	return &value
}
