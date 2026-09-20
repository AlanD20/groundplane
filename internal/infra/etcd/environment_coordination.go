package etcd

import (
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupschedule"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentCoordinationPrefix             = "/v1/runtime/environment-coordination/"
	maximumEnvironmentCoordinationRecordBytes = 4 * 1024
)

// EnvironmentCoordinationRecord is the Environment's singleton mutation and
// scheduling authority. Its etcd modification revision remains the mutation
// fence; scheduling state is embedded so ordinary mutations can preserve it.
type EnvironmentCoordinationRecord struct {
	EnvironmentID              string                      `json:"environment_id"`
	ScheduleClockFloor         time.Time                   `json:"schedule_clock_floor"`
	CurrentBackupScheduleState *CurrentBackupScheduleState `json:"current_backup_schedule_state,omitempty"`
}

// CurrentBackupScheduleState binds scheduling progress to one exact durable
// Backup Policy value without creating a policy-revision cursor collection.
type CurrentBackupScheduleState struct {
	PolicyDigest    string    `json:"policy_digest"`
	Frequency       string    `json:"frequency"`
	EnabledAt       time.Time `json:"enabled_at"`
	LastEvaluatedAt time.Time `json:"last_evaluated_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// environmentCoordinationEvidence retains the exact read bytes. Ordinary
// resource mutations rewrite those bytes verbatim and change only ModRevision.
type environmentCoordinationEvidence struct {
	Record EnvironmentCoordinationRecord
	Value  []byte
}

func environmentCoordinationKey(environmentID string) string {
	return environmentCoordinationPrefix + environmentID
}

func validateEnvironmentCoordinationRecord(record EnvironmentCoordinationRecord) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		!validEnvironmentCoordinationInstant(record.ScheduleClockFloor) {
		return errs.New(errs.KindValidationFailed, "environment coordination is invalid")
	}
	state := record.CurrentBackupScheduleState
	if state == nil {
		return nil
	}
	if !validEnvironmentCoordinationDigest(state.PolicyDigest) ||
		!validEnvironmentCoordinationFrequency(state.Frequency) ||
		!validEnvironmentCoordinationInstant(state.EnabledAt) ||
		!validEnvironmentCoordinationInstant(state.LastEvaluatedAt) ||
		!validEnvironmentCoordinationInstant(state.UpdatedAt) ||
		state.LastEvaluatedAt.Before(state.EnabledAt) ||
		state.UpdatedAt.Before(state.LastEvaluatedAt) ||
		record.ScheduleClockFloor.Before(state.UpdatedAt) {
		return errs.New(errs.KindValidationFailed, "environment coordination schedule is invalid")
	}
	return nil
}

func encodeEnvironmentCoordinationRecord(record EnvironmentCoordinationRecord) ([]byte, error) {
	if err := validateEnvironmentCoordinationRecord(record); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("environment-coordination", record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumEnvironmentCoordinationRecordBytes {
		clear(value)
		return nil, errs.New(errs.KindValidationFailed, "environment coordination exceeds 4 KiB")
	}
	return value, nil
}

func decodeEnvironmentCoordinationRecord(value []byte) (EnvironmentCoordinationRecord, error) {
	if len(value) == 0 || len(value) > maximumEnvironmentCoordinationRecordBytes {
		return EnvironmentCoordinationRecord{}, corruptEnvironmentCoordination()
	}
	record, err := recordcodec.Decode[EnvironmentCoordinationRecord](value, "environment-coordination")
	if err != nil || validateEnvironmentCoordinationRecord(record) != nil {
		return EnvironmentCoordinationRecord{}, corruptEnvironmentCoordination()
	}
	return record, nil
}

func (evidence environmentCoordinationEvidence) rewriteMutation() (etcdstore.Mutation, error) {
	decoded, err := decodeEnvironmentCoordinationRecord(evidence.Value)
	if err != nil || !equalEnvironmentCoordinationRecord(decoded, evidence.Record) {
		return etcdstore.Mutation{}, corruptEnvironmentCoordination()
	}
	return etcdstore.Mutation{
		Type:  etcdstore.MutationPut,
		Key:   environmentCoordinationKey(evidence.Record.EnvironmentID),
		Value: append([]byte(nil), evidence.Value...),
	}, nil
}

func equalEnvironmentCoordinationRecord(left, right EnvironmentCoordinationRecord) bool {
	if left.EnvironmentID != right.EnvironmentID ||
		!left.ScheduleClockFloor.Equal(right.ScheduleClockFloor) ||
		(left.CurrentBackupScheduleState == nil) != (right.CurrentBackupScheduleState == nil) {
		return false
	}
	if left.CurrentBackupScheduleState == nil {
		return true
	}
	leftState := left.CurrentBackupScheduleState
	rightState := right.CurrentBackupScheduleState
	return leftState.PolicyDigest == rightState.PolicyDigest &&
		leftState.Frequency == rightState.Frequency &&
		leftState.EnabledAt.Equal(rightState.EnabledAt) &&
		leftState.LastEvaluatedAt.Equal(rightState.LastEvaluatedAt) &&
		leftState.UpdatedAt.Equal(rightState.UpdatedAt)
}

func backupPolicyScheduleDigest(policy BackupPolicyRecord) (string, error) {
	if err := validateBackupPolicyRecord(policy); err != nil {
		return "", err
	}
	if !validEnvironmentCoordinationInstant(policy.UpdatedAt) {
		return "", errs.New(errs.KindValidationFailed, "backup policy schedule digest time is invalid")
	}
	if policy.Frequency != "" && !validEnvironmentCoordinationFrequency(policy.Frequency) {
		return "", errs.New(errs.KindValidationFailed, "backup policy schedule digest frequency is invalid")
	}
	// encodeBackupPolicyRecord is the sole canonical durable policy encoding;
	// hashing that value binds schedule state to every persisted policy field.
	value, err := encodeBackupPolicyRecord(policy)
	if err != nil {
		return "", err
	}
	defer clear(value)
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

// replaceEnvironmentCoordinationSchedule applies the complete policy
// replacement transition at one monotonic logical boundary. A disabled policy
// removes schedule state while retaining the boundary. Enable and frequency
// changes seed a new cadence; same-frequency replacement carries EnabledAt.
func replaceEnvironmentCoordinationSchedule(
	current EnvironmentCoordinationRecord,
	replacement BackupPolicyRecord,
	now time.Time,
) (EnvironmentCoordinationRecord, time.Time, error) {
	if err := validateEnvironmentCoordinationRecord(current); err != nil {
		return EnvironmentCoordinationRecord{}, time.Time{}, err
	}
	if err := validateBackupPolicyRecord(replacement); err != nil {
		return EnvironmentCoordinationRecord{}, time.Time{}, err
	}
	if replacement.EnvironmentID != current.EnvironmentID || !validEnvironmentCoordinationInstant(now.UTC()) {
		return EnvironmentCoordinationRecord{}, time.Time{}, errs.New(
			errs.KindValidationFailed,
			"environment coordination replacement boundary is invalid",
		)
	}
	var schedule backupschedule.Schedule
	if replacement.Frequency != "" {
		var err error
		schedule, err = backupschedule.Parse(replacement.Frequency)
		if err != nil {
			return EnvironmentCoordinationRecord{}, time.Time{}, err
		}
	}
	boundary := now.UTC()
	if current.ScheduleClockFloor.After(boundary) {
		boundary = current.ScheduleClockFloor
	}
	if current.CurrentBackupScheduleState != nil &&
		current.CurrentBackupScheduleState.LastEvaluatedAt.After(boundary) {
		boundary = current.CurrentBackupScheduleState.LastEvaluatedAt
	}
	next := EnvironmentCoordinationRecord{
		EnvironmentID:      current.EnvironmentID,
		ScheduleClockFloor: boundary,
	}
	if !replacement.Enabled {
		return next, time.Time{}, nil
	}
	digest, err := backupPolicyScheduleDigest(replacement)
	if err != nil {
		return EnvironmentCoordinationRecord{}, time.Time{}, err
	}
	enabledAt := boundary
	if current.CurrentBackupScheduleState != nil &&
		current.CurrentBackupScheduleState.Frequency == replacement.Frequency {
		enabledAt = current.CurrentBackupScheduleState.EnabledAt
	}
	next.CurrentBackupScheduleState = &CurrentBackupScheduleState{
		PolicyDigest:    digest,
		Frequency:       replacement.Frequency,
		EnabledAt:       enabledAt,
		LastEvaluatedAt: boundary,
		UpdatedAt:       boundary,
	}
	nextRunAt, err := schedule.NextOccurrence(boundary)
	if err != nil {
		return EnvironmentCoordinationRecord{}, time.Time{}, err
	}
	return next, nextRunAt, nil
}

func validEnvironmentCoordinationDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == value
}

func validEnvironmentCoordinationFrequency(value string) bool {
	_, err := backupschedule.Parse(value)
	return err == nil
}

func validEnvironmentCoordinationInstant(value time.Time) bool {
	if value.IsZero() || value.Location() != time.UTC {
		return false
	}
	nanoseconds := value.UnixNano()
	return nanoseconds > 0 && time.Unix(0, nanoseconds).UTC().Equal(value)
}

func corruptEnvironmentCoordination() error {
	return errs.New(errs.KindInternal, "environment coordination is corrupt")
}
