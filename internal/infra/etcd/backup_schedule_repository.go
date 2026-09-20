package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backupschedule"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupDueRetention = 90 * 24 * time.Hour

// BackupScheduleCandidate is the small durable policy projection consumed by
// the single Controller scheduler pass.
type BackupScheduleCandidate struct {
	EnvironmentID  string
	Policy         backuppolicy.BackupPolicyRecord
	PolicyRevision int64
	Err            error
}

type BackupScheduleEvaluation struct {
	EnvironmentID  string
	PolicyRevision int64
	ReadRevision   int64
	ScheduledAt    time.Time
	Due            bool
	Overlap        bool
	EvaluatedAt    time.Time
}

// ListBackupScheduleCandidates lists valid policy records. Invalid records
// are deliberately ignored: a bad policy is never executable work. Disabled
// records are included so a prior coordination singleton is cleared promptly.
func (repository *BackupRuntimeRepository) ListBackupScheduleCandidates(
	ctx context.Context,
) ([]BackupScheduleCandidate, error) {
	if repository == nil || repository.store == nil {
		return nil, errs.New(errs.KindInternal, "backup runtime repository is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	var candidates []BackupScheduleCandidate
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: backuppolicy.PolicyPrefix, StartExclusive: start,
			Limit: maximumBackupRuntimeListLimit,
		})
		if err != nil {
			return nil, err
		}
		if page == nil {
			return nil, errs.New(errs.KindInternal, "backup policy schedule range is incomplete")
		}
		for _, value := range page.Values {
			environmentID := strings.TrimPrefix(value.Key, backuppolicy.PolicyPrefix)
			candidate := BackupScheduleCandidate{
				EnvironmentID: environmentID, PolicyRevision: value.ModRevision,
			}
			policy, decodeErr := backuppolicy.DecodeBackupPolicyRecord(value.Value)
			clear(value.Value)
			if ids.Validate(ids.KindEnvironment, environmentID) != nil || decodeErr != nil ||
				policy.EnvironmentID != environmentID {
				candidate.Err = corruptBackupRuntimeRecord()
				candidates = append(candidates, candidate)
				continue
			}
			candidate.Policy = policy
			if !policy.Enabled {
				continue
			}
			if _, parseErr := backupschedule.Parse(policy.Frequency); parseErr != nil {
				candidate.Err = parseErr
			}
			candidates = append(candidates, candidate)
		}
		if !page.More || len(page.Values) == 0 {
			break
		}
		start = page.Values[len(page.Values)-1].Key
	}
	return candidates, nil
}

func (repository *BackupRuntimeRepository) EvaluateBackupSchedule(
	ctx context.Context, environmentID string, now time.Time,
) (BackupScheduleEvaluation, error) {
	if repository == nil || repository.store == nil {
		return BackupScheduleEvaluation{}, errs.New(errs.KindInternal, "backup runtime repository is not configured")
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return BackupScheduleEvaluation{}, err
	}
	now = now.UTC()
	if !validBackupRuntimeInstant(now) {
		return BackupScheduleEvaluation{}, errs.New(errs.KindValidationFailed, "backup schedule clock is invalid")
	}
	keys := []string{
		backuppolicy.BackupPolicyKey(environmentID),
		environmentCoordinationKey(environmentID),
		hierarchyrecord.EnvironmentOperationLockKey(environmentID),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return BackupScheduleEvaluation{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil {
		return BackupScheduleEvaluation{}, errs.New(errs.KindStateConflict, "backup schedule is not initialized")
	}
	defer clearKeyValues(read.Values)
	policy, err := backuppolicy.DecodeBackupPolicyRecord(read.Values[0].Value)
	if err != nil || !policy.Enabled {
		return BackupScheduleEvaluation{}, errs.New(errs.KindStateConflict, "backup policy is disabled or invalid")
	}
	coord, err := decodeEnvironmentCoordinationRecord(read.Values[1].Value)
	if err != nil || coord.CurrentBackupScheduleState == nil {
		return BackupScheduleEvaluation{}, errs.New(errs.KindStateConflict, "backup schedule is not initialized")
	}
	digest, err := backupPolicyScheduleDigest(policy)
	if err != nil {
		return BackupScheduleEvaluation{}, err
	}
	state := coord.CurrentBackupScheduleState
	if state.PolicyDigest != digest || state.Frequency != policy.Frequency {
		return BackupScheduleEvaluation{}, errs.New(errs.KindStateConflict, "backup schedule policy digest changed")
	}
	if !now.After(coord.ScheduleClockFloor) {
		return BackupScheduleEvaluation{
			EnvironmentID: environmentID, PolicyRevision: read.Values[0].ModRevision,
			ReadRevision: read.ReadRevision, EvaluatedAt: now,
		}, nil
	}
	schedule, err := backupschedule.Parse(policy.Frequency)
	if err != nil {
		return BackupScheduleEvaluation{}, err
	}
	occurrence, due, err := schedule.LatestOccurrence(state.LastEvaluatedAt, now)
	if err != nil {
		return BackupScheduleEvaluation{}, err
	}
	evaluation := BackupScheduleEvaluation{
		EnvironmentID: environmentID, PolicyRevision: read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision, EvaluatedAt: now,
	}
	if !due {
		if now.After(state.LastEvaluatedAt) {
			next := *state
			next.LastEvaluatedAt, next.UpdatedAt = now, now
			coord.CurrentBackupScheduleState = &next
			if coord.ScheduleClockFloor.Before(now) {
				coord.ScheduleClockFloor = now
			}
			value, encodeErr := encodeEnvironmentCoordinationRecord(coord)
			if encodeErr != nil {
				return BackupScheduleEvaluation{}, encodeErr
			}
			defer clear(value)
			result, txErr := repository.store.Transact(
				ctx,
				[]etcdstore.Condition{
					{Key: keys[0], ModRevision: read.Values[0].ModRevision},
					{Key: keys[1], ModRevision: read.Values[1].ModRevision},
				},
				[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[1], Value: value}},
			)
			if txErr != nil {
				return BackupScheduleEvaluation{}, txErr
			}
			if !result.Succeeded {
				return BackupScheduleEvaluation{}, errs.New(errs.KindStateConflict, "backup schedule changed")
			}
		}
		return evaluation, nil
	}
	evaluation.ScheduledAt, evaluation.Due = occurrence, true
	if read.Values[2] != nil {
		evaluation.Overlap = true
	}
	return evaluation, nil
}

// SkipScheduledBackup records an overlap outcome and advances coordination in
// one CAS transaction. No Task is created for this outcome.
func (repository *BackupRuntimeRepository) SkipScheduledBackup(
	ctx context.Context, evaluation BackupScheduleEvaluation, now time.Time,
) error {
	if repository == nil || repository.store == nil {
		return errs.New(errs.KindInternal, "backup runtime repository is not configured")
	}
	if !evaluation.Overlap || evaluation.ScheduledAt.IsZero() || evaluation.ReadRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "backup overlap evaluation is invalid")
	}
	keys := []string{
		backuppolicy.BackupPolicyKey(evaluation.EnvironmentID),
		environmentCoordinationKey(evaluation.EnvironmentID),
		hierarchyrecord.EnvironmentOperationLockKey(evaluation.EnvironmentID),
	}
	dueKey, err := backupDueOutcomeKey(evaluation.EnvironmentID, evaluation.PolicyRevision, evaluation.ScheduledAt)
	if err != nil {
		return err
	}
	retention := now.UTC().Add(backupDueRetention)
	retentionKey, err := backupDueRetentionIndexKey(
		retention,
		evaluation.EnvironmentID,
		evaluation.PolicyRevision,
		evaluation.ScheduledAt,
	)
	if err != nil {
		return err
	}
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: append(keys, dueKey), Revision: evaluation.ReadRevision},
	)
	if err != nil {
		return err
	}
	if read == nil || len(read.Values) != 4 || read.ReadRevision != evaluation.ReadRevision || read.Values[1] == nil ||
		read.Values[2] == nil {
		return errs.New(errs.KindStateConflict, "backup overlap evidence changed")
	}
	defer clearKeyValues(read.Values)
	if read.Values[3] != nil {
		return nil
	}
	coord, err := decodeEnvironmentCoordinationRecord(read.Values[1].Value)
	if err != nil || coord.CurrentBackupScheduleState == nil {
		return errs.New(errs.KindStateConflict, "backup schedule changed")
	}
	state := *coord.CurrentBackupScheduleState
	if now.UTC().After(state.LastEvaluatedAt) {
		state.LastEvaluatedAt, state.UpdatedAt = now.UTC(), now.UTC()
		coord.CurrentBackupScheduleState = &state
		if coord.ScheduleClockFloor.Before(now.UTC()) {
			coord.ScheduleClockFloor = now.UTC()
		}
	}
	coordValue, err := encodeEnvironmentCoordinationRecord(coord)
	if err != nil {
		return err
	}
	defer clear(coordValue)
	dueValue, err := encodeBackupDueOutcomeRecord(BackupDueOutcomeRecord{
		EnvironmentID: evaluation.EnvironmentID, PolicyRevision: evaluation.PolicyRevision,
		ScheduledAt: evaluation.ScheduledAt.UTC(), Outcome: BackupDueSkippedOverlap,
		CreatedAt: now.UTC(), RetainUntil: retention,
	})
	if err != nil {
		return err
	}
	defer clear(dueValue)
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{
			{Key: keys[0], ModRevision: read.Values[0].ModRevision},
			{Key: keys[1], ModRevision: read.Values[1].ModRevision},
			{Key: keys[2], ModRevision: read.Values[2].ModRevision},
			{Key: dueKey},
			{Key: retentionKey},
		},
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: dueKey, Value: dueValue},
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: []byte(dueKey)},
			{Type: etcdstore.MutationPut, Key: keys[1], Value: coordValue},
		},
	)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "backup schedule changed")
	}
	return nil
}

func (repository *BackupRuntimeRepository) prepareScheduledBackupPublication(
	ctx context.Context, record BackupRunRecord, fixedRevision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if record.ScheduledAt == nil || record.Initiator != BackupRunInitiatorSchedule {
		return nil, nil, errs.New(errs.KindValidationFailed, "scheduled backup publication metadata is missing")
	}
	keys := []string{backuppolicy.BackupPolicyKey(record.EnvironmentID), environmentCoordinationKey(record.EnvironmentID)}
	dueKey, err := backupDueOutcomeKey(record.EnvironmentID, record.PolicyRevision, *record.ScheduledAt)
	if err != nil {
		return nil, nil, err
	}
	retention := record.CreatedAt.UTC().Add(backupDueRetention)
	retentionKey, err := backupDueRetentionIndexKey(
		retention,
		record.EnvironmentID,
		record.PolicyRevision,
		*record.ScheduledAt,
	)
	if err != nil {
		return nil, nil, err
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: append(keys, dueKey), Revision: fixedRevision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || len(read.Values) != 3 || read.ReadRevision != fixedRevision || read.Values[0] == nil ||
		read.Values[1] == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "backup schedule coordination is unavailable")
	}
	defer clearKeyValues(read.Values)
	if read.Values[2] != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "backup scheduled occurrence already has an outcome")
	}
	policy, err := backuppolicy.DecodeBackupPolicyRecord(read.Values[0].Value)
	if err != nil || !policy.Enabled || policy.EnvironmentID != record.EnvironmentID ||
		read.Values[0].ModRevision != record.PolicyRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "backup scheduled policy changed")
	}
	coord, err := decodeEnvironmentCoordinationRecord(read.Values[1].Value)
	if err != nil || coord.CurrentBackupScheduleState == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "backup schedule coordination is invalid")
	}
	digest, err := backupPolicyScheduleDigest(policy)
	if err != nil {
		return nil, nil, err
	}
	state := *coord.CurrentBackupScheduleState
	if state.PolicyDigest != digest || state.Frequency != policy.Frequency ||
		state.LastEvaluatedAt.After(record.CreatedAt.UTC()) {
		return nil, nil, errs.New(errs.KindStateConflict, "backup schedule digest or clock changed")
	}
	state.LastEvaluatedAt, state.UpdatedAt = record.CreatedAt.UTC(), record.CreatedAt.UTC()
	coord.CurrentBackupScheduleState = &state
	if coord.ScheduleClockFloor.Before(record.CreatedAt.UTC()) {
		coord.ScheduleClockFloor = record.CreatedAt.UTC()
	}
	coordValue, err := encodeEnvironmentCoordinationRecord(coord)
	if err != nil {
		return nil, nil, err
	}
	dueValue, err := encodeBackupDueOutcomeRecord(BackupDueOutcomeRecord{
		EnvironmentID: record.EnvironmentID, PolicyRevision: record.PolicyRevision,
		ScheduledAt: record.ScheduledAt.UTC(), Outcome: BackupDueDispatched,
		TaskID: record.TaskID, CreatedAt: record.CreatedAt.UTC(), RetainUntil: retention,
	})
	if err != nil {
		clear(coordValue)
		return nil, nil, err
	}
	return []etcdstore.Condition{
		{Key: keys[1], ModRevision: read.Values[1].ModRevision},
		{Key: dueKey},
		{Key: retentionKey},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: dueKey, Value: dueValue},
		{Type: etcdstore.MutationPut, Key: retentionKey, Value: []byte(dueKey)},
		{Type: etcdstore.MutationPut, Key: keys[1], Value: coordValue},
	}, nil
}

func (repository *BackupRuntimeRepository) exactScheduledBackupRunSubordinates(
	ctx context.Context, run BackupRunRecord, readRevision, commitRevision int64,
) bool {
	if run.ScheduledAt == nil {
		return false
	}
	dueKey, err := backupDueOutcomeKey(run.EnvironmentID, run.PolicyRevision, *run.ScheduledAt)
	if err != nil {
		return false
	}
	retainUntil := run.CreatedAt.UTC().Add(backupDueRetention)
	retentionKey, err := backupDueRetentionIndexKey(
		retainUntil,
		run.EnvironmentID,
		run.PolicyRevision,
		*run.ScheduledAt,
	)
	if err != nil {
		return false
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentCoordinationKey(run.EnvironmentID), dueKey, retentionKey}, Revision: readRevision,
	})
	if err != nil || read == nil || read.ReadRevision != readRevision || len(read.Values) != 3 {
		return false
	}
	defer clearKeyValues(read.Values)
	for _, value := range read.Values {
		if value == nil || value.ModRevision != commitRevision {
			return false
		}
	}
	coord, err := decodeEnvironmentCoordinationRecord(read.Values[0].Value)
	if err != nil || coord.CurrentBackupScheduleState == nil ||
		!coord.CurrentBackupScheduleState.LastEvaluatedAt.Equal(run.CreatedAt) {
		return false
	}
	due, err := decodeBackupDueOutcomeRecord(read.Values[1].Value)
	if err != nil || due.EnvironmentID != run.EnvironmentID || due.PolicyRevision != run.PolicyRevision ||
		!due.ScheduledAt.Equal(*run.ScheduledAt) || due.Outcome != BackupDueDispatched || due.TaskID != run.TaskID ||
		!due.CreatedAt.Equal(run.CreatedAt) || !due.RetainUntil.Equal(retainUntil) {
		return false
	}
	return string(read.Values[2].Value) == dueKey
}
