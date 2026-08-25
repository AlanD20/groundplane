package etcd

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const coordinationTestEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// Rationale: the Environment singleton is the only scheduling authority and must
// round-trip its complete bounded state without a policy-revision collection.
func TestEnvironmentCoordinationRecordRoundTripsCompleteSchedule(t *testing.T) {
	t.Parallel()
	enabledAt := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	lastEvaluatedAt := enabledAt.Add(2 * time.Hour)
	record := EnvironmentCoordinationRecord{
		EnvironmentID:      coordinationTestEnvironmentID,
		ScheduleClockFloor: lastEvaluatedAt,
		CurrentBackupScheduleState: &CurrentBackupScheduleState{
			PolicyDigest:    "26c0b60b0baf19530342375ef75108ff8feefb8b8f9c231a8595b4cad436de60",
			Frequency:       "*-*-* 03:15:00",
			EnabledAt:       enabledAt,
			LastEvaluatedAt: lastEvaluatedAt,
			UpdatedAt:       lastEvaluatedAt,
		},
	}

	encoded, err := encodeEnvironmentCoordinationRecord(record)
	if err != nil {
		t.Fatalf("encodeEnvironmentCoordinationRecord() error = %v", err)
	}
	decoded, err := decodeEnvironmentCoordinationRecord(encoded)
	if err != nil {
		t.Fatalf("decodeEnvironmentCoordinationRecord() error = %v", err)
	}
	if decoded.EnvironmentID != record.EnvironmentID ||
		!decoded.ScheduleClockFloor.Equal(record.ScheduleClockFloor) ||
		decoded.CurrentBackupScheduleState == nil ||
		*decoded.CurrentBackupScheduleState != *record.CurrentBackupScheduleState {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}
	if len(encoded) > maximumEnvironmentCoordinationRecordBytes {
		t.Fatalf("encoded coordination bytes = %d, want <= %d", len(encoded), maximumEnvironmentCoordinationRecordBytes)
	}
}

// Rationale: a disabled or unconfigured Environment retains only its monotonic
// clock floor; the optional schedule state must be absent rather than encoded
// as empty or partial scheduling authority.
func TestEnvironmentCoordinationRecordAllowsScheduleAbsence(t *testing.T) {
	t.Parallel()
	record := EnvironmentCoordinationRecord{
		EnvironmentID:      coordinationTestEnvironmentID,
		ScheduleClockFloor: time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC),
	}
	encoded, err := encodeEnvironmentCoordinationRecord(record)
	if err != nil {
		t.Fatalf("encodeEnvironmentCoordinationRecord() error = %v", err)
	}
	decoded, err := decodeEnvironmentCoordinationRecord(encoded)
	if err != nil || decoded.CurrentBackupScheduleState != nil {
		t.Fatalf("decodeEnvironmentCoordinationRecord() = %#v, %v", decoded, err)
	}
}

// Rationale: malformed scheduling authority and oversized values must fail
// closed before they can become a mutation fence or scheduling authority.
func TestEnvironmentCoordinationRecordRejectsInvalidAndOversizedState(t *testing.T) {
	t.Parallel()
	floor := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	invalid := EnvironmentCoordinationRecord{
		EnvironmentID:      coordinationTestEnvironmentID,
		ScheduleClockFloor: floor,
		CurrentBackupScheduleState: &CurrentBackupScheduleState{
			PolicyDigest:    strings.Repeat("a", 64),
			Frequency:       "*-*-* 03:15:00",
			EnabledAt:       floor,
			LastEvaluatedAt: floor.Add(-time.Second),
			UpdatedAt:       floor,
		},
	}
	if _, err := encodeEnvironmentCoordinationRecord(invalid); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("encodeEnvironmentCoordinationRecord(invalid) error = %v", err)
	}
	valid := EnvironmentCoordinationRecord{
		EnvironmentID:      coordinationTestEnvironmentID,
		ScheduleClockFloor: floor,
	}
	encoded, err := encodeEnvironmentCoordinationRecord(valid)
	if err != nil {
		t.Fatalf("encodeEnvironmentCoordinationRecord(valid) error = %v", err)
	}
	exact := append(encoded, bytes.Repeat(
		[]byte{' '}, maximumEnvironmentCoordinationRecordBytes-len(encoded),
	)...)
	if len(exact) != maximumEnvironmentCoordinationRecordBytes {
		t.Fatalf("exact coordination bytes = %d", len(exact))
	}
	if _, err := decodeEnvironmentCoordinationRecord(exact); err != nil {
		t.Fatalf("decodeEnvironmentCoordinationRecord(4096 bytes) error = %v", err)
	}
	oversized := append(exact, ' ')
	if _, err := decodeEnvironmentCoordinationRecord(oversized); !errors.Is(
		err,
		errs.New(errs.KindInternal, ""),
	) {
		t.Fatalf("decodeEnvironmentCoordinationRecord(oversized) error = %v", err)
	}
}

// Rationale: schedule authority binds the complete canonical durable policy,
// so changing any policy field or source order must change its digest.
func TestBackupPolicyScheduleDigestBindsCanonicalPolicy(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	policy := BackupPolicyRecord{
		EnvironmentID: coordinationTestEnvironmentID,
		Enabled:       true,
		Frequency:     "*-*-* 03:15:00",
		Keep:          7,
		Encryption:    "none",
		ConnectorID:   "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs: []string{
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			"spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		},
		UpdatedAt: at,
	}
	digest, err := backupPolicyScheduleDigest(policy)
	if err != nil {
		t.Fatalf("backupPolicyScheduleDigest() error = %v", err)
	}
	const golden = "4943b66263a15ae97a5bc2a568631b1594c9c7d29932ff6eef1947ed5aed046f"
	repeated, err := backupPolicyScheduleDigest(policy)
	if err != nil || digest != golden || repeated != golden {
		t.Fatalf("repeated digests = %q and %q, want %q; error = %v", digest, repeated, golden, err)
	}
	reordered := policy
	reordered.SourceIDs = []string{policy.SourceIDs[1], policy.SourceIDs[0]}
	reorderedDigest, err := backupPolicyScheduleDigest(reordered)
	if err != nil {
		t.Fatalf("backupPolicyScheduleDigest(reordered) error = %v", err)
	}
	if len(digest) != 64 || digest == reorderedDigest {
		t.Fatalf("digests = %q and %q", digest, reorderedDigest)
	}
}

// Rationale: an equivalent zero-offset location is still not the canonical UTC
// representation persisted and hashed by the scheduling authority.
func TestBackupPolicyScheduleDigestRejectsEquivalentNonUTCTimestamp(t *testing.T) {
	t.Parallel()
	policy := coordinationTestPolicy(time.Date(
		2026, 8, 24, 10, 0, 0, 0, time.FixedZone("equivalent-zero-offset", 0),
	), "*-*-* 03:15:00")
	if _, err := backupPolicyScheduleDigest(policy); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("backupPolicyScheduleDigest(non-UTC) error = %v", err)
	}
}

// Rationale: policy replacement has one closed state table and one logical
// boundary that cannot regress when the wall clock moves backward.
func TestReplaceEnvironmentCoordinationScheduleTransitions(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	oldPolicy := coordinationTestPolicy(base, "*-*-* 03:15:00")
	oldDigest, err := backupPolicyScheduleDigest(oldPolicy)
	if err != nil {
		t.Fatalf("backupPolicyScheduleDigest(old) error = %v", err)
	}
	existing := EnvironmentCoordinationRecord{
		EnvironmentID:      coordinationTestEnvironmentID,
		ScheduleClockFloor: base.Add(2 * time.Hour),
		CurrentBackupScheduleState: &CurrentBackupScheduleState{
			PolicyDigest:    oldDigest,
			Frequency:       oldPolicy.Frequency,
			EnabledAt:       base.Add(-24 * time.Hour),
			LastEvaluatedAt: base.Add(2 * time.Hour),
			UpdatedAt:       base.Add(2 * time.Hour),
		},
	}
	tests := []struct {
		name              string
		current           EnvironmentCoordinationRecord
		replacement       BackupPolicyRecord
		now               time.Time
		wantSchedule      bool
		wantEnabledAt     time.Time
		wantLastEvaluated time.Time
		wantNextRunAt     time.Time
	}{
		{
			name: "disabled retains maximum floor", current: existing,
			replacement: BackupPolicyRecord{EnvironmentID: coordinationTestEnvironmentID, UpdatedAt: base},
			now:         base.Add(-time.Hour), wantSchedule: false,
		},
		{
			name: "enable seeds at boundary",
			current: EnvironmentCoordinationRecord{
				EnvironmentID: coordinationTestEnvironmentID, ScheduleClockFloor: base,
			},
			replacement:       coordinationTestPolicy(base, "*-*-* 03:15:00"),
			now:               base.Add(time.Hour),
			wantSchedule:      true,
			wantEnabledAt:     base.Add(time.Hour),
			wantLastEvaluated: base.Add(time.Hour),
			wantNextRunAt:     time.Date(2026, 8, 25, 3, 15, 0, 0, time.UTC),
		},
		{
			name:              "frequency change reseeds",
			current:           existing,
			replacement:       coordinationTestPolicy(base, "Mon *-*-* 03:15:00"),
			now:               base.Add(3 * time.Hour),
			wantSchedule:      true,
			wantEnabledAt:     base.Add(3 * time.Hour),
			wantLastEvaluated: base.Add(3 * time.Hour),
			wantNextRunAt:     time.Date(2026, 8, 31, 3, 15, 0, 0, time.UTC),
		},
		{
			name:              "same frequency carries enabled time and bounds evaluation",
			current:           existing,
			replacement:       coordinationTestPolicy(base, oldPolicy.Frequency),
			now:               base.Add(3 * time.Hour),
			wantSchedule:      true,
			wantEnabledAt:     existing.CurrentBackupScheduleState.EnabledAt,
			wantLastEvaluated: base.Add(3 * time.Hour),
			wantNextRunAt:     time.Date(2026, 8, 25, 3, 15, 0, 0, time.UTC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, nextRunAt, err := replaceEnvironmentCoordinationSchedule(
				test.current,
				test.replacement,
				test.now,
			)
			if err != nil {
				t.Fatalf("replaceEnvironmentCoordinationSchedule() error = %v", err)
			}
			wantFloor := test.now
			if test.current.ScheduleClockFloor.After(wantFloor) {
				wantFloor = test.current.ScheduleClockFloor
			}
			if test.current.CurrentBackupScheduleState != nil &&
				test.current.CurrentBackupScheduleState.LastEvaluatedAt.After(wantFloor) {
				wantFloor = test.current.CurrentBackupScheduleState.LastEvaluatedAt
			}
			if !next.ScheduleClockFloor.Equal(wantFloor) {
				t.Fatalf("ScheduleClockFloor = %s, want %s", next.ScheduleClockFloor, wantFloor)
			}
			if (next.CurrentBackupScheduleState != nil) != test.wantSchedule {
				t.Fatalf("CurrentBackupScheduleState = %#v, want present %t", next.CurrentBackupScheduleState, test.wantSchedule)
			}
			if !test.wantSchedule {
				if !nextRunAt.IsZero() {
					t.Fatalf("nextRunAt = %s, want zero", nextRunAt)
				}
				return
			}
			state := next.CurrentBackupScheduleState
			if !state.EnabledAt.Equal(test.wantEnabledAt) || !state.LastEvaluatedAt.Equal(test.wantLastEvaluated) ||
				!state.UpdatedAt.Equal(wantFloor) {
				t.Fatalf("schedule state = %#v", state)
			}
			if !nextRunAt.Equal(test.wantNextRunAt) {
				t.Fatalf("nextRunAt = %s, want %s", nextRunAt, test.wantNextRunAt)
			}
			digest, digestErr := backupPolicyScheduleDigest(test.replacement)
			if digestErr != nil || state.PolicyDigest != digest {
				t.Fatalf("PolicyDigest = %q, want %q; error = %v", state.PolicyDigest, digest, digestErr)
			}
		})
	}
}

// Rationale: retained disabled configuration is still durable policy input, so
// a non-empty frequency must use the same closed grammar as enabled state.
func TestReplaceEnvironmentCoordinationScheduleRejectsInvalidDisabledFrequency(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	current := EnvironmentCoordinationRecord{
		EnvironmentID: coordinationTestEnvironmentID, ScheduleClockFloor: at,
	}
	replacement := BackupPolicyRecord{
		EnvironmentID: coordinationTestEnvironmentID,
		Frequency:     "daily",
		Keep:          7,
		Encryption:    "none",
		ConnectorID:   "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs:     []string{"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		UpdatedAt:     at,
	}
	if _, _, err := replaceEnvironmentCoordinationSchedule(
		current,
		replacement,
		at,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("replaceEnvironmentCoordinationSchedule(invalid disabled frequency) error = %v", err)
	}
}

// Rationale: ordinary resource mutations advance only the MVCC revision; they
// must rewrite the complete coordination payload without re-encoding it.
func TestEnvironmentCoordinationRewritePreservesExactBytes(t *testing.T) {
	t.Parallel()
	raw := []byte("{ \"schema\":1,\"kind\":\"environment-coordination\",\"data\":{" +
		"\"environment_id\":\"" + coordinationTestEnvironmentID + "\"," +
		"\"schedule_clock_floor\":\"2026-08-24T12:00:00Z\"," +
		"\"current_backup_schedule_state\":{" +
		"\"policy_digest\":\"26c0b60b0baf19530342375ef75108ff8feefb8b8f9c231a8595b4cad436de60\"," +
		"\"frequency\":\"*-*-* 03:15:00\"," +
		"\"enabled_at\":\"2026-08-24T10:00:00Z\"," +
		"\"last_evaluated_at\":\"2026-08-24T11:00:00Z\"," +
		"\"updated_at\":\"2026-08-24T12:00:00Z\"}} }")
	record, err := decodeEnvironmentCoordinationRecord(raw)
	if err != nil {
		t.Fatalf("decodeEnvironmentCoordinationRecord() error = %v", err)
	}
	evidence := environmentCoordinationEvidence{Record: record, Value: raw}
	mutation, err := evidence.rewriteMutation()
	if err != nil {
		t.Fatalf("rewriteMutation() error = %v", err)
	}
	if mutation.Key != environmentCoordinationKey(coordinationTestEnvironmentID) ||
		!bytes.Equal(mutation.Value, raw) {
		t.Fatalf("rewrite mutation = %#v, want exact bytes %q", mutation, raw)
	}
	mutation.Value[0] = 'x'
	if raw[0] != '{' {
		t.Fatal("rewrite mutation aliases read evidence")
	}
	equivalentLocation := time.FixedZone("equivalent-zero-offset", 0)
	equivalent := evidence
	equivalent.Record.ScheduleClockFloor = evidence.Record.ScheduleClockFloor.In(equivalentLocation)
	equivalentState := *evidence.Record.CurrentBackupScheduleState
	equivalentState.EnabledAt = equivalentState.EnabledAt.In(equivalentLocation)
	equivalentState.LastEvaluatedAt = equivalentState.LastEvaluatedAt.In(equivalentLocation)
	equivalentState.UpdatedAt = equivalentState.UpdatedAt.In(equivalentLocation)
	equivalent.Record.CurrentBackupScheduleState = &equivalentState
	if _, err := equivalent.rewriteMutation(); err != nil {
		t.Fatalf("rewriteMutation(equivalent instants) error = %v", err)
	}
	changed := evidence
	changed.Record.CurrentBackupScheduleState = &CurrentBackupScheduleState{
		PolicyDigest:    evidence.Record.CurrentBackupScheduleState.PolicyDigest,
		Frequency:       evidence.Record.CurrentBackupScheduleState.Frequency,
		EnabledAt:       evidence.Record.CurrentBackupScheduleState.EnabledAt,
		LastEvaluatedAt: evidence.Record.CurrentBackupScheduleState.LastEvaluatedAt.Add(time.Second),
		UpdatedAt:       evidence.Record.CurrentBackupScheduleState.UpdatedAt,
	}
	if _, err := changed.rewriteMutation(); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("rewriteMutation(changed record) error = %v, want internal", err)
	}
}

func coordinationTestPolicy(at time.Time, frequency string) BackupPolicyRecord {
	return BackupPolicyRecord{
		EnvironmentID: coordinationTestEnvironmentID,
		Enabled:       true,
		Frequency:     frequency,
		Keep:          7,
		Encryption:    "none",
		ConnectorID:   "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		SourceIDs:     []string{"spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		UpdatedAt:     at,
	}
}
