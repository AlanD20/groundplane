package etcd

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: scheduler replay and retention cleanup depend on one canonical key identity for a due occurrence.
func TestBackupRuntimeDueKeysPreserveCompositeIdentity(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	scheduledAt := time.Unix(0, 123).UTC()
	retainUntil := time.Unix(0, 456).UTC()

	dueKey, err := backupDueOutcomeKey(environmentID, 41, scheduledAt)
	if err != nil {
		t.Fatalf("backupDueOutcomeKey() error = %v", err)
	}
	if dueKey != "/v1/runtime/backup-due/"+environmentID+
		"/00000000000000000041/00000000000000000123" {
		t.Fatalf("backupDueOutcomeKey() = %q", dueKey)
	}
	retentionKey, err := backupDueRetentionIndexKey(retainUntil, environmentID, 41, scheduledAt)
	if err != nil {
		t.Fatalf("backupDueRetentionIndexKey() error = %v", err)
	}
	wantRetention := "/v1/indexes/backup-due/by-retention/00000000000000000456/" + environmentID +
		"/00000000000000000041/00000000000000000123"
	if retentionKey != wantRetention {
		t.Fatalf("backupDueRetentionIndexKey() = %q, want %q", retentionKey, wantRetention)
	}
}

// Rationale: fixed-revision point lists use ascending etcd ranges while the product contract requires newest first.
func TestBackupRecoveryPointIndexSuffixInvertsCanonicalULIDOrder(t *testing.T) {
	olderID := "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	newerID := "rp_01BRZ3NDEKTSV4RRFFQ69G5FAV"
	older, err := invertedBackupRecoveryPointID(olderID)
	if err != nil {
		t.Fatalf("invertedBackupRecoveryPointID(older) error = %v", err)
	}
	newer, err := invertedBackupRecoveryPointID(newerID)
	if err != nil {
		t.Fatalf("invertedBackupRecoveryPointID(newer) error = %v", err)
	}
	if len(older) != 26 || len(newer) != 26 || newer >= older {
		t.Fatalf("inverted suffixes older=%q newer=%q do not order newest first", older, newer)
	}
	restored, ok := invertBackupRecoveryPointULIDBody(older)
	if !ok || restored != strings.TrimPrefix(olderID, "rp_") {
		t.Fatalf("double inversion = %q, %v", restored, ok)
	}
}

// Rationale: Connector deletion fences address exact durable point identities rather than public newest-first rows.
func TestBackupConnectorReverseReferenceKeysUseRawStablePointID(t *testing.T) {
	connectorID := "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	pointID := "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"

	pointKey, err := backupRecoveryPointConnectorIndexKey(connectorID, pointID)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	wantPointKey := "/v1/indexes/recovery-points/by-connector/" + connectorID + "/" + pointID
	if pointKey != wantPointKey {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() = %q, want %q", pointKey, wantPointKey)
	}

	orphanKey, err := backupOrphanConnectorIndexKey(connectorID, pointID)
	if err != nil {
		t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
	}
	wantOrphanKey := "/v1/indexes/backup-orphans/by-connector/" + connectorID + "/" + pointID
	if orphanKey != wantOrphanKey {
		t.Fatalf("backupOrphanConnectorIndexKey() = %q, want %q", orphanKey, wantOrphanKey)
	}
}

// Rationale: every runtime category needs a stable constructor before repositories compose atomic compare/mutation plans.
func TestBackupRuntimeKeyConstructorsUseLockedV1Roots(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	taskID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	sourceID := "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	pointID := "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	connectorID := "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	volumeID := "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	cursorKey, err := backupScheduleCursorKey(environmentID, 7)
	if err != nil {
		t.Fatalf("backupScheduleCursorKey() error = %v", err)
	}
	volumeExclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetVolume, volumeID)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	connectorPointKey, err := backupRecoveryPointConnectorIndexKey(connectorID, pointID)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	runEnvironmentKey, err := backupRunEnvironmentIndexKey(environmentID, taskID)
	if err != nil {
		t.Fatalf("backupRunEnvironmentIndexKey() error = %v", err)
	}
	orphanConnectorKey, err := backupOrphanConnectorIndexKey(connectorID, pointID)
	if err != nil {
		t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
	}
	orphanEnvironmentKey, err := backupOrphanEnvironmentIndexKey(environmentID, pointID)
	if err != nil {
		t.Fatalf("backupOrphanEnvironmentIndexKey() error = %v", err)
	}
	restoreEnvironmentKey, err := backupRestoreEnvironmentIndexKey(environmentID, taskID)
	if err != nil {
		t.Fatalf("backupRestoreEnvironmentIndexKey() error = %v", err)
	}
	rotationEnvironmentKey, err := backupKeyRotationEnvironmentIndexKey(environmentID, taskID)
	if err != nil {
		t.Fatalf("backupKeyRotationEnvironmentIndexKey() error = %v", err)
	}

	cases := map[string]string{
		"cursor":               cursorKey,
		"lock":                 environmentOperationLockKey(environmentID),
		"source exclusion":     volumeExclusionKey,
		"run":                  backupRunKey(taskID),
		"run environment":      runEnvironmentKey,
		"point":                backupRecoveryPointKey(pointID),
		"point connector":      connectorPointKey,
		"orphan":               backupOrphanKey(pointID),
		"orphan connector":     orphanConnectorKey,
		"orphan environment":   orphanEnvironmentKey,
		"retention":            backupRetentionKey(sourceID, pointID),
		"prune":                backupRecoveryPointPruneKey(pointID),
		"prune dispatch":       backupRecoveryPointPruneDispatchKey(taskID),
		"terminal receipt":     backupTerminalReceiptKey(taskID),
		"restore":              backupRestoreKey(taskID),
		"restore environment":  restoreEnvironmentKey,
		"restore service":      backupRestoreServiceKey(taskID, 1),
		"rotation":             backupKeyRotationKey(taskID),
		"rotation environment": rotationEnvironmentKey,
		"mutation epoch":       environmentMutationEpochKey(environmentID),
	}
	wants := map[string]string{
		"cursor":           "/v1/runtime/backup-schedule-cursors/" + environmentID + "/00000000000000000007",
		"lock":             "/v1/runtime/environment-operation-locks/" + environmentID,
		"source exclusion": "/v1/runtime/backup-source-target-exclusions/volume/" + volumeID,
		"run":              "/v1/runtime/backup-runs/" + taskID,
		"run environment":  "/v1/indexes/backup-runs/by-environment/" + environmentID + "/" + taskID,
		"point":            "/v1/records/recovery-points/" + pointID,
		"point connector": "/v1/indexes/recovery-points/by-connector/" + connectorID +
			"/" + pointID,
		"orphan": "/v1/runtime/backup-orphans/" + pointID,
		"orphan connector": "/v1/indexes/backup-orphans/by-connector/" + connectorID +
			"/" + pointID,
		"orphan environment": "/v1/indexes/backup-orphans/by-environment/" + environmentID +
			"/ZYN70WAJHC564V77GG8SPFTGN4",
		"retention":            "/v1/runtime/backup-retention/" + sourceID + "/" + pointID,
		"prune":                "/v1/runtime/recovery-point-prunes/" + pointID,
		"prune dispatch":       "/v1/runtime/recovery-point-prune-dispatches/" + taskID,
		"terminal receipt":     "/v1/runtime/backup-terminal-receipts/" + taskID,
		"restore":              "/v1/runtime/backup-restores/" + taskID,
		"restore environment":  "/v1/indexes/backup-restores/by-environment/" + environmentID + "/" + taskID,
		"restore service":      "/v1/runtime/backup-restore-services/" + taskID + "/00000000000000000001",
		"rotation":             "/v1/runtime/backup-key-rotations/" + taskID,
		"rotation environment": "/v1/indexes/backup-key-rotations/by-environment/" + environmentID + "/" + taskID,
		"mutation epoch":       "/v1/runtime/environment-mutation-epochs/" + environmentID,
	}
	for name, got := range cases {
		if got != wants[name] {
			t.Errorf("%s key = %q, want %q", name, got, wants[name])
		}
	}
}

// Rationale: lexically ordered scheduler keys must reject nonpositive and UnixNano-overflowing instants.
func TestBackupRuntimeOrderedKeysRejectNoncanonicalTimes(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	invalidTimes := []time.Time{
		time.Unix(0, 0).UTC(),
		time.Date(1969, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	for _, invalid := range invalidTimes {
		if _, err := backupDueOutcomeKey(environmentID, 1, invalid); !errors.Is(
			err,
			errs.New(errs.KindValidationFailed, ""),
		) {
			t.Fatalf("backupDueOutcomeKey(%s) error = %v, want validation", invalid, err)
		}
	}
	if _, err := backupScheduleCursorKey(environmentID, 0); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("backupScheduleCursorKey(revision 0) error = %v, want validation", err)
	}
}
