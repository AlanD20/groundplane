package backuppolicymutations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReplacementCandidateRejectsPrivateEvidenceDrift(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		wantKind errs.Kind
		mutate   func(*ReplacementCandidate)
	}{
		{
			name: "non-ready environment", wantKind: errs.KindStateConflict,
			mutate: func(candidate *ReplacementCandidate) {
				candidate.Environment.Record.ProvisioningState = hierarchy.EnvironmentProvisioningProvisioning
			},
		},
		{
			name: "source order", wantKind: errs.KindValidationFailed,
			mutate: func(candidate *ReplacementCandidate) {
				volume := replacementCandidateVolumeSource(
					candidate.Replacement.EnvironmentID,
					candidate.Replacement.UpdatedAt,
					40,
				)
				candidate.Sources = append(candidate.Sources, volume)
				candidate.Replacement.SourceIDs = append(candidate.Replacement.SourceIDs, volume.Source.Record.ID)
				candidate.Sources[0], candidate.Sources[1] = candidate.Sources[1], candidate.Sources[0]
			},
		},
		{
			name: "duplicate source identity", wantKind: errs.KindValidationFailed,
			mutate: func(candidate *ReplacementCandidate) {
				duplicate := candidate.Sources[0]
				duplicate.Source.Record.ID = ids.NewAt(ids.KindBackupSource, candidate.Replacement.UpdatedAt, 50)
				duplicate.Source.Revision++
				duplicate.Source.ReadRevision++
				candidate.Sources = append(candidate.Sources, duplicate)
				candidate.Replacement.SourceIDs = append(candidate.Replacement.SourceIDs, duplicate.Source.Record.ID)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := validReplacementCandidate(t)
			test.mutate(&candidate)
			if err := ValidateReplacementCandidate(context.Background(), candidate); !errors.Is(
				err, errs.New(test.wantKind, ""),
			) {
				t.Fatalf("ValidateReplacementCandidate() error = %v", err)
			}
		})
	}
}

func TestReplacementProjectionPreservesMaximumPublicKeep(t *testing.T) {
	t.Parallel()
	projection := backupPolicyProjectionFromCandidate(ReplacementCandidate{
		Replacement: backuppolicy.BackupPolicyRecord{Keep: backuppolicy.MaximumBackupPolicyKeep},
	})
	if projection.Keep != backuppolicy.MaximumBackupPolicyKeep {
		t.Fatalf("projection Keep = %d, want %d", projection.Keep, backuppolicy.MaximumBackupPolicyKeep)
	}
}

func TestReplacementPlanFitsWorstCaseAtomicBudget(t *testing.T) {
	t.Parallel()
	marker := idempotency.IdempotencyMarker{RetainUntil: time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)}
	for _, test := range []struct {
		count int
		want  int
	}{{count: backuppolicy.MaximumBackupPolicySources, want: 84}, {count: backuppolicy.MaximumBackupPolicySources + 1, want: 89}} {
		candidate := replacementBudgetCandidate(t, test.count)
		plan, err := PrepareBackupPolicyReplacement(candidate)
		if err != nil {
			t.Fatalf("PrepareBackupPolicyReplacement(%d sources) error = %v", test.count, err)
		}
		defer plan.Clear()
		if operations := plan.OperationCount(marker); operations != test.want ||
			operations > keyvalue.MaximumOperations {
			t.Fatalf(
				"%d-source operation budget = %d, want %d <= %d",
				test.count,
				operations,
				test.want,
				keyvalue.MaximumOperations,
			)
		}
	}
}

func validReplacementCandidate(t *testing.T) ReplacementCandidate {
	t.Helper()
	now := time.Date(2026, 8, 23, 16, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 1)
	projectID := ids.NewAt(ids.KindProject, now, 2)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	connectorID := ids.NewAt(ids.KindConnector, now, 4)
	sourceID := ids.NewAt(ids.KindBackupSource, now, 5)
	source := backuppolicy.BackupSourceRecord{
		ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceConfig,
		TargetID: environmentID, CreatedAt: now,
	}
	candidate := ReplacementCandidate{
		Environment: keyvalue.Versioned[hierarchy.EnvironmentRecord]{
			Record: hierarchy.EnvironmentRecord{
				ID: environmentID, ProjectID: projectID, Name: "production", NetworkPool: "10.240.0.0/24",
				VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
				ProvisioningState: hierarchy.EnvironmentProvisioningReady,
				CreateTaskID:      ids.NewAt(ids.KindTask, now, 6), CreatedAt: now,
			},
			Revision: 1, ReadRevision: 1,
		},
		Project: keyvalue.Versioned[hierarchy.ProjectRecord]{
			Record: hierarchy.ProjectRecord{
				ID: projectID, TenantID: tenantID, Slug: "backup-project", Name: "Backup Project", Kind: hierarchy.ProjectKindTenant,
			},
			Revision: 1, ReadRevision: 1,
		},
		MutationEpoch: keyvalue.Versioned[backupruntime.EnvironmentMutationEpochRecord]{
			Record:   backupruntime.EnvironmentMutationEpochRecord{EnvironmentID: environmentID},
			Revision: 1, ReadRevision: 1,
		},
		Coordination: keyvalue.Versioned[environmentcoordination.EnvironmentCoordinationRecord]{
			Record: environmentcoordination.EnvironmentCoordinationRecord{
				EnvironmentID:      environmentID,
				ScheduleClockFloor: now,
			},
			Revision: 1, ReadRevision: 1,
		},
		Replacement: backuppolicy.BackupPolicyRecord{
			EnvironmentID: environmentID, Frequency: "*-*-* 03:00:00", Keep: 7,
			Encryption: "age", ConnectorID: connectorID, SourceIDs: []string{sourceID}, UpdatedAt: now,
		},
		Sources: []SourceEvidence{{
			Source: keyvalue.Versioned[backuppolicy.BackupSourceRecord]{Record: source, Revision: 1, ReadRevision: 1},
			EnvironmentIndex: &keyvalue.KeyValue{
				Key: backuppolicy.BackupSourceEnvironmentKey(
					environmentID,
					sourceID,
				), Value: []byte(sourceID), ModRevision: 1,
			},
			IdentityIndex: &keyvalue.KeyValue{
				Key: backuppolicy.BackupSourceIdentityKey(
					environmentID,
					source.Kind,
					source.TargetID,
				), Value: []byte(sourceID), ModRevision: 1,
			},
		}},
	}
	if err := SealBackupPolicyCandidateSchedule(&candidate, now); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReplacementCandidate(context.Background(), candidate); err != nil {
		t.Fatalf("valid candidate rejected: %v", err)
	}
	return candidate
}

func replacementCandidateVolumeSource(environmentID string, now time.Time, seed int64) SourceEvidence {
	sourceID := ids.NewAt(ids.KindBackupSource, now, seed)
	volumeID := ids.NewAt(ids.KindVolume, now, seed+1)
	revisionID := ids.NewAt(ids.KindTask, now, seed+2)
	source := backuppolicy.BackupSourceRecord{
		ID: sourceID, EnvironmentID: environmentID, Kind: core.BackupSourceVolume,
		TargetID: volumeID, CreatedAt: now,
	}
	volume := environmentprojection.EnvironmentVolumeIdentity{ID: volumeID, Slug: "data", Key: "data"}
	return SourceEvidence{
		Source: keyvalue.Versioned[backuppolicy.BackupSourceRecord]{Record: source, Revision: 1, ReadRevision: 1},
		EnvironmentIndex: &keyvalue.KeyValue{
			Key: backuppolicy.BackupSourceEnvironmentKey(
				environmentID,
				sourceID,
			), Value: []byte(sourceID), ModRevision: 1,
		},
		IdentityIndex: &keyvalue.KeyValue{
			Key: backuppolicy.BackupSourceIdentityKey(
				environmentID,
				source.Kind,
				source.TargetID,
			), Value: []byte(sourceID), ModRevision: 1,
		},
		Volume: &environmentqueries.BackupVolumeProjectionEvidence{
			Projection: keyvalue.Versioned[environmentprojection.EnvironmentComposeProjection]{
				Record: environmentprojection.EnvironmentComposeProjection{
					EnvironmentID: environmentID, RevisionID: revisionID, RenderGeneration: 1,
					Volumes: []environmentprojection.EnvironmentVolumeIdentity{volume},
				},
				Revision: 1, ReadRevision: 1,
			},
			ProjectionRoot: 1, DependencyDigest: strings.Repeat("a", 64), Volume: volume,
		},
	}
}

func replacementBudgetCandidate(t *testing.T, count int) ReplacementCandidate {
	t.Helper()
	candidate := validReplacementCandidate(t)
	now := candidate.Replacement.UpdatedAt
	oldConnectorID := ids.NewAt(ids.KindConnector, now, 60)
	newConnectorID := candidate.Replacement.ConnectorID
	candidate.Replacement.Enabled = true
	candidate.Sources = make([]SourceEvidence, count)
	candidate.Replacement.SourceIDs = make([]string, count)
	for index := range candidate.Sources {
		candidate.Sources[index] = replacementCandidateVolumeSource(
			candidate.Replacement.EnvironmentID,
			now,
			int64(100+index*3),
		)
		candidate.Replacement.SourceIDs[index] = candidate.Sources[index].Source.Record.ID
	}
	candidate.Current = &keyvalue.Versioned[backuppolicy.BackupPolicyRecord]{
		Record: backuppolicy.BackupPolicyRecord{
			EnvironmentID: candidate.Replacement.EnvironmentID, Enabled: true,
			Frequency: "*-*-* 02:00:00", Keep: 3, Encryption: "none",
			ConnectorID: oldConnectorID, SourceIDs: []string{candidate.Replacement.SourceIDs[0]}, UpdatedAt: now.Add(-time.Second),
		},
		Revision: 1, ReadRevision: 1,
	}
	candidate.Connector = &keyvalue.Versioned[connectors.Record]{
		Record: connectors.Record{Connector: core.Connector{ID: newConnectorID}}, Revision: 1, ReadRevision: 1,
	}
	candidate.ConnectorOwnerIndex = &keyvalue.KeyValue{
		Key:         connectors.ConnectorEnvironmentKey(candidate.Replacement.EnvironmentID, newConnectorID),
		ModRevision: 1,
	}
	candidate.ConnectorReferences = []ConnectorReferenceEvidence{
		{ConnectorID: oldConnectorID, Entry: &keyvalue.KeyValue{ModRevision: 1}},
		{ConnectorID: newConnectorID},
	}
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	candidate.InitialKey, err = NewInitialKey(candidate.Replacement.EnvironmentID, now, &BackupPolicyInitialKeyMaterial{
		Recipient: identity.Recipient().String(), Ciphertext: []byte("controller-sealed-age-identity"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return candidate
}
