package backuppolicymutations

import (
	"context"
	"encoding/json"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	coordinationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentcoordination"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func ValidateBackupPolicyReplacement(
	ctx context.Context,
	candidate ReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if err := ValidateReplacementCandidate(ctx, candidate); err != nil {
		return err
	}
	return validateBackupPolicyReplacementMarker(candidate, marker)
}

func validateBackupPolicyReplacementMarker(
	candidate ReplacementCandidate,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != candidate.Replacement.EnvironmentID ||
		marker.Locator.Method != http.MethodPut || marker.Locator.Route != backupPolicyReplacementRoute ||
		marker.ReplayTarget != nil || marker.Response.Status != http.StatusOK ||
		marker.Response.ContentKind != "application/json" || !json.Valid(marker.Response.Body) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || !marker.TerminalAt.Equal(marker.CreatedAt) ||
		!marker.RetainUntil.Equal(marker.TerminalAt.Add(idempotencyrecord.MarkerRetention)) {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy marker must be the retained completed response for its exact put operation",
		)
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func ValidateReplacementCandidate(
	ctx context.Context,
	candidate ReplacementCandidate,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(candidate.Environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(candidate.Project.Record); err != nil {
		return err
	}
	if err := backupruntime.ValidateEnvironmentMutationEpochRecord(candidate.MutationEpoch.Record); err != nil {
		return err
	}
	if err := backuppolicy.ValidateBackupPolicyRecord(candidate.Replacement); err != nil {
		return err
	}
	if !validReplacementRevision(candidate.Environment.Revision, candidate.Environment.ReadRevision) ||
		!(validReplacementRevision(candidate.Coordination.Revision, candidate.Coordination.ReadRevision) ||
			(candidate.Coordination.Revision == 0 && candidate.Current == nil &&
				candidate.Coordination.ReadRevision > 0)) ||
		!candidate.ScheduleSealed ||
		!validReplacementRevision(candidate.Project.Revision, candidate.Project.ReadRevision) ||
		!validReplacementRevision(candidate.MutationEpoch.Revision, candidate.MutationEpoch.ReadRevision) ||
		candidate.Replacement.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.MutationEpoch.Record.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.Coordination.Record.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.NextCoordination.EnvironmentID != candidate.Environment.Record.ID ||
		candidate.Environment.Record.ProjectID != candidate.Project.Record.ID ||
		candidate.Project.Record.Kind != hierarchyrecord.ProjectKindTenant || candidate.Project.Record.TenantID == "" {
		return errs.New(errs.KindValidationFailed, "backup policy hierarchy is invalid")
	}
	if candidate.Environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return errs.New(errs.KindStateConflict, "environment is not ready for backup policy replacement")
	}
	if !coordinationrecord.Equal(candidate.NextCoordination, mustBackupPolicyScheduleTransition(candidate)) {
		return errs.New(errs.KindValidationFailed, "backup policy schedule transition is invalid")
	}
	if candidate.Replacement.Enabled {
		if err := backuppolicy.ValidateFrequency(candidate.Replacement.Frequency); err != nil {
			return err
		}
	}
	if candidate.Current != nil {
		if err := backuppolicy.ValidateBackupPolicyRecord(candidate.Current.Record); err != nil {
			return err
		}
		if !validReplacementRevision(candidate.Current.Revision, candidate.Current.ReadRevision) ||
			candidate.Current.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "current backup policy evidence is invalid")
		}
	}
	if len(candidate.Sources) != len(candidate.Replacement.SourceIDs) {
		return errs.New(errs.KindValidationFailed, "backup policy source evidence is incomplete")
	}
	type sourceIdentity struct {
		kind     string
		targetID string
	}
	identities := make(map[sourceIdentity]struct{}, len(candidate.Sources))
	for index := range candidate.Sources {
		source := candidate.Sources[index].Source
		if err := backuppolicy.ValidateBackupSourceRecord(source.Record); err != nil {
			return err
		}
		if !validReplacementRevision(source.Revision, source.ReadRevision) ||
			source.Record.ID != candidate.Replacement.SourceIDs[index] ||
			source.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy source evidence is invalid")
		}
		identity := sourceIdentity{kind: string(source.Record.Kind), targetID: source.Record.TargetID}
		if _, duplicate := identities[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source identities must be unique")
		}
		identities[identity] = struct{}{}
	}
	for index := range candidate.Sources {
		if err := ValidateSourceEvidence(
			candidate.Replacement.EnvironmentID,
			candidate.Replacement.SourceIDs[index],
			candidate.Sources[index],
		); err != nil {
			return err
		}
	}
	if candidate.Replacement.Enabled {
		if candidate.Connector == nil {
			return errs.New(errs.KindValidationFailed, "enabled backup policy requires connector evidence")
		}
		if err := connectorrecord.ValidateRecord(candidate.Connector.Record); err != nil {
			return err
		}
		if !validReplacementRevision(candidate.Connector.Revision, candidate.Connector.ReadRevision) ||
			candidate.Connector.Record.Connector.ID != candidate.Replacement.ConnectorID ||
			candidate.Connector.Record.Connector.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy connector evidence is invalid")
		}
		if !validBackupPolicyIndex(
			candidate.ConnectorOwnerIndex,
			connectorrecord.ConnectorEnvironmentKey(
				candidate.Replacement.EnvironmentID,
				candidate.Replacement.ConnectorID,
			),
			candidate.Replacement.ConnectorID,
		) {
			return recordcodec.CorruptRecord()
		}
	} else if candidate.Connector != nil || candidate.ConnectorOwnerIndex != nil {
		return errs.New(errs.KindValidationFailed, "disabled backup policy cannot carry connector evidence")
	}
	if err := validateBackupPolicyKeyEvidence(candidate); err != nil {
		return err
	}
	return validateBackupPolicyConnectorReferences(candidate)
}

func validReplacementRevision(revision int64, readRevision int64) bool {
	return revision > 0 && readRevision >= revision
}

func ValidateSourceEvidence(
	environmentID string,
	wantSourceID string,
	evidence SourceEvidence,
) error {
	if err := backuppolicy.ValidateBackupSourceRecord(evidence.Source.Record); err != nil {
		return err
	}
	if !validReplacementRevision(evidence.Source.Revision, evidence.Source.ReadRevision) ||
		evidence.Source.Record.ID != wantSourceID || evidence.Source.Record.EnvironmentID != environmentID {
		return errs.New(errs.KindValidationFailed, "backup policy source evidence is invalid")
	}
	if !validBackupPolicyIndex(
		evidence.EnvironmentIndex,
		backuppolicy.BackupSourceEnvironmentKey(environmentID, evidence.Source.Record.ID),
		evidence.Source.Record.ID,
	) || !validBackupPolicyIndex(
		evidence.IdentityIndex,
		backuppolicy.BackupSourceIdentityKey(environmentID, evidence.Source.Record.Kind, evidence.Source.Record.TargetID),
		evidence.Source.Record.ID,
	) {
		return recordcodec.CorruptRecord()
	}
	switch string(evidence.Source.Record.Kind) {
	case "config":
		if evidence.Source.Record.TargetID != environmentID || evidence.Attach != nil ||
			evidence.Volume != nil || evidence.TargetOwnerIndex != nil {
			return errs.New(errs.KindValidationFailed, "config backup source cannot carry target evidence")
		}
	case "attach":
		if evidence.Attach == nil || evidence.Volume != nil ||
			attachrecord.ValidateAttachRecord(evidence.Attach.Record) != nil ||
			!validReplacementRevision(evidence.Attach.Revision, evidence.Attach.ReadRevision) ||
			evidence.Attach.Record.ID != evidence.Source.Record.TargetID ||
			!evidence.Attach.Record.OwnsCredential() ||
			evidence.Attach.Record.EnvironmentID != environmentID ||
			!validBackupPolicyIndex(
				evidence.TargetOwnerIndex,
				attachrecord.AttachOwnerKey(environmentID, evidence.Attach.Record.ID),
				evidence.Attach.Record.ID,
			) {
			return errs.New(errs.KindValidationFailed, "attach backup source evidence is invalid")
		}
	case "volume":
		if evidence.Volume == nil || evidence.Attach != nil ||
			evidence.TargetOwnerIndex != nil ||
			!validReplacementRevision(evidence.Volume.Projection.Revision, evidence.Volume.Projection.ReadRevision) ||
			evidence.Volume.ProjectionRoot <= 0 ||
			evidence.Volume.Volume.ID != evidence.Source.Record.TargetID ||
			evidence.Volume.Projection.Record.EnvironmentID != environmentID ||
			evidence.Volume.Projection.Record.RevisionID == "" ||
			!recordcodec.ValidSHA256(evidence.Volume.DependencyDigest) {
			return errs.New(errs.KindValidationFailed, "volume backup source evidence is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup policy source kind is invalid")
	}
	return nil
}

func validBackupPolicyIndex(entry *etcdstore.KeyValue, key string, value string) bool {
	return entry != nil && entry.Key == key && entry.ModRevision > 0 && string(entry.Value) == value
}

func validateBackupPolicyKeyEvidence(candidate ReplacementCandidate) error {
	if candidate.ExistingKey != nil {
		if err := backuppolicy.ValidateVersionedBackupKey(*candidate.ExistingKey); err != nil {
			return err
		}
		if candidate.ExistingKey.Record.EnvironmentID != candidate.Replacement.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "backup policy key owner is invalid")
		}
	}
	wantsInitialKey := candidate.Replacement.Enabled && candidate.Replacement.Encryption == "age" &&
		candidate.ExistingKey == nil
	if wantsInitialKey != (candidate.InitialKey != nil) {
		return errs.New(errs.KindValidationFailed, "backup policy initial key evidence is invalid")
	}
	if candidate.InitialKey == nil {
		return nil
	}
	if err := backuppolicy.ValidateBackupKeyRecord(candidate.InitialKey.Record); err != nil {
		return err
	}
	if err := backuppolicy.ValidateBackupKeyEncryptedValue(candidate.InitialKey.Encrypted); err != nil {
		return err
	}
	if candidate.InitialKey.Record.EnvironmentID != candidate.Replacement.EnvironmentID ||
		candidate.InitialKey.Encrypted.EnvironmentID != candidate.Replacement.EnvironmentID ||
		candidate.InitialKey.Record.KeyEra != 1 || candidate.InitialKey.Encrypted.KeyEra != 1 ||
		!candidate.InitialKey.Record.CreatedAt.Equal(candidate.InitialKey.Record.RotatedAt) {
		return errs.New(errs.KindValidationFailed, "backup policy initial key must be one sealed era-1 pair")
	}
	return nil
}

func validateBackupPolicyConnectorReferences(candidate ReplacementCandidate) error {
	environmentID := candidate.Replacement.EnvironmentID
	oldConnectorID := ""
	if candidate.Current != nil && candidate.Current.Record.Enabled {
		oldConnectorID = candidate.Current.Record.ConnectorID
	}
	newConnectorID := ""
	if candidate.Replacement.Enabled {
		newConnectorID = candidate.Replacement.ConnectorID
	}
	expected := make([]struct {
		connectorID string
		present     bool
	}, 0, 2)
	if oldConnectorID != "" {
		expected = append(expected, struct {
			connectorID string
			present     bool
		}{connectorID: oldConnectorID, present: true})
	}
	if newConnectorID != "" && newConnectorID != oldConnectorID {
		expected = append(expected, struct {
			connectorID string
			present     bool
		}{connectorID: newConnectorID})
	}
	if len(candidate.ConnectorReferences) != len(expected) {
		return errs.New(errs.KindValidationFailed, "backup policy connector reference evidence is incomplete")
	}
	for index, want := range expected {
		evidence := candidate.ConnectorReferences[index]
		if evidence.ConnectorID != want.connectorID {
			return errs.New(errs.KindValidationFailed, "backup policy connector reference order is invalid")
		}
		if want.present {
			if evidence.Entry == nil || evidence.Entry.Key != backuppolicy.BackupPolicyConnectorReferenceKey(
				want.connectorID,
				environmentID,
			) || evidence.Entry.ModRevision <= 0 || string(evidence.Entry.Value) != environmentID {
				return recordcodec.CorruptRecord()
			}
		} else if evidence.Entry != nil {
			return recordcodec.CorruptRecord()
		}
	}
	return nil
}
