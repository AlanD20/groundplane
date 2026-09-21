package backuppolicymutations

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupqueries "github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupsources"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// BackupPolicyInitialKeyMaterial is cryptographic output supplied by the
// application. Persistence owns the era, owner, and lifecycle timestamps.
type BackupPolicyInitialKeyMaterial struct {
	Recipient  string
	Ciphertext []byte
}

// PreparedBackupPolicyReplacement keeps raw compare evidence opaque while
// exposing the exact typed projection the application serializes into the
// protected 200 response.
type PreparedBackupPolicyReplacement struct {
	candidate          ReplacementCandidate
	projection         backupqueries.BackupPolicyProjection
	requiresInitialKey bool
}

func (prepared PreparedBackupPolicyReplacement) Projection() backupqueries.BackupPolicyProjection {
	return backupqueries.CloneBackupPolicyProjection(prepared.projection)
}

func (prepared PreparedBackupPolicyReplacement) RequiresInitialKey() bool {
	return prepared.requiresInitialKey
}

// FinalizeSchedule chooses the replacement logical boundary after any initial
// key preparation and immediately before the protected response is sealed.
func (prepared PreparedBackupPolicyReplacement) FinalizeSchedule(
	now time.Time,
) (PreparedBackupPolicyReplacement, error) {
	if prepared.requiresInitialKey || (prepared.candidate.InitialKey == nil &&
		prepared.candidate.Replacement.Enabled &&
		prepared.candidate.Replacement.Encryption == "age" &&
		prepared.candidate.ExistingKey == nil) {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed, "backup policy initial key is not prepared",
		)
	}
	if err := SealBackupPolicyCandidateSchedule(&prepared.candidate, now); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if err := ValidateReplacementCandidate(context.Background(), prepared.candidate); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.projection = backupPolicyProjectionFromCandidate(prepared.candidate)
	return prepared, nil
}

// Destroy clears private key ciphertext retained by an abandoned or completed
// prepared replacement. The prepared value must not be reused afterward.
func (prepared *PreparedBackupPolicyReplacement) Destroy() {
	if prepared == nil {
		return
	}
	if prepared.candidate.ExistingKey != nil {
		clear(prepared.candidate.ExistingKey.Encrypted.Ciphertext)
	}
	if prepared.candidate.InitialKey != nil {
		clear(prepared.candidate.InitialKey.Encrypted.Ciphertext)
	}
	*prepared = PreparedBackupPolicyReplacement{}
}

// PrepareBackupPolicyReplacement resolves stable source catalog identities and
// captures every hierarchy, owner-index, Connector, key, and reverse-reference
// compare inside the etcd adapter. Callers never construct KeyValue evidence.
func (repository *Repository) PrepareBackupPolicyReplacement(
	ctx context.Context,
	input backuppolicy.BackupPolicyReplacementInput,
) (PreparedBackupPolicyReplacement, error) {
	if err := backuppolicy.ValidateReplacementInput(ctx, input); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if repository.store == nil {
		return PreparedBackupPolicyReplacement{}, errs.New(errs.KindInternal, "hierarchy store is required")
	}
	hierarchy := hierarchyrecord.NewReader(repository.store)
	environment, err := hierarchy.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	project, err := hierarchy.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Record.TenantID == "" {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed,
			"backing environments cannot own backup policies",
		)
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindStateConflict,
			"environment is not ready for backup policy replacement",
		)
	}

	resolved := make([]etcdstore.Versioned[backuppolicy.BackupSourceRecord], len(input.Sources))
	for index, source := range input.Sources {
		if err := repository.validateBackupPolicySelectionTarget(ctx, input.EnvironmentID, source); err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
		resolved[index], err = backupsources.EnsureBackupSource(
			ctx,
			repository.store,
			repository.now,
			environment,
			project,
			source.Kind,
			source.TargetID,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}

	now := repository.now().UTC()
	candidate, keyFound, err := repository.loadBackupPolicyReplacementBase(
		ctx,
		input,
		environment.Record.ProjectID,
		project.Record.TenantID,
		now,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	keepCandidate := false
	defer func() {
		if candidate.ExistingKey != nil && !keepCandidate {
			clear(candidate.ExistingKey.Encrypted.Ciphertext)
		}
	}()
	for index, source := range resolved {
		candidate.Replacement.SourceIDs[index] = source.Record.ID
		candidate.Sources[index], err = repository.loadSourceEvidence(
			ctx,
			source,
			candidate.MutationEpoch.ReadRevision,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}
	if input.Enabled {
		candidate.Connector, candidate.ConnectorOwnerIndex, err = repository.loadBackupPolicyConnectorEvidence(
			ctx,
			input.EnvironmentID,
			input.ConnectorID,
			candidate.MutationEpoch.ReadRevision,
		)
		if err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
	}
	candidate.ConnectorReferences, err = repository.loadBackupPolicyConnectorReferences(
		ctx,
		candidate,
		candidate.MutationEpoch.ReadRevision,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if err := SealBackupPolicyCandidateSchedule(&candidate, now); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	requiresInitialKey := input.Enabled && input.Encryption == "age" && !keyFound
	projection := backupqueries.BackupPolicyProjection{}
	if !requiresInitialKey {
		if err := ValidateReplacementCandidate(ctx, candidate); err != nil {
			return PreparedBackupPolicyReplacement{}, err
		}
		projection = backupPolicyProjectionFromCandidate(candidate)
	}
	keepCandidate = true
	return PreparedBackupPolicyReplacement{
		candidate: candidate, projection: projection, requiresInitialKey: requiresInitialKey,
	}, nil
}

func (repository *Repository) SupplyBackupPolicyInitialKey(
	ctx context.Context,
	prepared PreparedBackupPolicyReplacement,
	material BackupPolicyInitialKeyMaterial,
) (PreparedBackupPolicyReplacement, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	if !prepared.requiresInitialKey || prepared.candidate.InitialKey != nil {
		return PreparedBackupPolicyReplacement{}, errs.New(
			errs.KindValidationFailed, "backup policy preparation does not require initial key material",
		)
	}
	initial, err := NewInitialKey(
		prepared.candidate.Replacement.EnvironmentID,
		prepared.candidate.Replacement.UpdatedAt,
		&material,
	)
	if err != nil {
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.candidate.InitialKey = initial
	if err := ValidateReplacementCandidate(ctx, prepared.candidate); err != nil {
		clear(initial.Encrypted.Ciphertext)
		return PreparedBackupPolicyReplacement{}, err
	}
	prepared.requiresInitialKey = false
	prepared.projection = backupPolicyProjectionFromCandidate(prepared.candidate)
	return prepared, nil
}
