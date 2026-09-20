package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingServiceCreationStagePrefix = "/v1/staging/backing-services/"

// BackingServiceCreationStage owns the stable identities needed before a new
// backing Environment can enter the normal desired-revision staging protocol.
// It deliberately contains no bootstrap plaintext or encrypted value.
type BackingServiceCreationStage struct {
	Locator       IdempotencyLocator `json:"locator"`
	RequestSHA256 string             `json:"request_sha256"`
	ProjectID     string             `json:"project_id"`
	EnvironmentID string             `json:"environment_id"`
	TaskID        string             `json:"task_id"`
	CreatedAt     time.Time          `json:"created_at"`
	Existing      bool               `json:"-"`
}

func (repository *HierarchyRepository) ClaimBackingServiceCreationStage(
	ctx context.Context,
	candidate BackingServiceCreationStage,
) (Versioned[BackingServiceCreationStage], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	if err := validateBackingServiceCreationStage(candidate); err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	key, err := backingServiceCreationStageKey(candidate.Locator)
	if err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	value, err := encodeEnvelope("backing_service_creation_stage", candidate)
	if err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	if result.Succeeded {
		return Versioned[BackingServiceCreationStage]{
			Record:       candidate,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		}, nil
	}
	existing, err := repository.store.Get(ctx, key)
	if err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	if existing == nil || existing.Entry == nil {
		return Versioned[BackingServiceCreationStage]{}, stateConflict(
			"backing-service creation stage",
			candidate.TaskID,
		)
	}
	record, err := decodeBackingServiceCreationStage(existing.Entry.Value)
	if err != nil {
		return Versioned[BackingServiceCreationStage]{}, err
	}
	if record.Locator != candidate.Locator || record.RequestSHA256 != candidate.RequestSHA256 {
		return Versioned[BackingServiceCreationStage]{}, errs.New(
			errs.KindIdempotencyMismatch,
			"Idempotency-Key was already used with a different backing-service request",
		)
	}
	record.Existing = true
	return Versioned[BackingServiceCreationStage]{
		Record:       record,
		Revision:     existing.Entry.ModRevision,
		ReadRevision: existing.ReadRevision,
	}, nil
}

func backingServiceCreationStageKey(locator IdempotencyLocator) (string, error) {
	markerKey, err := CapabilityIdempotencyMarkerKey(locator)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(markerKey))
	return backingServiceCreationStagePrefix + hex.EncodeToString(digest[:]), nil
}

func decodeBackingServiceCreationStage(value []byte) (BackingServiceCreationStage, error) {
	record, err := decodeEnvelope[BackingServiceCreationStage](value, "backing_service_creation_stage")
	if err != nil || validateBackingServiceCreationStage(record) != nil {
		return BackingServiceCreationStage{}, errs.New(errs.KindInternal, "Backing-service creation stage is corrupt")
	}
	return record, nil
}

func validateBackingServiceCreationStage(record BackingServiceCreationStage) error {
	if _, err := CapabilityIdempotencyMarkerKey(record.Locator); err != nil {
		return err
	}
	if record.Locator.ScopeKind != IdempotencyScopePlatform || record.Locator.ScopeID != "-" {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage scope is invalid")
	}
	digest, err := hex.DecodeString(record.RequestSHA256)
	if err != nil || len(digest) != sha256.Size || record.RequestSHA256 != hex.EncodeToString(digest) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation request digest is invalid")
	}
	if ids.Validate(ids.KindProject, record.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || !ValidDesiredRevisionTime(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage identity is invalid")
	}
	return nil
}
