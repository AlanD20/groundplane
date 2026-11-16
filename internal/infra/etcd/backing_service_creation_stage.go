package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingServiceCreationStagePrefix = "/v1/staging/backing-services/"

// BackingServiceCreationStage owns the stable identities needed before a new
// backing Environment can enter the normal desired-revision staging protocol.
// Its private encrypted seed is captured only after dependencies settle and
// keeps generated bootstrap credentials stable across interrupted compilation.
type BackingServiceCreationStage struct {
	BootstrapSeed secrets.StoredValue                  `json:"bootstrap_seed"`
	Locator       idempotencyrecord.IdempotencyLocator `json:"locator"`
	RequestSHA256 string                               `json:"request_sha256"`
	ProjectID     string                               `json:"project_id"`
	EnvironmentID string                               `json:"environment_id"`
	TaskID        string                               `json:"task_id"`
	CreatedAt     time.Time                            `json:"created_at"`
	Existing      bool                                 `json:"-"`
}

func (repository *HierarchyRepository) ClaimBackingServiceCreationStage(
	ctx context.Context,
	candidate BackingServiceCreationStage,
	acceptedTaskRevision int64,
) (etcdstore.Versioned[BackingServiceCreationStage], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	if err := validateBackingServiceCreationStage(candidate); err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	key, err := backingServiceCreationStageKey(candidate.Locator)
	if err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	value, err := recordcodec.Encode("backing_service_creation_stage", candidate)
	if err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	defer clear(value)
	if acceptedTaskRevision <= 0 {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, errs.New(
			errs.KindValidationFailed,
			"Backing creation requires accepted Task authority",
		)
	}
	result, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{
			{Key: key},
			{Key: workqueue.TaskKey(candidate.TaskID), ModRevision: acceptedTaskRevision},
		},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	if result.Succeeded {
		return etcdstore.Versioned[BackingServiceCreationStage]{
			Record:       candidate,
			Revision:     result.Revision,
			ReadRevision: result.Revision,
		}, nil
	}
	existing, err := repository.store.Get(ctx, key)
	if err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	if existing == nil || existing.Entry == nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, recordcodec.StateConflict(
			"backing-service creation stage",
			candidate.TaskID,
		)
	}
	record, err := decodeBackingServiceCreationStage(existing.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, err
	}
	if record.Locator != candidate.Locator || record.RequestSHA256 != candidate.RequestSHA256 ||
		record.TaskID != candidate.TaskID ||
		record.ProjectID != candidate.ProjectID ||
		record.EnvironmentID != candidate.EnvironmentID {
		return etcdstore.Versioned[BackingServiceCreationStage]{}, errs.New(
			errs.KindIdempotencyMismatch,
			"Idempotency-Key was already used with a different backing-service request",
		)
	}
	record.Existing = true
	return etcdstore.Versioned[BackingServiceCreationStage]{
		Record:       record,
		Revision:     existing.Entry.ModRevision,
		ReadRevision: existing.ReadRevision,
	}, nil
}

func backingServiceCreationStageKey(locator idempotencyrecord.IdempotencyLocator) (string, error) {
	markerKey, err := idempotencyrecord.IdempotencyMarkerKey(locator)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(markerKey))
	return backingServiceCreationStagePrefix + hex.EncodeToString(digest[:]), nil
}

func decodeBackingServiceCreationStage(value []byte) (BackingServiceCreationStage, error) {
	record, err := recordcodec.Decode[BackingServiceCreationStage](value, "backing_service_creation_stage")
	if err != nil || validateBackingServiceCreationStage(record) != nil {
		return BackingServiceCreationStage{}, errs.New(errs.KindInternal, "Backing-service creation stage is corrupt")
	}
	return record, nil
}

func validateBackingServiceCreationStage(record BackingServiceCreationStage) error {
	if _, err := idempotencyrecord.IdempotencyMarkerKey(record.Locator); err != nil {
		return err
	}
	if secrets.ValidateStoredValue(record.BootstrapSeed) != nil || record.BootstrapSeed.Cipher != "age-x25519" ||
		record.BootstrapSeed.SecretID != ids.DeriveAt(
			ids.KindSecret,
			record.CreatedAt,
			record.TaskID,
			"backing-bootstrap-seed",
		) {
		return errs.New(errs.KindValidationFailed, "Backing creation encrypted bootstrap seed is invalid")
	}
	if record.Locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform || record.Locator.ScopeID != "-" {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage scope is invalid")
	}
	digest, err := hex.DecodeString(record.RequestSHA256)
	if err != nil || len(digest) != sha256.Size || record.RequestSHA256 != hex.EncodeToString(digest) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation request digest is invalid")
	}
	if ids.Validate(ids.KindProject, record.ProjectID) != nil ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, record.TaskID) != nil || !blueprints.ValidBlueprintRecordTime(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage identity is invalid")
	}
	return nil
}
