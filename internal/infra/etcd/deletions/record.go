package deletions

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
	"unicode/utf8"
)

// DeletionTargetKind is the closed hierarchy deletion catalog. Additional
// resource kinds enter this catalog only with their resource-specific
// finalization contract.
type DeletionTargetKind string

const (
	DeletionTargetTenant       DeletionTargetKind = "tenant"
	DeletionTargetProject      DeletionTargetKind = "project"
	DeletionTargetEnvironment  DeletionTargetKind = "environment"
	DeletionTargetEntry        DeletionTargetKind = "entry"
	DeletionTargetRoute        DeletionTargetKind = "route"
	DeletionTargetScript       DeletionTargetKind = "script"
	DeletionTargetSecret       DeletionTargetKind = "secret"
	DeletionTargetConnector    DeletionTargetKind = "connector"
	DeletionTargetZone         DeletionTargetKind = "zone"
	DeletionTargetReleaseGroup DeletionTargetKind = "release_group"
	DeletionTargetService      DeletionTargetKind = "service"
	DeletionTargetRunner       DeletionTargetKind = "runner"
)

// DeletionPhase records which authority may advance a destructive operation.
// A completed tombstone is never retained.
type DeletionPhase string

const (
	DeletionPhaseHostEffects DeletionPhase = "host_effects"
	DeletionPhaseFinalizing  DeletionPhase = "finalizing"
)

// DeletionCheckpoint identifies the last fully finalized descendant. Empty
// fields are the initial checkpoint; nonempty fields are always paired.
type DeletionCheckpoint struct {
	ResourceKind string `json:"resource_kind,omitempty"`
	StableID     string `json:"stable_id,omitempty"`
}

// DeletionTombstoneRecord fences one resource while its destructive Task
// performs any authority-owned effects and atomic finalization.
type DeletionTombstoneRecord struct {
	TargetKind     DeletionTargetKind `json:"target_kind"`
	TargetID       string             `json:"target_id"`
	TargetRevision int64              `json:"target_revision"`
	TaskID         string             `json:"task_id"`
	Phase          DeletionPhase      `json:"phase"`
	Checkpoint     DeletionCheckpoint `json:"checkpoint"`
	CreatedAt      time.Time          `json:"created_at"`
	UpdatedAt      time.Time          `json:"updated_at"`
}

func EncodeDeletionTombstone(record DeletionTombstoneRecord) ([]byte, error) {
	if err := ValidateDeletionTombstone(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("deletion-tombstone", record)
}

func DecodeDeletionTombstone(value []byte) (DeletionTombstoneRecord, error) {
	record, err := recordcodec.Decode[DeletionTombstoneRecord](value, "deletion-tombstone")
	if err != nil {
		return DeletionTombstoneRecord{}, err
	}
	if err := ValidateDeletionTombstone(record); err != nil {
		return DeletionTombstoneRecord{}, CorruptDeletionTombstone()
	}
	return record, nil
}

func ValidateDeletionTombstone(record DeletionTombstoneRecord) error {
	if err := ValidateDeletionTarget(record.TargetKind, record.TargetID); err != nil {
		return err
	}
	if record.TargetRevision <= 0 || recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		(record.Phase != DeletionPhaseHostEffects && record.Phase != DeletionPhaseFinalizing) ||
		record.CreatedAt.IsZero() || !record.CreatedAt.Equal(record.CreatedAt.UTC()) ||
		record.UpdatedAt.Before(
			record.CreatedAt,
		) || !record.UpdatedAt.Equal(record.UpdatedAt.UTC()) {
		return errs.New(errs.KindValidationFailed, "deletion tombstone lifecycle is invalid")
	}
	checkpoint := record.Checkpoint
	if (checkpoint.ResourceKind == "") != (checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is incomplete")
	}
	if checkpoint.ResourceKind != "" && (!ValidDeletionCheckpointKind(checkpoint.ResourceKind) ||
		!utf8.ValidString(checkpoint.StableID) || checkpoint.StableID == "") {
		return errs.New(errs.KindValidationFailed, "deletion checkpoint is invalid")
	}
	return nil
}

func ValidateDeletionTarget(kind DeletionTargetKind, id string) error {
	var expected ids.Kind
	switch kind {
	case DeletionTargetTenant:
		expected = ids.KindTenant
	case DeletionTargetProject:
		expected = ids.KindProject
	case DeletionTargetEnvironment:
		expected = ids.KindEnvironment
	case DeletionTargetEntry:
		expected = ids.KindEnvEntry
	case DeletionTargetRoute:
		expected = ids.KindRoute
	case DeletionTargetScript:
		expected = ids.KindScript
	case DeletionTargetSecret:
		expected = ids.KindSecret
	case DeletionTargetConnector:
		expected = ids.KindConnector
	case DeletionTargetZone:
		expected = ids.KindNetwork
	case DeletionTargetReleaseGroup:
		expected = ids.KindReleaseGroup
	case DeletionTargetService:
		expected = ids.KindService
	case DeletionTargetRunner:
		expected = ids.KindRunner
	default:
		return errs.New(errs.KindValidationFailed, "deletion target kind is invalid")
	}
	if recordcodec.ValidateID(expected, id) != nil {
		return errs.New(errs.KindValidationFailed, "deletion target id is invalid")
	}
	return nil
}

func ValidDeletionCheckpointKind(value string) bool {
	switch value {
	case "zone", "service", "route", "volume", "entry", "script", "release_group", "component",
		"connector", "backup_policy", "blueprint_revision", "environment", "project":
		return true
	default:
		return false
	}
}

func CorruptDeletionTombstone() error {
	return errs.New(errs.KindInternal, "deletion tombstone is corrupt")
}
