package etcd

import (
	"bytes"
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryRemovalIntentPrefix = "/v1/records/entry-removal-intents/"

// EntryRemovalIntent is the private immutable candidate owned by one removal
// Task. Public Entry and applied projection state stay active until successful
// terminal acknowledgement promotes CandidateProjection and removes Entry.
type EntryRemovalIntent struct {
	TaskID                    string                                         `json:"task_id"`
	EnvironmentID             string                                         `json:"environment_id"`
	EntryID                   string                                         `json:"entry_id"`
	EntryRevision             int64                                          `json:"entry_revision"`
	CurrentProjectionRevision int64                                          `json:"current_projection_revision,omitempty"`
	CurrentProjection         *projectionrecord.EnvironmentComposeProjection `json:"current_projection,omitempty"`
	CandidateProjection       *projectionrecord.EnvironmentComposeProjection `json:"candidate_projection,omitempty"`
	Desired                   *EntryRemovalDesiredRevision                   `json:"desired,omitempty"`
	Status                    TaskStatus                                     `json:"status"`
	CreatedAt                 time.Time                                      `json:"created_at"`
	TerminalAt                *time.Time                                     `json:"terminal_at,omitempty"`
}

// EntryRemovalDesiredRevision binds the existing staged desired revision to
// terminal removal. Applied cleanup authority remains independently pinned.
type EntryRemovalDesiredRevision struct {
	DescriptorID     string `json:"descriptor_id"`
	BaseRevisionID   string `json:"base_revision_id"`
	RevisionID       string `json:"revision_id"`
	RenderGeneration uint64 `json:"render_generation"`
}

func NewDesiredEntryRemovalIntent(
	entryID, baseRevisionID string, claim EnvironmentBlueprintStageClaim,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
) (EntryRemovalIntent, error) {
	if err := validateEnvironmentBlueprintStageClaim(claim); err != nil {
		return EntryRemovalIntent{}, err
	}
	intent, err := NewEntryRemovalIntent(claim.TaskID, claim.EnvironmentID, entryID,
		claim.BaselineHeadRevision, projection, claim.CreatedAt)
	if err != nil {
		return EntryRemovalIntent{}, err
	}
	intent.Desired = &EntryRemovalDesiredRevision{DescriptorID: claim.DescriptorID,
		BaseRevisionID: baseRevisionID, RevisionID: claim.RevisionID, RenderGeneration: claim.RenderGeneration}
	if intent.CandidateProjection != nil {
		intent.CandidateProjection.RevisionID = claim.RevisionID
		intent.CandidateProjection.RenderGeneration = claim.RenderGeneration
	}
	if err := validateEntryRemovalIntent(intent); err != nil {
		return EntryRemovalIntent{}, err
	}
	return intent, nil
}

func NewEntryRemovalIntent(
	taskID string,
	environmentID string,
	entryID string,
	entryRevision int64,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	createdAt time.Time,
) (EntryRemovalIntent, error) {
	intent := EntryRemovalIntent{
		TaskID: taskID, EnvironmentID: environmentID, EntryID: entryID,
		EntryRevision: entryRevision, Status: TaskStatusPending, CreatedAt: createdAt,
	}
	if projection != nil {
		candidate, changed, err := projectionrecord.RemoveEnvironmentEntry(projection.Record, entryID)
		if err != nil {
			return EntryRemovalIntent{}, err
		}
		if changed {
			current := projectionrecord.CloneEnvironmentComposeProjection(projection.Record)
			intent.CurrentProjectionRevision = projection.Revision
			intent.CurrentProjection = &current
			intent.CandidateProjection = &candidate
		}
	}
	if err := validateEntryRemovalIntent(intent); err != nil {
		return EntryRemovalIntent{}, err
	}
	return cloneEntryRemovalIntent(intent), nil
}

func entryRemovalIntentKey(taskID string) string {
	return entryRemovalIntentPrefix + taskID
}

func (repository *HierarchyRepository) GetEntryRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[EntryRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EntryRemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[EntryRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry removal intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, entryRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[EntryRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[EntryRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Entry removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[EntryRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeEntryRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[EntryRemovalIntent]{}, false, corruptEntryRemovalIntent()
	}
	return etcdstore.Versioned[EntryRemovalIntent]{
		Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func terminalEntryRemovalIntent(
	intent EntryRemovalIntent,
	status TaskStatus,
	terminalAt time.Time,
) (EntryRemovalIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return EntryRemovalIntent{}, errs.New(errs.KindStateConflict, "Entry removal intent is not pending")
	}
	terminal := cloneEntryRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(terminalAt)
	if err := validateEntryRemovalIntent(terminal); err != nil {
		return EntryRemovalIntent{}, err
	}
	return terminal, nil
}

func encodeEntryRemovalIntent(intent EntryRemovalIntent) ([]byte, error) {
	if err := validateEntryRemovalIntent(intent); err != nil {
		return nil, err
	}
	return recordcodec.Encode("entry_removal_intent", intent)
}

func decodeEntryRemovalIntent(value []byte) (EntryRemovalIntent, error) {
	intent, err := recordcodec.Decode[EntryRemovalIntent](value, "entry_removal_intent")
	if err != nil {
		return EntryRemovalIntent{}, err
	}
	if err := validateEntryRemovalIntent(intent); err != nil {
		return EntryRemovalIntent{}, corruptEntryRemovalIntent()
	}
	return intent, nil
}

func validateEntryRemovalIntent(intent EntryRemovalIntent) error {
	if recordcodec.ValidateID(ids.KindTask, intent.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindEnvEntry, intent.EntryID) != nil || intent.EntryRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Entry removal intent identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("Entry removal intent created_at", intent.CreatedAt); err != nil {
		return err
	}
	if desired := intent.Desired; desired != nil {
		if ids.Validate(ids.KindTask, "task_"+desired.DescriptorID) != nil ||
			ids.Validate(ids.KindTask, desired.BaseRevisionID) != nil ||
			ids.Validate(ids.KindTask, desired.RevisionID) != nil || desired.RenderGeneration == 0 ||
			desired.RevisionID == desired.BaseRevisionID {
			return errs.New(errs.KindValidationFailed, "Entry removal desired revision is invalid")
		}
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Entry removal intent has a terminal timestamp")
		}
	} else {
		if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil ||
			intent.TerminalAt.Before(intent.CreatedAt) {
			return errs.New(errs.KindValidationFailed, "Entry removal intent terminal state is invalid")
		}
		if err := recordcodec.ValidateTimestamp("Entry removal intent terminal_at", *intent.TerminalAt); err != nil {
			return err
		}
	}
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		if intent.CurrentProjection != nil || intent.CandidateProjection != nil ||
			intent.CurrentProjectionRevision != 0 {
			return errs.New(errs.KindValidationFailed, "Entry removal intent projection state is incomplete")
		}
		return nil
	}
	if intent.CurrentProjectionRevision <= 0 ||
		intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		projectionrecord.ValidateEnvironmentComposeProjection(*intent.CurrentProjection) != nil ||
		projectionrecord.ValidateEnvironmentComposeProjection(*intent.CandidateProjection) != nil {
		return errs.New(errs.KindValidationFailed, "Entry removal intent projection is invalid")
	}
	expected, changed, err := projectionrecord.RemoveEnvironmentEntry(*intent.CurrentProjection, intent.EntryID)
	if desired := intent.Desired; desired != nil {
		if desired.RenderGeneration <= intent.CurrentProjection.RenderGeneration {
			return errs.New(errs.KindValidationFailed, "Entry removal desired generation does not advance")
		}
		expected.RevisionID, expected.RenderGeneration = desired.RevisionID, desired.RenderGeneration
	}
	if err != nil || !changed || !sameEntryRemovalProjection(expected, *intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Entry removal candidate projection changed")
	}
	return nil
}

func sameEntryRemovalProjection(left projectionrecord.EnvironmentComposeProjection, right projectionrecord.EnvironmentComposeProjection) bool {
	leftValue, leftErr := projectionrecord.EncodeEnvironmentComposeProjectionStorage(left)
	rightValue, rightErr := projectionrecord.EncodeEnvironmentComposeProjectionStorage(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func cloneEntryRemovalIntent(source EntryRemovalIntent) EntryRemovalIntent {
	clone := source
	if source.Desired != nil {
		value := *source.Desired
		clone.Desired = &value
	}
	if source.CurrentProjection != nil {
		value := projectionrecord.CloneEnvironmentComposeProjection(*source.CurrentProjection)
		clone.CurrentProjection = &value
	}
	if source.CandidateProjection != nil {
		value := projectionrecord.CloneEnvironmentComposeProjection(*source.CandidateProjection)
		clone.CandidateProjection = &value
	}
	clone.TerminalAt = cloneTimePointer(source.TerminalAt)
	return clone
}

func corruptEntryRemovalIntent() error {
	return errs.New(errs.KindInternal, "Entry removal intent is corrupt")
}
