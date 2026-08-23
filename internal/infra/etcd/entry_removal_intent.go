package etcd

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const entryRemovalIntentPrefix = "/v1/records/entry-removal-intents/"

// EntryRemovalIntent is the private immutable candidate owned by one removal
// Task. Public Entry and applied projection state stay active until successful
// terminal acknowledgement promotes CandidateProjection and removes Entry.
type EntryRemovalIntent struct {
	TaskID                    string                        `json:"task_id"`
	EnvironmentID             string                        `json:"environment_id"`
	EntryID                   string                        `json:"entry_id"`
	EntryRevision             int64                         `json:"entry_revision"`
	CurrentProjectionRevision int64                         `json:"current_projection_revision,omitempty"`
	CurrentProjection         *EnvironmentComposeProjection `json:"current_projection,omitempty"`
	CandidateProjection       *EnvironmentComposeProjection `json:"candidate_projection,omitempty"`
	Status                    TaskStatus                    `json:"status"`
	CreatedAt                 time.Time                     `json:"created_at"`
	TerminalAt                *time.Time                    `json:"terminal_at,omitempty"`
}

func NewEntryRemovalIntent(
	taskID string,
	environmentID string,
	entryID string,
	entryRevision int64,
	projection *Versioned[EnvironmentComposeProjection],
	createdAt time.Time,
) (EntryRemovalIntent, error) {
	intent := EntryRemovalIntent{
		TaskID: taskID, EnvironmentID: environmentID, EntryID: entryID,
		EntryRevision: entryRevision, Status: TaskStatusPending, CreatedAt: createdAt,
	}
	if projection != nil {
		candidate, changed, err := RemoveEnvironmentEntry(projection.Record, entryID)
		if err != nil {
			return EntryRemovalIntent{}, err
		}
		if changed {
			current := cloneEnvironmentComposeProjection(projection.Record)
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
) (Versioned[EntryRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EntryRemovalIntent]{}, false, err
	}
	if validateStableID(ids.KindTask, taskID) != nil {
		return Versioned[EntryRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry removal intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, entryRemovalIntentKey(taskID))
	if err != nil {
		return Versioned[EntryRemovalIntent]{}, false, err
	}
	if result == nil {
		return Versioned[EntryRemovalIntent]{}, false, errs.New(
			errs.KindInternal,
			"Entry removal intent read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[EntryRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeEntryRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return Versioned[EntryRemovalIntent]{}, false, corruptEntryRemovalIntent()
	}
	return Versioned[EntryRemovalIntent]{
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
	return encodeEnvelope("entry_removal_intent", intent)
}

func decodeEntryRemovalIntent(value []byte) (EntryRemovalIntent, error) {
	intent, err := decodeEnvelope[EntryRemovalIntent](value, "entry_removal_intent")
	if err != nil {
		return EntryRemovalIntent{}, err
	}
	if err := validateEntryRemovalIntent(intent); err != nil {
		return EntryRemovalIntent{}, corruptEntryRemovalIntent()
	}
	return intent, nil
}

func validateEntryRemovalIntent(intent EntryRemovalIntent) error {
	if validateStableID(ids.KindTask, intent.TaskID) != nil ||
		validateStableID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		validateStableID(ids.KindEnvEntry, intent.EntryID) != nil || intent.EntryRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Entry removal intent identity is invalid")
	}
	if err := validateTimestamp("Entry removal intent created_at", intent.CreatedAt); err != nil {
		return err
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
		if err := validateTimestamp("Entry removal intent terminal_at", *intent.TerminalAt); err != nil {
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
		validateEnvironmentComposeProjection(*intent.CurrentProjection) != nil ||
		validateEnvironmentComposeProjection(*intent.CandidateProjection) != nil {
		return errs.New(errs.KindValidationFailed, "Entry removal intent projection is invalid")
	}
	expected, changed, err := RemoveEnvironmentEntry(*intent.CurrentProjection, intent.EntryID)
	if err != nil || !changed || !sameEntryRemovalProjection(expected, *intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Entry removal candidate projection changed")
	}
	return nil
}

func sameEntryRemovalProjection(left EnvironmentComposeProjection, right EnvironmentComposeProjection) bool {
	leftValue, leftErr := encodeEnvironmentComposeProjection(left)
	rightValue, rightErr := encodeEnvironmentComposeProjection(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftValue, rightValue)
}

func cloneEntryRemovalIntent(source EntryRemovalIntent) EntryRemovalIntent {
	clone := source
	if source.CurrentProjection != nil {
		value := cloneEnvironmentComposeProjection(*source.CurrentProjection)
		clone.CurrentProjection = &value
	}
	if source.CandidateProjection != nil {
		value := cloneEnvironmentComposeProjection(*source.CandidateProjection)
		clone.CandidateProjection = &value
	}
	clone.TerminalAt = cloneTimePointer(source.TerminalAt)
	return clone
}

func corruptEntryRemovalIntent() error {
	return errs.New(errs.KindInternal, "Entry removal intent is corrupt")
}
