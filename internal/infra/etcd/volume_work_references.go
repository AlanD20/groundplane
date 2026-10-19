package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareVolumeDesiredReferences keeps the original closed removal rule:
// removing one Volume cannot omit another Volume, an Entry, or unrelated
// desired resources. Captured Script references remain deletion fences.
func (repository *HierarchyRepository) prepareVolumeDesiredReferences(ctx context.Context,
	task workexecution.Task, projection projectionrecord.EnvironmentComposeProjection,
	expectedHeadRevision, readRevision int64,
) ([]keyvalue.Condition, error) {
	if ids.Validate(ids.KindVolume, task.TargetID) != nil || task.Scope.EnvironmentID != projection.EnvironmentID ||
		expectedHeadRevision < 0 || readRevision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "Volume desired mutation has invalid resource ownership")
	}
	previous, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx,
		projection.EnvironmentID,
		readRevision,
	)
	if err != nil {
		return nil, err
	}
	if (expectedHeadRevision == 0 && found) ||
		(expectedHeadRevision > 0 && (!found || previous.Revision != expectedHeadRevision)) {
		return nil, errs.New(errs.KindStateConflict, "Environment desired state changed before Volume publication")
	}
	removedID := ""
	if task.Action == workexecution.ActionRemove {
		removedID = task.TargetID
	}
	if err := projectionrecord.ValidateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(previous.Record, found, projection, removedID); err != nil {
		return nil, err
	}
	if err := projectionrecord.PreserveEnvironmentNonEntryDesiredResources(previous.Record, found, projection); err != nil {
		return nil, err
	}
	// Volume actions never carry Entry-removal authority, even when an Entry
	// currently has no Script references.
	retained := make(map[string]struct{}, len(projection.Entries))
	for _, entry := range projection.Entries {
		retained[entry.Entry.ID] = struct{}{}
	}
	for _, entry := range previous.Record.Entries {
		if _, exists := retained[entry.Entry.ID]; !exists {
			return nil, errs.New(errs.KindValidationFailed, "Volume mutation cannot remove an Entry")
		}
	}
	if removedID == "" {
		return nil, nil
	}
	existed := false
	for _, volume := range previous.Record.Volumes {
		if volume.ID == removedID {
			existed = true
			break
		}
	}
	if !existed {
		return nil, errs.New(errs.KindStateConflict, "Volume removal target is absent from its captured baseline")
	}
	for _, volume := range projection.Volumes {
		if volume.ID == removedID {
			return nil, errs.New(errs.KindValidationFailed, "Volume removal still declares its target")
		}
	}
	return prepareVolumeScriptAbsence(ctx, repository.store, removedID, readRevision)
}
