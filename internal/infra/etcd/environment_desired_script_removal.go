package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedDesiredScriptRemoval struct {
	entryIDs   []string
	volumeID   string
	conditions []etcdstore.Condition
}

// Desired publication may remove Entries or the one explicitly targeted Volume.
// Retaining a stable id permits edits without invalidating older Script sources.
func (repository *HierarchyRepository) prepareDesiredScriptRemoval(
	ctx context.Context,
	environmentID string,
	expectedHeadRevision, readRevision int64,
	projection projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
) (preparedDesiredScriptRemoval, error) {
	previous, hasPrevious, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx,
		environmentID,
		readRevision,
	)
	if err != nil {
		return preparedDesiredScriptRemoval{}, err
	}
	if (expectedHeadRevision == 0 && hasPrevious) ||
		(expectedHeadRevision > 0 && (!hasPrevious || previous.Revision != expectedHeadRevision)) {
		return preparedDesiredScriptRemoval{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if err := validateEnvironmentComposeProjectionPublicationAdvance(previous.Record, hasPrevious, projection, task); err != nil {
		return preparedDesiredScriptRemoval{}, err
	}
	retained := make(map[string]struct{}, len(projection.Entries)+len(projection.Volumes))
	for _, entry := range projection.Entries {
		retained[entry.Entry.ID] = struct{}{}
	}
	for _, volume := range projection.Volumes {
		retained[volume.ID] = struct{}{}
	}
	var removal preparedDesiredScriptRemoval
	for _, entry := range previous.Record.Entries {
		if _, exists := retained[entry.Entry.ID]; exists {
			continue
		}
		fences, err := prepareEntryScriptAbsence(ctx, repository.store, entry.Entry.ID, readRevision)
		if err != nil {
			return preparedDesiredScriptRemoval{}, err
		}
		removal.entryIDs = append(removal.entryIDs, entry.Entry.ID)
		removal.conditions = append(removal.conditions, fences...)
	}
	if task.Type == taskjournal.TaskRemove && task.Params[TaskResourceKindParam] == TaskResourceVolume {
		// The advance validator above already proves that no other Volume is
		// omitted. Preserve that closed explicit-removal authority here.
		for _, volume := range previous.Record.Volumes {
			if volume.ID != task.Target {
				continue
			}
			if _, exists := retained[volume.ID]; exists {
				continue
			}
			fences, err := prepareVolumeScriptAbsence(ctx, repository.store, volume.ID, readRevision)
			if err != nil {
				return preparedDesiredScriptRemoval{}, err
			}
			removal.volumeID = volume.ID
			removal.conditions = append(removal.conditions, fences...)
		}
	}
	return removal, nil
}

func (removal preparedDesiredScriptRemoval) classifyConflict(
	baseCount int, base idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	entries := classifyEntryScriptAbsenceConflict(removal.entryIDs, baseCount, base)
	if removal.volumeID == "" {
		return entries
	}
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseCount+len(removal.conditions) {
			return errs.New(errs.KindInternal, "desired removal Script compare evidence is incomplete")
		}
		volumeStart := len(values) - 2
		if err := classifyVolumeScriptReferences(removal.volumeID, values[volumeStart:]); err != nil {
			return err
		}
		return entries(revision, values[:volumeStart])
	}
}
