package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalreferences"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A retained declaration identifies physical members, not current references.
// Check the new sealed head and any actual applied state separately; neither is
// manufactured from the removal source. The caller fences the head and applied
// projection in the same publication transaction.
func (repository *TaskRepository) authoredServiceRemovalReferenceGuards(
	ctx context.Context, intent environmentchanges.ServiceRemovalIntent,
	current keyvalue.Versioned[servicerecord.ServiceRecord],
	source keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection],
) ([]keyvalue.Condition, error) {
	if intent.Authored.ProjectionSource == environmentchanges.ServiceRemovalAppliedProjection {
		guards, err := serviceremovalreferences.Validate(ctx, repository.store, current, source)
		return guards.Conditions(), err
	}
	for _, component := range source.Record.Components {
		if slices.Contains(component.Runtime.GeneratedServices, intent.ServiceID) {
			return nil, errs.New(
				errs.KindResourceInUse,
				"Component-generated Services are removed through their Component",
			)
		}
	}
	keys := []string{
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(intent.EnvironmentID, intent.Authored.ParentTaskID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: source.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != source.ReadRevision || len(read.Values) != len(keys) {
		return nil, errs.New(errs.KindInternal, "Service removal reference read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, errs.New(
			errs.KindStateConflict,
			"Service removal is waiting for the new Environment runtime projection",
		)
	}
	var conditions []keyvalue.Condition
	for index, value := range read.Values {
		if value == nil {
			continue
		}
		projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
		if err != nil || projection.EnvironmentID != intent.EnvironmentID ||
			index == 0 && projection.RevisionID != intent.Authored.ParentTaskID {
			return nil, errs.New(errs.KindInternal, "Service removal reference projection is corrupt")
		}
		guards, err := serviceremovalreferences.Validate(ctx, repository.store, current,
			keyvalue.Versioned[projectionrecord.EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: read.ReadRevision,
			})
		if err != nil {
			return nil, err
		}
		if index == 0 {
			conditions = append(conditions, guards.Conditions()...)
			conditions = append(conditions, keyvalue.Condition{Key: keys[0], ModRevision: value.ModRevision})
		}
	}
	return conditions, nil
}
