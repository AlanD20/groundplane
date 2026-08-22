package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComponentRepository owns Environment-component singleton identity,
// fixed-revision reads, and desired/runtime CAS separation.
type ComponentRepository struct {
	store hierarchyStore
}

func NewComponentRepository(store Store) (*ComponentRepository, error) {
	return newComponentRepository(store)
}

func newComponentRepository(store hierarchyStore) (*ComponentRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Component store is required")
	}
	return &ComponentRepository{store: store}, nil
}

func (repository *ComponentRepository) CreateEnvironmentComponent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ComponentRecord,
) (Versioned[ComponentRecord], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, record); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	value, err := encodeComponentRecord(record)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		componentWriteConditions(environment, project, record, nil, 0, 0),
		[]Mutation{
			{Type: MutationPut, Key: componentKey(record.Desired.ID), Value: value},
			{
				Type:  MutationPut,
				Key:   componentEnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID),
				Value: []byte(record.Desired.ID),
			},
			{
				Type:  MutationPut,
				Key:   componentEnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind),
				Value: []byte(record.Desired.ID),
			},
		},
	)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ComponentRecord]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, record, 0,
		)
	}
	return Versioned[ComponentRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ComponentRepository) GetComponent(
	ctx context.Context,
	id string,
) (Versioned[ComponentRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if err := validateID(ids.KindComponent, id); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		componentKey(id),
		id,
		errs.KindComponentNotFound,
		decodeComponentRecord,
		func(record ComponentRecord) string { return record.Desired.ID },
	)
}

func (repository *ComponentRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ComponentRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ComponentRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"components",
		"environment",
		environmentID,
		componentEnvironmentOwnerPrefix(environmentID),
		componentKey,
		ids.KindComponent,
		request,
		decodeComponentRecord,
		func(record ComponentRecord) string { return record.Desired.ID },
		func(record ComponentRecord) bool {
			return record.Desired.Owner == core.ComponentOwnerEnvironment && record.Desired.OwnerID == environmentID
		},
	)
}

func (repository *ComponentRepository) ReplaceDesired(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ComponentRecord],
	desired core.Component,
) (Versioned[ComponentRecord], error) {
	replacement, err := ReplaceComponentDesired(current.Record, desired)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *ComponentRepository) ReplaceRuntime(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ComponentRecord],
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (Versioned[ComponentRecord], error) {
	replacement, err := SetComponentRuntime(current.Record, generatedServices, pinnedIPv4, healthy)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *ComponentRepository) replace(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ComponentRecord],
	replacement ComponentRecord,
) (Versioned[ComponentRecord], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, replacement); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if err := validateComponentVersion(current); err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if current.Record.Desired.ID != replacement.Desired.ID {
		return Versioned[ComponentRecord]{}, errs.New(errs.KindValidationFailed, "Component replacement changed id")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			componentEnvironmentOwnerKey(current.Record.Desired.OwnerID, current.Record.Desired.ID),
			componentEnvironmentKindKey(current.Record.Desired.OwnerID, current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return Versioned[ComponentRecord]{}, errs.New(errs.KindInternal, "Component indexes are missing or corrupt")
	}
	value, err := encodeComponentRecord(replacement)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		componentWriteConditions(
			environment,
			project,
			current.Record,
			&current,
			indexes.Values[0].ModRevision,
			indexes.Values[1].ModRevision,
		),
		[]Mutation{{Type: MutationPut, Key: componentKey(replacement.Desired.ID), Value: value}},
	)
	if err != nil {
		return Versioned[ComponentRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ComponentRecord]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
		)
	}
	return Versioned[ComponentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func componentWriteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ComponentRecord,
	current *Versioned[ComponentRecord],
	ownerRevision int64,
	kindRevision int64,
) []Condition {
	primary := Condition{Key: componentKey(record.Desired.ID)}
	owner := Condition{Key: componentEnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID)}
	kind := Condition{Key: componentEnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind)}
	if current != nil {
		primary.ModRevision = current.Revision
		owner.ModRevision = ownerRevision
		kind.ModRevision = kindRevision
	}
	return []Condition{
		primary,
		owner,
		kind,
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("component", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", project.Record.TenantID)},
	}
}

func validateEnvironmentComponentHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ComponentRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	if err := validateComponentRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.Record.ProjectID != project.Record.ID ||
		record.Desired.Owner != core.ComponentOwnerEnvironment || record.Desired.OwnerID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Component hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateComponentVersion(current Versioned[ComponentRecord]) error {
	if err := validateComponentRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Component version metadata is invalid")
	}
	return nil
}

func classifyComponentWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ComponentRecord,
	expectedRevision int64,
) error {
	if len(values) != 9 {
		return errs.New(errs.KindInternal, "Component write compare evidence is incomplete")
	}
	componentID := record.Desired.ID
	if expectedRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Component stable identity is already in use")
		}
		if values[2] != nil {
			return errs.New(errs.KindNameConflict, "Component kind already exists for this Environment")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindComponentNotFound, "Component was not found")
		}
		if values[0].ModRevision != expectedRevision {
			return stateConflict("component", componentID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != componentID {
				return errs.New(errs.KindInternal, "Component index changed or is corrupt")
			}
		}
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7, 8} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Component hierarchy deletion is in progress")
		}
	}
	return stateConflict("component", componentID)
}
