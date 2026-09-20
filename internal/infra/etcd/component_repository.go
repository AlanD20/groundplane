package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComponentRepository owns Environment-component singleton identity,
// fixed-revision reads, and desired/runtime CAS separation.
type ComponentRepository struct {
	store hierarchyStore
}

func NewComponentRepository(store etcdstore.Store) (*ComponentRepository, error) {
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
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if references, err := componentrecord.SecretReferences(record); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	} else if len(references) > 0 {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"configured Components with Secret references must be published through reconciliation",
		)
	}
	value, err := componentrecord.EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		componentWriteConditions(environment, project, record, nil, 0, 0),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(record.Desired.ID), Value: value},
			{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID),
				Value: []byte(record.Desired.ID),
			},
			{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind),
				Value: []byte(record.Desired.ID),
			},
			componentrecord.WriteFenceMutation(record.Desired.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[componentrecord.Record]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, record, 0,
		)
	}
	return etcdstore.Versioned[componentrecord.Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ComponentRepository) GetComponent(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := validateContext(ctx); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindComponent, id); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		componentrecord.RecordKey(id),
		id,
		errs.KindComponentNotFound,
		componentrecord.DecodeRecord,
		func(record componentrecord.Record) string { return record.Desired.ID },
	)
}

func (repository *ComponentRepository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[componentrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[componentrecord.Record]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"components",
		"environment",
		environmentID,
		componentrecord.EnvironmentOwnerPrefix(environmentID),
		componentrecord.RecordKey,
		ids.KindComponent,
		request,
		componentrecord.DecodeRecord,
		func(record componentrecord.Record) string { return record.Desired.ID },
		func(record componentrecord.Record) bool {
			return record.Desired.Owner == core.ComponentOwnerEnvironment && record.Desired.OwnerID == environmentID
		},
	)
}

func (repository *ComponentRepository) ReplaceDesired(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
) (etcdstore.Versioned[componentrecord.Record], error) {
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *ComponentRepository) ReplaceRuntime(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[componentrecord.Record],
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (etcdstore.Versioned[componentrecord.Record], error) {
	replacement, err := componentrecord.SetRuntime(current.Record, generatedServices, pinnedIPv4, healthy)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *ComponentRepository) replace(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[componentrecord.Record],
	replacement componentrecord.Record,
) (etcdstore.Versioned[componentrecord.Record], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, replacement); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if err := validateComponentVersion(current); err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if current.Record.Desired.ID != replacement.Desired.ID {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(errs.KindValidationFailed, "Component replacement changed id")
	}
	currentSecretIDs, err := componentrecord.SecretReferences(current.Record)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	nextSecretIDs, err := componentrecord.SecretReferences(replacement)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !equalSecretReferences(currentSecretIDs, nextSecretIDs) {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"Component Secret reference changes require Component reconciliation",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			componentrecord.EnvironmentOwnerKey(current.Record.Desired.OwnerID, current.Record.Desired.ID),
			componentrecord.EnvironmentKindKey(current.Record.Desired.OwnerID, current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(errs.KindInternal, "Component indexes are missing or corrupt")
	}
	value, err := componentrecord.EncodeRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
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
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(replacement.Desired.ID), Value: value},
			componentrecord.WriteFenceMutation(replacement.Desired.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[componentrecord.Record]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
		)
	}
	return etcdstore.Versioned[componentrecord.Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func componentWriteConditions(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record componentrecord.Record,
	current *etcdstore.Versioned[componentrecord.Record],
	ownerRevision int64,
	kindRevision int64,
) []etcdstore.Condition {
	primary := etcdstore.Condition{Key: componentrecord.RecordKey(record.Desired.ID)}
	owner := etcdstore.Condition{Key: componentrecord.EnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID)}
	kind := etcdstore.Condition{Key: componentrecord.EnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind)}
	if current != nil {
		primary.ModRevision = current.Revision
		owner.ModRevision = ownerRevision
		kind.ModRevision = kindRevision
	}
	return []etcdstore.Condition{
		primary,
		owner,
		kind,
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("component", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", project.Record.TenantID)},
	}
}

func validateEnvironmentComponentHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record componentrecord.Record,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := componentrecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.Record.ProjectID != project.Record.ID ||
		record.Desired.Owner != core.ComponentOwnerEnvironment || record.Desired.OwnerID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Component hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateComponentVersion(current etcdstore.Versioned[componentrecord.Record]) error {
	if err := componentrecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Component version metadata is invalid")
	}
	return nil
}

func classifyComponentWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record componentrecord.Record,
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
