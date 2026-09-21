package components

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) CreateEnvironmentComponent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record Record,
) (etcdstore.Versioned[Record], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, record); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if references, err := SecretReferences(record); err != nil {
		return etcdstore.Versioned[Record]{}, err
	} else if len(references) > 0 {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindStateConflict,
			"configured Components with Secret references must be published through reconciliation",
		)
	}
	value, err := EncodeRecord(record)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		componentWriteConditions(environment, project, record, nil, 0, 0),
		[]etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: RecordKey(record.Desired.ID), Value: value},
			{
				Type:  etcdstore.MutationPut,
				Key:   EnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID),
				Value: []byte(record.Desired.ID),
			},
			{
				Type:  etcdstore.MutationPut,
				Key:   EnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind),
				Value: []byte(record.Desired.ID),
			},
			WriteFenceMutation(record.Desired.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[Record]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, record, 0,
		)
	}
	return etcdstore.Versioned[Record]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *Repository) GetComponent(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindComponent, id); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return recordquery.Get(
		ctx,
		repository.store,
		RecordKey(id),
		id,
		errs.KindComponentNotFound,
		DecodeRecord,
		func(record Record) string { return record.Desired.ID },
	)
}

func (repository *Repository) ListEnvironmentComponents(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[Record]{}, err
	}
	return recordquery.ListIndex(
		ctx,
		repository.store,
		"components",
		"environment",
		environmentID,
		EnvironmentOwnerPrefix(environmentID),
		RecordKey,
		ids.KindComponent,
		request,
		DecodeRecord,
		func(record Record) string { return record.Desired.ID },
		func(record Record) bool {
			return record.Desired.Owner == core.ComponentOwnerEnvironment && record.Desired.OwnerID == environmentID
		},
	)
}

func (repository *Repository) ReplaceDesired(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[Record],
	desired core.Component,
) (etcdstore.Versioned[Record], error) {
	replacement, err := ReplaceDesired(current.Record, desired)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *Repository) ReplaceRuntime(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[Record],
	generatedServices []string,
	pinnedIPv4 string,
	healthy bool,
) (etcdstore.Versioned[Record], error) {
	replacement, err := SetRuntime(current.Record, generatedServices, pinnedIPv4, healthy)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	return repository.replace(ctx, environment, project, current, replacement)
}

func (repository *Repository) replace(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[Record],
	replacement Record,
) (etcdstore.Versioned[Record], error) {
	if err := validateEnvironmentComponentHierarchy(ctx, environment, project, replacement); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if err := ValidateComponentVersion(current); err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if current.Record.Desired.ID != replacement.Desired.ID {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindValidationFailed, "Component replacement changed id")
	}
	currentSecretIDs, err := SecretReferences(current.Record)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	nextSecretIDs, err := SecretReferences(replacement)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !EqualSecretReferences(currentSecretIDs, nextSecretIDs) {
		return etcdstore.Versioned[Record]{}, errs.New(
			errs.KindStateConflict,
			"Component Secret reference changes require Component reconciliation",
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			EnvironmentOwnerKey(current.Record.Desired.OwnerID, current.Record.Desired.ID),
			EnvironmentKindKey(current.Record.Desired.OwnerID, current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return etcdstore.Versioned[Record]{}, errs.New(errs.KindInternal, "Component indexes are missing or corrupt")
	}
	value, err := EncodeRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
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
			{Type: etcdstore.MutationPut, Key: RecordKey(replacement.Desired.ID), Value: value},
			WriteFenceMutation(replacement.Desired.ID),
		},
	)
	if err != nil {
		return etcdstore.Versioned[Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[Record]{}, classifyComponentWriteConflict(
			result.FailureReads, environment, project, current.Record, current.Revision,
		)
	}
	return etcdstore.Versioned[Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func componentWriteConditions(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record Record,
	current *etcdstore.Versioned[Record],
	ownerRevision int64,
	kindRevision int64,
) []etcdstore.Condition {
	primary := etcdstore.Condition{Key: RecordKey(record.Desired.ID)}
	owner := etcdstore.Condition{Key: EnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID)}
	kind := etcdstore.Condition{Key: EnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind)}
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
		{Key: deletions.TombstoneKey("component", record.Desired.ID)},
		{Key: deletions.TombstoneKey("environment", environment.Record.ID)},
		{Key: deletions.TombstoneKey("project", project.Record.ID)},
		{Key: deletions.TombstoneKey("tenant", project.Record.TenantID)},
	}
}

func validateEnvironmentComponentHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record Record,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return err
	}
	if err := ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.Record.ProjectID != project.Record.ID ||
		record.Desired.Owner != core.ComponentOwnerEnvironment || record.Desired.OwnerID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Component hierarchy ownership or revisions are invalid")
	}
	return nil
}

func ValidateComponentVersion(current etcdstore.Versioned[Record]) error {
	if err := ValidateRecord(current.Record); err != nil {
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
	record Record,
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
			return recordcodec.StateConflict("component", componentID)
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
		return recordcodec.StateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return recordcodec.StateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7, 8} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Component hierarchy deletion is in progress")
		}
	}
	return recordcodec.StateConflict("component", componentID)
}
