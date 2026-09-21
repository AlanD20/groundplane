package connectormutations

import (
	"context"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) LoadConnectorMutationFence(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	domainKeys []string,
) (environmentfence.Evidence, *etcdstore.GetManyResult, error) {
	keys := append([]string(nil), domainKeys...)
	environmentIndex := len(keys)
	keys = append(keys, hierarchyrecord.EnvironmentKey(environment.Record.ID))
	projectIndex := len(keys)
	keys = append(keys, hierarchyrecord.ProjectKey(project.Record.ID))
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return environmentfence.Evidence{}, nil, err
	}
	if result == nil || result.ReadRevision <= 0 || len(result.Values) != len(keys) {
		return environmentfence.Evidence{}, nil, errs.New(
			errs.KindInternal,
			"Connector mutation fixed-revision evidence is incomplete",
		)
	}
	for index, value := range result.Values {
		if value != nil && value.Key != keys[index] {
			etcdstore.ClearValues(result.Values)
			return environmentfence.Evidence{}, nil, errs.New(
				errs.KindInternal,
				"Connector mutation fixed-revision evidence is corrupt",
			)
		}
	}
	if result.Values[environmentIndex] == nil {
		etcdstore.ClearValues(result.Values)
		return environmentfence.Evidence{}, nil, errs.New(
			errs.KindEnvironmentNotFound,
			"Connector Environment was not found",
		)
	}
	if result.Values[environmentIndex].ModRevision != environment.Revision {
		etcdstore.ClearValues(result.Values)
		return environmentfence.Evidence{}, nil, recordcodec.StateConflict(
			"Connector Environment",
			environment.Record.ID,
		)
	}
	if result.Values[projectIndex] == nil {
		etcdstore.ClearValues(result.Values)
		return environmentfence.Evidence{}, nil, errs.New(
			errs.KindProjectNotFound,
			"Connector Project was not found",
		)
	}
	if result.Values[projectIndex].ModRevision != project.Revision {
		etcdstore.ClearValues(result.Values)
		return environmentfence.Evidence{}, nil, recordcodec.StateConflict(
			"Connector Project",
			project.Record.ID,
		)
	}
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		environment.Record.ID,
		result.ReadRevision,
	)
	if err != nil {
		etcdstore.ClearValues(result.Values)
		return environmentfence.Evidence{}, nil, err
	}
	return fence, result, nil
}

func ValidateConnectorHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record connectorrecord.Record,
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
	if err := connectorrecord.ValidateRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision <= 0 || project.Revision <= 0 ||
		project.ReadRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Connector owner revisions are invalid")
	}
	if record.Connector.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(
			errs.KindConnectorScopeInvalid,
			"Connector must have exactly one Environment owner",
		)
	}
	return nil
}

func ClassifyConnectorCreateConflict(
	reads []*etcdstore.KeyValue,
	fence environmentfence.Evidence,
	secretConditionCount int,
) error {
	want := 5 + fence.ConditionCount() + secretConditionCount
	if len(reads) != want {
		return errs.New(errs.KindInternal, "Connector create conflict read is incomplete")
	}
	if reads[0] != nil || reads[2] != nil || reads[3] != nil {
		return errs.New(errs.KindStateConflict, "Connector stable identity already exists")
	}
	if reads[1] != nil {
		return errs.New(errs.KindStateConflict, "Connector name already exists in the Environment")
	}
	if reads[4] != nil {
		return errs.New(errs.KindResourceInUse, "Connector deletion is in progress")
	}
	if conflict := fence.ClassifyConflict(reads[5 : 5+fence.ConditionCount()]); conflict != nil {
		return conflict
	}
	if secretConditionCount > 0 {
		return errs.New(errs.KindStateConflict, "Connector credential Secret changed during creation")
	}
	return errs.New(errs.KindStateConflict, "Connector owner changed or is being deleted")
}
