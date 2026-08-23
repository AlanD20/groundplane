package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptRepository owns Script records, Environment membership, name uniqueness, and CAS updates.
type ScriptRepository struct{ store hierarchyStore }

func NewScriptRepository(store Store) (*ScriptRepository, error) { return newScriptRepository(store) }

func newScriptRepository(store hierarchyStore) (*ScriptRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Script store is required")
	}
	return &ScriptRepository{store: store}, nil
}

func (repository *ScriptRepository) CreateScript(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
) (Versioned[ScriptRecord], error) {
	conditions, mutations, classify, err := repository.prepareScriptCreation(ctx, environment, project, target, record)
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ScriptRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[ScriptRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ScriptRepository) CreateScriptIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateScriptMutationMarker(marker, record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := repository.prepareScriptCreation(ctx, environment, project, target, record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ScriptRepository) prepareScriptCreation(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if err := validateScriptHierarchy(ctx, environment, project, target, record); err != nil {
		return nil, nil, nil, err
	}
	value, err := encodeScriptRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := scriptWriteConditions(environment, project, target, record, nil, 0, 0)
	mutations := []Mutation{
		{Type: MutationPut, Key: scriptKey(record.Desired.ID), Value: value},
		{
			Type:  MutationPut,
			Key:   scriptOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  MutationPut,
			Key:   scriptNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
	}
	classify := func(_ int64, values []*KeyValue) error {
		return classifyScriptWriteConflict(values, environment, project, target, record, 0)
	}
	return conditions, mutations, classify, nil
}

func (repository *ScriptRepository) GetScript(ctx context.Context, id string) (Versioned[ScriptRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if err := validateID(ids.KindScript, id); err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	return getRecord(
		ctx, repository.store, scriptKey(id), id, errs.KindScriptNotFound, decodeScriptRecord,
		func(record ScriptRecord) string { return record.Desired.ID },
	)
}

func (repository *ScriptRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ScriptRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ScriptRecord]{}, err
	}
	return listIndexPage(
		ctx, repository.store, "scripts", "environment", environmentID,
		scriptOwnerPrefix(environmentID), scriptKey, ids.KindScript, request, decodeScriptRecord,
		func(record ScriptRecord) string { return record.Desired.ID },
		func(record ScriptRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *ScriptRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[ScriptRecord],
	desired core.Script,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateScriptMutationMarker(marker, current.Record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	_, conditions, mutations, classify, err := repository.prepareScriptReplacement(
		ctx, environment, project, target, current, desired,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ScriptRepository) prepareScriptReplacement(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[ScriptRecord],
	desired core.Script,
) (ScriptRecord, []Condition, []Mutation, idempotencyPlanClassifier, error) {
	replacement, err := ReplaceScriptDesired(current.Record, desired)
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	if err := validateScriptHierarchy(ctx, environment, project, target, replacement); err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	if err := validateScriptVersion(current); err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			scriptOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
			scriptNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return ScriptRecord{}, nil, nil, nil, errs.New(errs.KindInternal, "Script indexes are missing or corrupt")
	}
	value, err := encodeScriptRecord(replacement)
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	conditions := scriptWriteConditions(
		environment, project, target, current.Record, &current,
		indexes.Values[0].ModRevision, indexes.Values[1].ModRevision,
	)
	mutations := []Mutation{{Type: MutationPut, Key: scriptKey(replacement.Desired.ID), Value: value}}
	classify := func(_ int64, values []*KeyValue) error {
		return classifyScriptWriteConflict(values, environment, project, target, current.Record, current.Revision)
	}
	return replacement, conditions, mutations, classify, nil
}

func validateScriptMutationMarker(marker IdempotencyMarker, environmentID string) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Script mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return validateIdempotencyMarker(marker)
}

func scriptWriteConditions(
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
	current *Versioned[ScriptRecord],
	ownerRevision int64,
	nameRevision int64,
) []Condition {
	scriptCondition := Condition{Key: scriptKey(record.Desired.ID)}
	ownerCondition := Condition{Key: scriptOwnerKey(record.EnvironmentID, record.Desired.ID)}
	nameCondition := Condition{Key: scriptNameKey(record.EnvironmentID, record.Desired.Name)}
	if current != nil {
		scriptCondition.ModRevision = current.Revision
		ownerCondition.ModRevision = ownerRevision
		nameCondition.ModRevision = nameRevision
	}
	conditions := []Condition{
		scriptCondition,
		ownerCondition,
		nameCondition,
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: serviceKey(target.Record.Desired.ID), ModRevision: target.Revision},
		{Key: deletionTombstoneKey("script", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func validateScriptHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
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
	if err := validateServiceVersion(target); err != nil {
		return err
	}
	if err := validateScriptRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID || target.Record.EnvironmentID != environment.Record.ID ||
		target.Record.Desired.ID != record.ServiceID || target.Record.Desired.Name != record.Desired.ServiceName {
		return errs.New(errs.KindValidationFailed, "Script hierarchy or target Service is invalid")
	}
	return nil
}

func validateScriptVersion(current Versioned[ScriptRecord]) error {
	if err := validateScriptRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Script version metadata is invalid")
	}
	return nil
}

func classifyScriptWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
	expectedScriptRevision int64,
) error {
	expected := 10
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Script write compare evidence is incomplete")
	}
	if expectedScriptRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Script stable identity is already in use")
		}
		if values[2] != nil {
			return errs.New(errs.KindNameConflict, "Script name is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindScriptNotFound, "Script was not found")
		}
		if values[0].ModRevision != expectedScriptRevision {
			return stateConflict("script", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Script index changed or is corrupt")
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
	if values[5] == nil {
		return errs.New(errs.KindServiceNotFound, "Script target Service was not found")
	}
	if values[5].ModRevision != target.Revision {
		return stateConflict("service", target.Record.Desired.ID)
	}
	for _, index := range []int{6, 7, 8, 9} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Script hierarchy or target deletion is in progress")
		}
	}
	if expected == 11 && values[10] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("script", record.Desired.ID)
}
