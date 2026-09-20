package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ScriptRepository owns Script records, Environment membership, slug uniqueness, and CAS updates.
type ScriptRepository struct{ store hierarchyStore }

func NewScriptRepository(store etcdstore.Store) (*ScriptRepository, error) {
	return newScriptRepository(store)
}

func newScriptRepository(store hierarchyStore) (*ScriptRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Script store is required")
	}
	return &ScriptRepository{store: store}, nil
}

func (repository *ScriptRepository) CreateScript(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
) (etcdstore.Versioned[scriptrecord.Record], error) {
	conditions, mutations, classify, err := repository.prepareScriptCreation(ctx, environment, project, target, record)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if !result.Succeeded {
		return etcdstore.Versioned[scriptrecord.Record]{}, classify(result.Revision, result.FailureReads)
	}
	created, err := readActiveScriptStorage(ctx, repository.store, record.Desired.ID, result.Revision)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	return repository.hydrateScriptBody(ctx, created.Script)
}

func (repository *ScriptRepository) CreateScriptIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
	marker idempotencyrecord.IdempotencyMarker,
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
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
) ([]etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	if err := validateScriptHierarchy(ctx, environment, project, target, record); err != nil {
		return nil, nil, nil, err
	}
	active, err := readActiveScriptSet(ctx, repository.store, record.EnvironmentID, environment.ReadRevision)
	if err != nil {
		return nil, nil, nil, err
	}
	record.ScriptSetGeneration = active.Record.GenerationID
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: scriptrecord.ScriptSetOwnerPrefix(
			record.EnvironmentID,
			active.Record.GenerationID,
		), Limit: 65, Revision: active.ReadRevision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if page == nil || page.ReadRevision != active.ReadRevision || len(page.Values) >= 64 {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Environment exceeds the 64 Script limit")
	}
	value, err := scriptrecord.EncodeRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	bodyGeneration, err := scriptrecord.NewScriptBodyGeneration(record)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	bodyValue, err := scriptrecord.EncodeScriptBodyGeneration(bodyGeneration)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	locatorValue, err := scriptrecord.EncodeScriptLocator(
		scriptrecord.LocatorRecord{ScriptID: record.Desired.ID, EnvironmentID: record.EnvironmentID},
	)
	if err != nil {
		clear(value)
		clear(bodyValue)
		return nil, nil, nil, err
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		clear(bodyValue)
		clear(locatorValue)
		return nil, nil, nil, err
	}
	conditions := scriptWriteConditions(environment, project, target, record, nil, active, 0, 0)
	conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptSetBodyGenerationKey(
		record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID, record.ActiveGeneration,
	)})
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: value,
		},
		{
			Type: etcdstore.MutationPut,
			Key: scriptrecord.ScriptSetBodyGenerationKey(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				record.Desired.ID,
				record.ActiveGeneration,
			),
			Value: bodyValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptLocatorKey(record.Desired.ID), Value: locatorValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyScriptWriteConflict(
			values,
			environment,
			project,
			target,
			record,
			0,
			scriptWriteConflictExtras{bodyGeneration: true},
		)
	}
	return conditions, mutations, classify, nil
}

func (repository *ScriptRepository) GetScript(ctx context.Context, id string) (etcdstore.Versioned[scriptrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindScript, id); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	locatorRead, err := repository.store.Get(ctx, scriptrecord.ScriptLocatorKey(id))
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if locatorRead == nil || locatorRead.Entry == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := scriptrecord.DecodeScriptLocator(locatorRead.Entry.Value)
	if err != nil || locator.ScriptID != id {
		return etcdstore.Versioned[scriptrecord.Record]{}, recordcodec.CorruptRecord()
	}
	active, err := readActiveScriptSet(ctx, repository.store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptrecord.ScriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, id),
		}, Revision: active.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := scriptrecord.DecodeRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != id || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return etcdstore.Versioned[scriptrecord.Record]{}, recordcodec.CorruptRecord()
	}
	return repository.hydrateScriptBody(ctx, etcdstore.Versioned[scriptrecord.Record]{
		Record: record, Revision: primary.Values[0].ModRevision, ReadRevision: active.ReadRevision,
	})
}

func (repository *ScriptRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[scriptrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	revision := int64(0)
	if request.Cursor != "" {
		cursor, cursorErr := recordcodec.DecodeCursor(request.Cursor)
		if cursorErr != nil {
			return etcdstore.Page[scriptrecord.Record]{}, cursorErr
		}
		revision = cursor.Revision
	}
	active, err := readActiveScriptSet(ctx, repository.store, environmentID, revision)
	if err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	page, err := listIndexPageAtRevision(
		ctx, repository.store, "scripts", "environment", environmentID,
		scriptrecord.ScriptSetOwnerPrefix(environmentID, active.Record.GenerationID),
		func(id string) string {
			return scriptrecord.ScriptSetScriptKey(environmentID, active.Record.GenerationID, id)
		},
		ids.KindScript, request, scriptrecord.DecodeRecord,
		func(record scriptrecord.Record) string { return record.Desired.ID },
		func(record scriptrecord.Record) bool { return record.EnvironmentID == environmentID }, active.ReadRevision,
	)
	if err != nil {
		return etcdstore.Page[scriptrecord.Record]{}, err
	}
	for index := range page.Items {
		hydrated, hydrateErr := repository.hydrateScriptBody(ctx, page.Items[index])
		if hydrateErr != nil {
			return etcdstore.Page[scriptrecord.Record]{}, hydrateErr
		}
		page.Items[index] = hydrated
	}
	return page, nil
}

func (repository *ScriptRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	desired core.Script,
	marker idempotencyrecord.IdempotencyMarker,
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
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	current etcdstore.Versioned[scriptrecord.Record],
	desired core.Script,
) (scriptrecord.Record, []etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
	replacement, err := scriptrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if err := validateScriptHierarchy(ctx, environment, project, target, replacement); err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if err := validateScriptVersion(current); err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	active, err := readActiveScriptSet(ctx, repository.store, current.Record.EnvironmentID, current.ReadRevision)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if current.Record.ScriptSetGeneration != active.Record.GenerationID {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
	replacement.ScriptSetGeneration = active.Record.GenerationID
	indexKeys := []string{
		scriptrecord.ScriptSetOwnerKey(current.Record.EnvironmentID, active.Record.GenerationID, current.Record.Desired.ID),
		scriptrecord.ScriptSetSlugKey(current.Record.EnvironmentID, active.Record.GenerationID, current.Record.Desired.Slug),
	}
	slugChanged := replacement.Desired.Slug != current.Record.Desired.Slug
	if slugChanged {
		indexKeys = append(
			indexKeys,
			scriptrecord.ScriptSetSlugKey(current.Record.EnvironmentID, active.Record.GenerationID, replacement.Desired.Slug),
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     indexKeys,
		Revision: current.ReadRevision,
	})
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != len(indexKeys) || indexes.Values[0] == nil ||
		indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(errs.KindInternal, "Script indexes are missing or corrupt")
	}
	if slugChanged && indexes.Values[2] != nil {
		return scriptrecord.Record{}, nil, nil, nil, errs.New(errs.KindNameConflict, "Script slug is already in use")
	}
	value, err := scriptrecord.EncodeRecord(replacement)
	if err != nil {
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	conditions := scriptWriteConditions(
		environment, project, target, current.Record, &current, active,
		indexes.Values[0].ModRevision, indexes.Values[1].ModRevision,
	)
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		return scriptrecord.Record{}, nil, nil, nil, err
	}
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptrecord.ScriptSetScriptKey(replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID),
			Value: value,
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(replacement.EnvironmentID), Value: activeValue},
	}
	extras := scriptWriteConflictExtras{}
	if slugChanged {
		extras.newSlug = replacement.Desired.Slug
		conditions = append(
			conditions,
			etcdstore.Condition{Key: scriptrecord.ScriptSetSlugKey(replacement.EnvironmentID, active.Record.GenerationID, extras.newSlug)},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key: scriptrecord.ScriptSetSlugKey(
					current.Record.EnvironmentID,
					active.Record.GenerationID,
					current.Record.Desired.Slug,
				),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptrecord.ScriptSetSlugKey(replacement.EnvironmentID, active.Record.GenerationID, extras.newSlug),
				Value: []byte(replacement.Desired.ID),
			},
		)
	}
	if replacement.ActiveGeneration != current.Record.ActiveGeneration {
		generation, generationErr := scriptrecord.NewScriptBodyGeneration(replacement)
		if generationErr != nil {
			clearMutationValues(mutations)
			return scriptrecord.Record{}, nil, nil, nil, generationErr
		}
		generationValue, generationErr := scriptrecord.EncodeScriptBodyGeneration(generation)
		if generationErr != nil {
			clearMutationValues(mutations)
			return scriptrecord.Record{}, nil, nil, nil, generationErr
		}
		extras.bodyGeneration = true
		conditions = append(conditions, etcdstore.Condition{Key: scriptrecord.ScriptSetBodyGenerationKey(
			replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration,
		)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetBodyGenerationKey(replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration),
			Value: generationValue,
		})
	}
	classify := func(_ int64, values []*etcdstore.KeyValue) error {
		return classifyScriptWriteConflict(
			values,
			environment,
			project,
			target,
			current.Record,
			current.Revision,
			extras,
		)
	}
	return replacement, conditions, mutations, classify, nil
}

func validateScriptMutationMarker(marker idempotencyrecord.IdempotencyMarker, environmentID string) error {
	if marker.Kind != idempotencyrecord.IdempotencyMarkerDirect || marker.State != idempotencyrecord.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Script mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func scriptWriteConditions(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
	current *etcdstore.Versioned[scriptrecord.Record],
	active etcdstore.Versioned[scriptrecord.SetGenerationRecord],
	ownerRevision int64,
	slugRevision int64,
) []etcdstore.Condition {
	scriptCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetScriptKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	ownerCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	slugCondition := etcdstore.Condition{
		Key: scriptrecord.ScriptSetSlugKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.Slug),
	}
	if current != nil {
		scriptCondition.ModRevision = current.Revision
		ownerCondition.ModRevision = ownerRevision
		slugCondition.ModRevision = slugRevision
	}
	conditions := []etcdstore.Condition{
		scriptCondition,
		ownerCondition,
		slugCondition,
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		serviceDesiredCondition(target),
		{Key: deletionTombstoneKey("script", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("service", target.Record.Desired.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: active.Revision},
	}
	if current == nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: scriptrecord.ScriptLocatorKey(record.Desired.ID)},
			etcdstore.Condition{Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID)},
		)
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func validateScriptHierarchy(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
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
	if err := validateServiceVersion(target); err != nil {
		return err
	}
	if target.Record.Desired.Replicas < 1 {
		return errs.New(errs.KindValidationFailed, "Script target Service must have positive replicas")
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || target.Record.Desired.Adapter != "" ||
		target.Record.BackingNetworkID != "" {
		return errs.New(errs.KindValidationFailed, "Script target must be an operator-owned Service")
	}
	if err := scriptrecord.ValidateRecord(record); err != nil {
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

func validateScriptVersion(current etcdstore.Versioned[scriptrecord.Record]) error {
	if err := scriptrecord.ValidateRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Script version metadata is invalid")
	}
	return nil
}

func classifyScriptWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[ServiceRecord],
	record scriptrecord.Record,
	expectedScriptRevision int64,
	extras scriptWriteConflictExtras,
) error {
	expected := 11
	if expectedScriptRevision == 0 {
		expected++
	}
	hasTenant := project.Record.TenantID != ""
	if hasTenant {
		expected++
	}
	if extras.newSlug != "" {
		expected++
	}
	if extras.bodyGeneration {
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
			return errs.New(errs.KindNameConflict, "Script slug is already in use")
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
	extraIndex := 10
	if values[extraIndex] == nil {
		return errs.New(errs.KindInternal, "Environment active Script-set generation is missing")
	}
	extraIndex++
	if expectedScriptRevision == 0 {
		if values[extraIndex] != nil {
			return errs.New(errs.KindStateConflict, "Script stable identity is already in use")
		}
		extraIndex++
	}
	if hasTenant && values[extraIndex] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	if hasTenant {
		extraIndex++
	}
	if extras.newSlug != "" {
		if values[extraIndex] != nil {
			return errs.New(errs.KindNameConflict, "Script slug is already in use")
		}
		extraIndex++
	}
	if extras.bodyGeneration && values[extraIndex] != nil {
		return errs.New(errs.KindStateConflict, "Script body generation is already in use")
	}
	return stateConflict("script", record.Desired.ID)
}

type scriptWriteConflictExtras struct {
	newSlug        string
	bodyGeneration bool
}

func (repository *ScriptRepository) hydrateScriptBody(
	ctx context.Context,
	record etcdstore.Versioned[scriptrecord.Record],
) (etcdstore.Versioned[scriptrecord.Record], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{scriptrecord.ScriptSetBodyGenerationKey(
			record.Record.EnvironmentID, record.Record.ScriptSetGeneration,
			record.Record.Desired.ID, record.Record.ActiveGeneration,
		)},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script body generation is missing")
	}
	defer clear(result.Values[0].Value)
	generation, err := scriptrecord.DecodeScriptBodyGeneration(result.Values[0].Value)
	if err != nil || generation.ScriptID != record.Record.Desired.ID ||
		generation.Generation != record.Record.ActiveGeneration {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script body generation is corrupt")
	}
	record.Record.Desired.Body = generation.Body
	if err := scriptrecord.ValidateRecord(record.Record); err != nil {
		return etcdstore.Versioned[scriptrecord.Record]{}, errs.New(errs.KindInternal, "Script aggregate is corrupt")
	}
	return record, nil
}
