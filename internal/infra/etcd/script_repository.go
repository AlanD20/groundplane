package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

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
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
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
	created, err := readActiveScriptStorage(ctx, repository.store, record.Desired.ID, result.Revision)
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	return repository.hydrateScriptBody(ctx, created.Script)
}

func (repository *ScriptRepository) CreateScriptIdempotent(
	ctx context.Context,
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
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
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
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
		Prefix: scriptSetOwnerPrefix(
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
	value, err := encodeScriptRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	bodyGeneration, err := newScriptBodyGeneration(record)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	bodyValue, err := encodeScriptBodyGeneration(bodyGeneration)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	locatorValue, err := encodeScriptLocator(
		scriptLocatorRecord{ScriptID: record.Desired.ID, EnvironmentID: record.EnvironmentID},
	)
	if err != nil {
		clear(value)
		clear(bodyValue)
		return nil, nil, nil, err
	}
	activeValue, err := encodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		clear(bodyValue)
		clear(locatorValue)
		return nil, nil, nil, err
	}
	conditions := scriptWriteConditions(environment, project, target, record, nil, active, 0, 0)
	conditions = append(conditions, etcdstore.Condition{Key: scriptSetBodyGenerationKey(
		record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID, record.ActiveGeneration,
	)})
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: value,
		},
		{
			Type: etcdstore.MutationPut,
			Key: scriptSetBodyGenerationKey(
				record.EnvironmentID,
				record.ScriptSetGeneration,
				record.Desired.ID,
				record.ActiveGeneration,
			),
			Value: bodyValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptLocatorKey(record.Desired.ID), Value: locatorValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
		{Type: etcdstore.MutationPut, Key: scriptSetActiveKey(record.EnvironmentID), Value: activeValue},
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

func (repository *ScriptRepository) GetScript(ctx context.Context, id string) (Versioned[ScriptRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindScript, id); err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	locatorRead, err := repository.store.Get(ctx, scriptLocatorKey(id))
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if locatorRead == nil || locatorRead.Entry == nil {
		return Versioned[ScriptRecord]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	locator, err := decodeScriptLocator(locatorRead.Entry.Value)
	if err != nil || locator.ScriptID != id {
		return Versioned[ScriptRecord]{}, recordcodec.CorruptRecord()
	}
	active, err := readActiveScriptSet(ctx, repository.store, locator.EnvironmentID, locatorRead.ReadRevision)
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	primary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			scriptSetScriptKey(locator.EnvironmentID, active.Record.GenerationID, id),
		}, Revision: active.ReadRevision,
	})
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if primary == nil || len(primary.Values) != 1 || primary.Values[0] == nil {
		return Versioned[ScriptRecord]{}, errs.New(errs.KindScriptNotFound, "Script was not found")
	}
	record, err := decodeScriptRecord(primary.Values[0].Value)
	if err != nil || record.Desired.ID != id || record.EnvironmentID != locator.EnvironmentID ||
		record.ScriptSetGeneration != active.Record.GenerationID {
		return Versioned[ScriptRecord]{}, recordcodec.CorruptRecord()
	}
	return repository.hydrateScriptBody(ctx, Versioned[ScriptRecord]{
		Record: record, Revision: primary.Values[0].ModRevision, ReadRevision: active.ReadRevision,
	})
}

func (repository *ScriptRepository) ListScripts(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ScriptRecord], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ScriptRecord]{}, err
	}
	revision := int64(0)
	if request.Cursor != "" {
		cursor, cursorErr := recordcodec.DecodeCursor(request.Cursor)
		if cursorErr != nil {
			return Page[ScriptRecord]{}, cursorErr
		}
		revision = cursor.Revision
	}
	active, err := readActiveScriptSet(ctx, repository.store, environmentID, revision)
	if err != nil {
		return Page[ScriptRecord]{}, err
	}
	page, err := listIndexPageAtRevision(
		ctx, repository.store, "scripts", "environment", environmentID,
		scriptSetOwnerPrefix(environmentID, active.Record.GenerationID),
		func(id string) string { return scriptSetScriptKey(environmentID, active.Record.GenerationID, id) },
		ids.KindScript, request, decodeScriptRecord,
		func(record ScriptRecord) string { return record.Desired.ID },
		func(record ScriptRecord) bool { return record.EnvironmentID == environmentID }, active.ReadRevision,
	)
	if err != nil {
		return Page[ScriptRecord]{}, err
	}
	for index := range page.Items {
		hydrated, hydrateErr := repository.hydrateScriptBody(ctx, page.Items[index])
		if hydrateErr != nil {
			return Page[ScriptRecord]{}, hydrateErr
		}
		page.Items[index] = hydrated
	}
	return page, nil
}

func (repository *ScriptRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
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
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	target Versioned[ServiceRecord],
	current Versioned[ScriptRecord],
	desired core.Script,
) (ScriptRecord, []etcdstore.Condition, []etcdstore.Mutation, idempotencyPlanClassifier, error) {
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
	active, err := readActiveScriptSet(ctx, repository.store, current.Record.EnvironmentID, current.ReadRevision)
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	if current.Record.ScriptSetGeneration != active.Record.GenerationID {
		return ScriptRecord{}, nil, nil, nil, errs.New(errs.KindStateConflict, "Script-set generation changed")
	}
	replacement.ScriptSetGeneration = active.Record.GenerationID
	indexKeys := []string{
		scriptSetOwnerKey(current.Record.EnvironmentID, active.Record.GenerationID, current.Record.Desired.ID),
		scriptSetSlugKey(current.Record.EnvironmentID, active.Record.GenerationID, current.Record.Desired.Slug),
	}
	slugChanged := replacement.Desired.Slug != current.Record.Desired.Slug
	if slugChanged {
		indexKeys = append(
			indexKeys,
			scriptSetSlugKey(current.Record.EnvironmentID, active.Record.GenerationID, replacement.Desired.Slug),
		)
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     indexKeys,
		Revision: current.ReadRevision,
	})
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != len(indexKeys) || indexes.Values[0] == nil ||
		indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return ScriptRecord{}, nil, nil, nil, errs.New(errs.KindInternal, "Script indexes are missing or corrupt")
	}
	if slugChanged && indexes.Values[2] != nil {
		return ScriptRecord{}, nil, nil, nil, errs.New(errs.KindNameConflict, "Script slug is already in use")
	}
	value, err := encodeScriptRecord(replacement)
	if err != nil {
		return ScriptRecord{}, nil, nil, nil, err
	}
	conditions := scriptWriteConditions(
		environment, project, target, current.Record, &current, active,
		indexes.Values[0].ModRevision, indexes.Values[1].ModRevision,
	)
	activeValue, err := encodeScriptSetGeneration(active.Record)
	if err != nil {
		clear(value)
		return ScriptRecord{}, nil, nil, nil, err
	}
	mutations := []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   scriptSetScriptKey(replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID),
			Value: value,
		},
		{Type: etcdstore.MutationPut, Key: scriptSetActiveKey(replacement.EnvironmentID), Value: activeValue},
	}
	extras := scriptWriteConflictExtras{}
	if slugChanged {
		extras.newSlug = replacement.Desired.Slug
		conditions = append(
			conditions,
			etcdstore.Condition{Key: scriptSetSlugKey(replacement.EnvironmentID, active.Record.GenerationID, extras.newSlug)},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type: etcdstore.MutationDelete,
				Key: scriptSetSlugKey(
					current.Record.EnvironmentID,
					active.Record.GenerationID,
					current.Record.Desired.Slug,
				),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   scriptSetSlugKey(replacement.EnvironmentID, active.Record.GenerationID, extras.newSlug),
				Value: []byte(replacement.Desired.ID),
			},
		)
	}
	if replacement.ActiveGeneration != current.Record.ActiveGeneration {
		generation, generationErr := newScriptBodyGeneration(replacement)
		if generationErr != nil {
			clearMutationValues(mutations)
			return ScriptRecord{}, nil, nil, nil, generationErr
		}
		generationValue, generationErr := encodeScriptBodyGeneration(generation)
		if generationErr != nil {
			clearMutationValues(mutations)
			return ScriptRecord{}, nil, nil, nil, generationErr
		}
		extras.bodyGeneration = true
		conditions = append(conditions, etcdstore.Condition{Key: scriptSetBodyGenerationKey(
			replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration,
		)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: scriptSetBodyGenerationKey(replacement.EnvironmentID, active.Record.GenerationID, replacement.Desired.ID, replacement.ActiveGeneration),
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
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
	current *Versioned[ScriptRecord],
	active Versioned[ScriptSetGenerationRecord],
	ownerRevision int64,
	slugRevision int64,
) []etcdstore.Condition {
	scriptCondition := etcdstore.Condition{
		Key: scriptSetScriptKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	ownerCondition := etcdstore.Condition{
		Key: scriptSetOwnerKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.ID),
	}
	slugCondition := etcdstore.Condition{
		Key: scriptSetSlugKey(record.EnvironmentID, active.Record.GenerationID, record.Desired.Slug),
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
		{Key: scriptSetActiveKey(record.EnvironmentID), ModRevision: active.Revision},
	}
	if current == nil {
		conditions = append(conditions,
			etcdstore.Condition{Key: scriptLocatorKey(record.Desired.ID)},
			etcdstore.Condition{Key: scriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID)},
		)
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	return conditions
}

func validateScriptHierarchy(
	ctx context.Context,
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
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
	values []*etcdstore.KeyValue,
	environment Versioned[hierarchyrecord.EnvironmentRecord],
	project Versioned[hierarchyrecord.ProjectRecord],
	target Versioned[ServiceRecord],
	record ScriptRecord,
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
	record Versioned[ScriptRecord],
) (Versioned[ScriptRecord], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{scriptSetBodyGenerationKey(
			record.Record.EnvironmentID, record.Record.ScriptSetGeneration,
			record.Record.Desired.ID, record.Record.ActiveGeneration,
		)},
		Revision: record.ReadRevision,
	})
	if err != nil {
		return Versioned[ScriptRecord]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return Versioned[ScriptRecord]{}, errs.New(errs.KindInternal, "Script body generation is missing")
	}
	defer clear(result.Values[0].Value)
	generation, err := decodeScriptBodyGeneration(result.Values[0].Value)
	if err != nil || generation.ScriptID != record.Record.Desired.ID ||
		generation.Generation != record.Record.ActiveGeneration {
		return Versioned[ScriptRecord]{}, errs.New(errs.KindInternal, "Script body generation is corrupt")
	}
	record.Record.Desired.Body = generation.Body
	if err := validateScriptRecord(record.Record); err != nil {
		return Versioned[ScriptRecord]{}, errs.New(errs.KindInternal, "Script aggregate is corrupt")
	}
	return record, nil
}
