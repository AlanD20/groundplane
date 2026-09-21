package etcd

import (
	"bytes"
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const scriptBlueprintStageBatchScripts = 7

// BlueprintScriptPublication is the bounded final active-generation flip for
// one fully staged Script set. Script and body bytes never enter the owning
// Environment desired-state transaction.
type BlueprintScriptPublication struct {
	environmentID string
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
}

func (publication BlueprintScriptPublication) validate(
	environmentID string,
	source blueprints.EnvironmentBlueprintSourceKind,
) error {
	if publication.IsZero() {
		return nil
	}
	if source != blueprints.EnvironmentBlueprintSourceApply || publication.environmentID != environmentID ||
		len(publication.conditions) != 1 || len(publication.mutations) != 1 {
		return errs.New(errs.KindValidationFailed, "Blueprint Script publication is invalid")
	}
	return nil
}

func (publication BlueprintScriptPublication) classify(values []*etcdstore.KeyValue) error {
	if len(values) != 1 || conditionMatchesRead(publication.conditions[0], values[0]) {
		return errs.New(errs.KindInternal, "Blueprint Script compare evidence is invalid")
	}
	return errs.New(errs.KindStateConflict, "active Script-set generation changed")
}

func (publication BlueprintScriptPublication) IsZero() bool {
	return publication.environmentID == "" && len(publication.conditions) == 0 && len(publication.mutations) == 0
}

func (publication *BlueprintScriptPublication) Clear() {
	if publication == nil {
		return
	}
	clearMutationValues(publication.mutations)
	publication.environmentID = ""
	publication.conditions = nil
	publication.mutations = nil
}

// PrepareBlueprintScriptPublication stages one complete, invisible next
// Script-set generation in deterministic batches. Each batch contains at most
// seven Scripts, hence at most fourteen Script/body records and no more than
// the production Store transaction-operation ceiling.
func (repository *ScriptRepository) PrepareBlueprintScriptPublication(
	ctx context.Context,
	environmentID string,
	readRevision int64,
	nextGenerationID string,
	current []etcdstore.Versioned[scriptrecord.Record],
	desired []scriptrecord.Record,
	generations []scriptrecord.BodyGenerationRecord,
) (BlueprintScriptPublication, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return BlueprintScriptPublication{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil || readRevision <= 0 {
		return BlueprintScriptPublication{}, errs.New(errs.KindValidationFailed, "Blueprint Script snapshot is invalid")
	}
	next := scriptrecord.SetGenerationRecord{EnvironmentID: environmentID, GenerationID: nextGenerationID}
	if err := scriptrecord.ValidateScriptSetGeneration(next); err != nil {
		return BlueprintScriptPublication{}, err
	}
	if len(current) > 64 || len(desired) > 64 {
		return BlueprintScriptPublication{}, errs.New(
			errs.KindValidationFailed,
			"Environment exceeds the 64 Script limit",
		)
	}
	active, err := readActiveScriptSet(ctx, repository.store, environmentID, readRevision)
	if err != nil {
		return BlueprintScriptPublication{}, err
	}
	if active.Record.GenerationID == nextGenerationID {
		return BlueprintScriptPublication{}, errs.New(
			errs.KindStateConflict,
			"next Script-set generation is already active",
		)
	}

	currentByID := make(map[string]etcdstore.Versioned[scriptrecord.Record], len(current))
	for _, versioned := range current {
		if versioned.ReadRevision != readRevision || versioned.Record.EnvironmentID != environmentID ||
			versioned.Record.ScriptSetGeneration != active.Record.GenerationID {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindInternal,
				"Blueprint Script snapshot is inconsistent",
			)
		}
		if err := validateScriptVersion(versioned); err != nil {
			return BlueprintScriptPublication{}, err
		}
		if versioned.Record.ActiveReferences != 0 {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindStateConflict,
				"active Script executions fence Blueprint publication",
			)
		}
		if _, duplicate := currentByID[versioned.Record.Desired.ID]; duplicate {
			return BlueprintScriptPublication{}, errs.New(errs.KindInternal, "Blueprint Script snapshot repeats an id")
		}
		currentByID[versioned.Record.Desired.ID] = versioned
	}

	ordered := append([]scriptrecord.Record(nil), desired...)
	sort.Slice(ordered, func(left, right int) bool { return ordered[left].Desired.ID < ordered[right].Desired.ID })
	desiredByID := make(map[string]struct{}, len(ordered))
	for index := range ordered {
		record := &ordered[index]
		if err := scriptrecord.ValidateRecord(*record); err != nil || record.EnvironmentID != environmentID {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script publication record is invalid",
			)
		}
		if record.ActiveReferences != 0 {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script publication cannot synthesize active references",
			)
		}
		if _, duplicate := desiredByID[record.Desired.ID]; duplicate {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Script publication repeats an id",
			)
		}
		desiredByID[record.Desired.ID] = struct{}{}
		record.ScriptSetGeneration = nextGenerationID
	}
	for id := range currentByID {
		if _, retained := desiredByID[id]; !retained {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Script projection omits durable state",
			)
		}
	}
	if err := validateBlueprintBodyGenerationInputs(currentByID, ordered, generations); err != nil {
		return BlueprintScriptPublication{}, err
	}

	for start := 0; start < len(ordered); {
		staged := false
		for end := min(start+scriptBlueprintStageBatchScripts, len(ordered)); end > start; end-- {
			fits, stageErr := repository.stageBlueprintScriptBatch(ctx, active, ordered[start:end], currentByID)
			if stageErr != nil {
				return BlueprintScriptPublication{}, stageErr
			}
			if !fits {
				continue
			}
			start = end
			staged = true
			break
		}
		if !staged {
			return BlueprintScriptPublication{}, errs.New(
				errs.KindInternal, "one valid Blueprint Script exceeds the staging transaction ceiling",
			)
		}
	}
	nextValue, err := scriptrecord.EncodeScriptSetGeneration(next)
	if err != nil {
		return BlueprintScriptPublication{}, err
	}
	return BlueprintScriptPublication{
		environmentID: environmentID,
		conditions:    []etcdstore.Condition{{Key: scriptrecord.ScriptSetActiveKey(environmentID), ModRevision: active.Revision}},
		mutations:     []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(environmentID), Value: nextValue}},
	}, nil
}

func validateBlueprintBodyGenerationInputs(
	current map[string]etcdstore.Versioned[scriptrecord.Record],
	desired []scriptrecord.Record,
	generations []scriptrecord.BodyGenerationRecord,
) error {
	want := make(map[string]scriptrecord.BodyGenerationRecord)
	for _, record := range desired {
		previous, exists := current[record.Desired.ID]
		if !exists || previous.Record.ActiveGeneration != record.ActiveGeneration {
			body, err := scriptrecord.NewScriptBodyGeneration(record)
			if err != nil {
				return err
			}
			want[record.Desired.ID] = body
		}
	}
	if len(want) != len(generations) {
		return errs.New(errs.KindValidationFailed, "Blueprint Script body generation set is incomplete")
	}
	for _, generation := range generations {
		expected, exists := want[generation.ScriptID]
		if !exists || generation != expected {
			return errs.New(errs.KindValidationFailed, "Blueprint Script body generation set is invalid")
		}
		delete(want, generation.ScriptID)
	}
	return nil
}

func (repository *ScriptRepository) stageBlueprintScriptBatch(
	ctx context.Context,
	active etcdstore.Versioned[scriptrecord.SetGenerationRecord],
	records []scriptrecord.Record,
	current map[string]etcdstore.Versioned[scriptrecord.Record],
) (bool, error) {
	lookupKeys := make([]string, 0, len(records)*3)
	for _, record := range records {
		lookupKeys = append(lookupKeys,
			scriptrecord.ScriptLocatorKey(record.Desired.ID),
			scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
			deletionTombstoneKey("script", record.Desired.ID),
		)
	}
	lookups, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: lookupKeys, Revision: active.ReadRevision})
	if err != nil {
		return false, err
	}
	if lookups == nil || lookups.ReadRevision != active.ReadRevision || len(lookups.Values) != len(lookupKeys) {
		return false, errs.New(errs.KindInternal, "Blueprint Script staging evidence is incomplete")
	}
	defer etcdstore.ClearValues(lookups.Values)

	conditions := []etcdstore.Condition{
		{Key: scriptrecord.ScriptSetActiveKey(active.Record.EnvironmentID), ModRevision: active.Revision},
		{Key: deletionTombstoneKey("environment", active.Record.EnvironmentID)},
	}
	mutations := make([]etcdstore.Mutation, 0, len(records)*5)
	for index, record := range records {
		locatorValue := lookups.Values[index*3]
		environmentLocatorValue := lookups.Values[index*3+1]
		if lookups.Values[index*3+2] != nil {
			return false, errs.New(errs.KindResourceInUse, "Script deletion is in progress")
		}
		locatorCondition := etcdstore.Condition{Key: scriptrecord.ScriptLocatorKey(record.Desired.ID)}
		if locatorValue != nil {
			locator, decodeErr := scriptrecord.DecodeScriptLocator(locatorValue.Value)
			if decodeErr != nil || locator.ScriptID != record.Desired.ID ||
				locator.EnvironmentID != record.EnvironmentID {
				return false, errs.New(errs.KindStateConflict, "Script stable identity is already in use")
			}
			locatorCondition.ModRevision = locatorValue.ModRevision
		} else if _, existed := current[record.Desired.ID]; existed {
			return false, errs.New(errs.KindInternal, "active Script locator is missing")
		}
		environmentLocatorCondition := etcdstore.Condition{
			Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
		}
		if environmentLocatorValue != nil {
			if string(environmentLocatorValue.Value) != record.Desired.ID {
				return false, errs.New(errs.KindInternal, "Script Environment locator is corrupt")
			}
			environmentLocatorCondition.ModRevision = environmentLocatorValue.ModRevision
		} else if locatorValue != nil {
			return false, errs.New(errs.KindInternal, "Script Environment locator is missing")
		}

		primaryKey := scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID)
		bodyKey := scriptrecord.ScriptSetBodyGenerationKey(
			record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID, record.ActiveGeneration,
		)
		ownerKey := scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID)
		slugKey := scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug)
		conditions = append(conditions,
			locatorCondition, environmentLocatorCondition,
			etcdstore.Condition{Key: deletionTombstoneKey("script", record.Desired.ID)},
			etcdstore.Condition{Key: primaryKey}, etcdstore.Condition{Key: bodyKey}, etcdstore.Condition{Key: ownerKey}, etcdstore.Condition{Key: slugKey},
		)
		primary, encodeErr := scriptrecord.EncodeRecord(record)
		if encodeErr != nil {
			clearMutationValues(mutations)
			return false, encodeErr
		}
		body, bodyErr := scriptrecord.NewScriptBodyGeneration(record)
		if bodyErr != nil {
			clear(primary)
			clearMutationValues(mutations)
			return false, bodyErr
		}
		bodyValue, encodeErr := scriptrecord.EncodeScriptBodyGeneration(body)
		if encodeErr != nil {
			clear(primary)
			clearMutationValues(mutations)
			return false, encodeErr
		}
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: primaryKey, Value: primary},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: bodyKey, Value: bodyValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: ownerKey, Value: []byte(record.Desired.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: slugKey, Value: []byte(record.Desired.ID)},
		)
		if _, existed := current[record.Desired.ID]; !existed {
			locator, locatorErr := scriptrecord.EncodeScriptLocator(
				scriptrecord.LocatorRecord{ScriptID: record.Desired.ID, EnvironmentID: record.EnvironmentID},
			)
			if locatorErr != nil {
				clearMutationValues(mutations)
				return false, locatorErr
			}
			mutations = append(
				mutations,
				etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptLocatorKey(record.Desired.ID), Value: locator},
			)
			mutations = append(mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, record.Desired.ID),
				Value: []byte(record.Desired.ID),
			})
		}
	}
	defer clearMutationValues(mutations)
	if len(records)*2 > 16 || len(conditions)+len(mutations) > etcdstore.MaximumOperations ||
		scriptStageEncodedBytes(conditions, mutations) > etcdstore.MaximumBytes {
		return false, nil
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		if repository.blueprintScriptBatchMatches(ctx, active, mutations) {
			return true, nil
		}
		return false, err
	}
	if !result.Succeeded {
		if repository.blueprintScriptBatchMatches(ctx, active, mutations) {
			return true, nil
		}
		return false, errs.New(errs.KindStateConflict, "Blueprint Script staging raced durable state")
	}
	return true, nil
}

func (repository *ScriptRepository) blueprintScriptBatchMatches(
	ctx context.Context,
	active etcdstore.Versioned[scriptrecord.SetGenerationRecord],
	mutations []etcdstore.Mutation,
) bool {
	keys := make([]string, 1, len(mutations)+1)
	keys[0] = scriptrecord.ScriptSetActiveKey(active.Record.EnvironmentID)
	for _, mutation := range mutations {
		if mutation.Type != etcdstore.MutationPut || mutation.Prefix {
			return false
		}
		keys = append(keys, mutation.Key)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil || read == nil || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[0].ModRevision != active.Revision {
		return false
	}
	for index, mutation := range mutations {
		if read.Values[index+1] == nil || !bytes.Equal(read.Values[index+1].Value, mutation.Value) {
			return false
		}
	}
	return true
}

func scriptStageEncodedBytes(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) int {
	size := 128
	for _, condition := range conditions {
		size += len(condition.Key) + 64
	}
	for _, mutation := range mutations {
		size += len(mutation.Key) + len(mutation.Value) + 64
	}
	return size
}
