package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"reflect"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedComponentTaskPublication struct {
	preparation ComponentTaskPreparation
	conditions  []etcdstore.Condition
	mutations   []etcdstore.Mutation
	values      [][]byte
	secrets     []componentTaskSecretReference
}

func (repository *HierarchyRepository) prepareComponentTaskPublication(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	task TaskRecord,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
	preparation ComponentTaskPreparation,
) (preparedComponentTaskPublication, error) {
	publication := preparedComponentTaskPublication{
		preparation: cloneComponentTaskPreparation(preparation),
		conditions: []etcdstore.Condition{{
			Key: componentTaskActiveEnvironmentKey(environment.Record.ID),
		}},
	}
	if componentTaskPreparationIsZero(preparation) {
		return publication, nil
	}
	if err := validateComponentTaskPreparation(preparation); err != nil {
		return preparedComponentTaskPublication{}, err
	}
	if err := validateComponentTaskOwner(task, preparation.Intent); err != nil {
		return preparedComponentTaskPublication{}, err
	}
	if err := validateComponentTaskPublicationZones(preparation, zoneChanges); err != nil {
		return preparedComponentTaskPublication{}, err
	}
	secretReferences, secretConditions, secretMutations, err := prepareComponentTaskSecretReferences(
		ctx,
		repository.store,
		environment.Record.ProjectID,
		task.ID,
		preparation.Intent.Candidates,
		0,
	)
	if err != nil {
		return preparedComponentTaskPublication{}, err
	}
	publication.secrets = secretReferences
	publication.conditions = append(publication.conditions, etcdstore.Condition{
		Key: componentTaskIntentKey(task.ID),
	})
	appliedProjectionCondition := etcdstore.Condition{Key: projectionrecord.EnvironmentComposeProjectionStorageKey(environment.Record.ID)}
	if preparation.appliedProjectionPresent {
		appliedProjectionCondition.ModRevision = preparation.appliedProjectionRevision
	}
	publication.conditions = append(publication.conditions, appliedProjectionCondition)
	publication.conditions = append(publication.conditions, etcdstore.Condition{
		Key: blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), ModRevision: preparation.desiredProjectionRevision,
	})
	for _, candidate := range preparation.Intent.Candidates {
		publication.conditions = append(publication.conditions, etcdstore.Condition{
			Key:         componentrecord.RecordKey(candidate.Current.Desired.ID),
			ModRevision: candidate.CurrentRevision,
		})
	}
	for _, address := range preparation.addresses {
		publication.conditions = append(publication.conditions, etcdstore.Condition{
			Key:         networkreservations.ComponentAddressRegistryKey(address.Zone.Record.Desired.ID),
			ModRevision: address.Current.Revision,
		})
	}
	publication.conditions = append(publication.conditions, secretConditions...)
	intentValue, err := encodeComponentTaskIntent(preparation.Intent)
	if err != nil {
		return preparedComponentTaskPublication{}, err
	}
	publication.values = append(publication.values, intentValue)
	publication.mutations = append(publication.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentTaskIntentKey(task.ID), Value: intentValue},
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		},
	)
	publication.mutations = append(publication.mutations, secretMutations...)
	for _, address := range preparation.addresses {
		if !address.Mutates {
			continue
		}
		value, encodeErr := networkreservations.EncodeComponentAddressRegistry(address.Zone.Record, address.Next)
		if encodeErr != nil {
			clearPreparedComponentTaskPublication(publication)
			return preparedComponentTaskPublication{}, encodeErr
		}
		publication.values = append(publication.values, value)
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: networkreservations.ComponentAddressRegistryKey(address.Zone.Record.Desired.ID), Value: value,
		})
	}
	return publication, nil
}

func validateComponentTaskPublicationZones(
	preparation ComponentTaskPreparation,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
) error {
	changes := make(map[string]blueprints.EnvironmentBlueprintZoneChange, len(zoneChanges))
	for _, change := range zoneChanges {
		changes[change.Record.Desired.ID] = change
	}
	for _, address := range preparation.addresses {
		change, found := changes[address.Zone.Record.Desired.ID]
		if !found || !reflect.DeepEqual(change.Record, address.Zone.Record) ||
			(change.Current == nil) != (address.Zone.Revision == 0) ||
			(change.Current != nil && change.Current.Revision != address.Zone.Revision) {
			return errs.New(errs.KindValidationFailed, "Component candidate Zone preparation changed")
		}
	}
	return nil
}

func componentTaskPreparationIsZero(preparation ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == "" && preparation.Intent.EnvironmentID == "" &&
		preparation.Intent.Status == "" && len(preparation.Intent.Candidates) == 0 &&
		preparation.Intent.RouteProjection == nil &&
		preparation.Intent.CreatedAt.IsZero() && preparation.Intent.TerminalAt == nil &&
		len(preparation.managedRuntimeSources) == 0 &&
		!preparation.appliedProjectionPresent && preparation.appliedProjectionRevision == 0 &&
		preparation.desiredProjectionRevision == 0 &&
		len(preparation.addresses) == 0
}

func composeEnvironmentBlueprintComponentPublication(
	existing []etcdstore.Condition,
	base idempotencyPlanClassifier,
	publication preparedComponentTaskPublication,
) ([]etcdstore.Condition, idempotencyPlanClassifier, error) {
	conditions := append([]etcdstore.Condition(nil), existing...)
	indices := make(map[string]int, len(existing)+len(publication.conditions))
	for index, condition := range conditions {
		indices[condition.Key] = index
	}
	publicationIndices := make([]int, len(publication.conditions))
	for index, condition := range publication.conditions {
		position, found := indices[condition.Key]
		if found {
			if conditions[position] != condition {
				return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Component source revisions disagree")
			}
		} else {
			position = len(conditions)
			indices[condition.Key] = position
			conditions = append(conditions, condition)
		}
		publicationIndices[index] = position
	}
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Blueprint Component compare evidence is incomplete")
		}
		if err := base(revision, values[:len(existing)]); err != nil {
			return err
		}
		// Both owners classify the same authoritative read at a shared key;
		// deduplication must not shift the Component's positional evidence.
		componentValues := make([]*etcdstore.KeyValue, len(publicationIndices))
		for index, position := range publicationIndices {
			componentValues[index] = values[position]
		}
		return publication.classify(componentValues)
	}
	return conditions, classify, nil
}

func (publication preparedComponentTaskPublication) classify(values []*etcdstore.KeyValue) error {
	if len(values) != len(publication.conditions) {
		return errs.New(errs.KindInternal, "Blueprint Component compare evidence is incomplete")
	}
	if values[0] != nil {
		return errs.New(errs.KindStateConflict, "Environment already has an active Component reconciliation")
	}
	if componentTaskPreparationIsZero(publication.preparation) {
		return nil
	}
	if values[1] != nil {
		return errs.New(errs.KindStateConflict, "Component candidate Task identity is already in use")
	}
	appliedProjectionValue := values[2]
	if publication.preparation.appliedProjectionPresent {
		if appliedProjectionValue == nil ||
			appliedProjectionValue.ModRevision != publication.preparation.appliedProjectionRevision {
			return errs.New(errs.KindStateConflict, "Component candidate applied projection changed")
		}
	} else if appliedProjectionValue != nil {
		return errs.New(errs.KindStateConflict, "Component candidate applied projection was published")
	}
	if keyValueRevision(values[3]) != publication.preparation.desiredProjectionRevision {
		return errs.New(errs.KindStateConflict, "Component candidate desired projection changed")
	}
	offset := 4
	for index, candidate := range publication.preparation.Intent.Candidates {
		value := values[offset+index]
		if value == nil {
			return errs.New(errs.KindComponentNotFound, "Component was not found")
		}
		if value.ModRevision != candidate.CurrentRevision {
			return recordcodec.StateConflict("component", candidate.Current.Desired.ID)
		}
	}
	offset += len(publication.preparation.Intent.Candidates)
	for index, address := range publication.preparation.addresses {
		value := values[offset+index]
		if address.Current.Revision == 0 {
			if value != nil {
				return recordcodec.StateConflict("Component address registry", address.Zone.Record.Desired.ID)
			}
			continue
		}
		if value == nil || value.ModRevision != address.Current.Revision {
			return recordcodec.StateConflict("Component address registry", address.Zone.Record.Desired.ID)
		}
	}
	offset += len(publication.preparation.addresses)
	for _, reference := range publication.secrets {
		if values[offset] == nil {
			return errs.New(errs.KindSecretNotFound, "Component Secret was not found")
		}
		if values[offset].ModRevision != reference.revision {
			return recordcodec.StateConflict("secret", reference.secretID)
		}
		if values[offset+1] != nil {
			return errs.New(errs.KindResourceInUse, "Component Secret deletion is in progress")
		}
		if values[offset+2] != nil {
			return errs.New(errs.KindStateConflict, "Component Secret candidate reference already exists")
		}
		offset += 3
	}
	return nil
}

func clearPreparedComponentTaskPublication(publication preparedComponentTaskPublication) {
	for _, value := range publication.values {
		clear(value)
	}
}
