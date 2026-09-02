package etcd

import (
	"context"
	"reflect"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedComponentTaskPublication struct {
	preparation ComponentTaskPreparation
	conditions  []Condition
	mutations   []Mutation
	values      [][]byte
	secrets     []componentTaskSecretReference
}

func (repository *HierarchyRepository) prepareComponentTaskPublication(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	task TaskRecord,
	zoneChanges []EnvironmentBlueprintZoneChange,
	preparation ComponentTaskPreparation,
) (preparedComponentTaskPublication, error) {
	publication := preparedComponentTaskPublication{
		preparation: cloneComponentTaskPreparation(preparation),
		conditions: []Condition{{
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
	publication.conditions = append(publication.conditions, Condition{
		Key: componentTaskIntentKey(task.ID),
	})
	appliedProjectionCondition := Condition{Key: environmentComposeProjectionKey(environment.Record.ID)}
	if preparation.appliedProjectionPresent {
		appliedProjectionCondition.ModRevision = preparation.appliedProjectionRevision
	}
	publication.conditions = append(publication.conditions, appliedProjectionCondition)
	for _, candidate := range preparation.Intent.Candidates {
		publication.conditions = append(publication.conditions, Condition{
			Key:         componentKey(candidate.Current.Desired.ID),
			ModRevision: candidate.CurrentRevision,
		})
	}
	for _, address := range preparation.addresses {
		publication.conditions = append(publication.conditions, Condition{
			Key:         componentAddressRegistryKey(address.Zone.Record.Desired.ID),
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
		Mutation{Type: MutationPut, Key: componentTaskIntentKey(task.ID), Value: intentValue},
		Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		},
	)
	publication.mutations = append(publication.mutations, secretMutations...)
	for _, address := range preparation.addresses {
		if !address.Mutates {
			continue
		}
		value, encodeErr := encodeComponentAddressRegistry(address.Zone.Record, address.Next)
		if encodeErr != nil {
			clearPreparedComponentTaskPublication(publication)
			return preparedComponentTaskPublication{}, encodeErr
		}
		publication.values = append(publication.values, value)
		publication.mutations = append(publication.mutations, Mutation{
			Type: MutationPut, Key: componentAddressRegistryKey(address.Zone.Record.Desired.ID), Value: value,
		})
	}
	return publication, nil
}

func validateComponentTaskPublicationZones(
	preparation ComponentTaskPreparation,
	zoneChanges []EnvironmentBlueprintZoneChange,
) error {
	changes := make(map[string]EnvironmentBlueprintZoneChange, len(zoneChanges))
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
		!preparation.appliedProjectionPresent && preparation.appliedProjectionRevision == 0 &&
		len(preparation.addresses) == 0
}

func classifyEnvironmentBlueprintComponentPublication(
	base idempotencyPlanClassifier,
	publication preparedComponentTaskPublication,
) idempotencyPlanClassifier {
	return func(revision int64, values []*KeyValue) error {
		publicationCount := len(publication.conditions)
		if len(values) < publicationCount {
			return errs.New(errs.KindInternal, "Blueprint Component compare evidence is incomplete")
		}
		baseValues := values[:len(values)-publicationCount]
		if err := base(revision, baseValues); err != nil {
			return err
		}
		return publication.classify(values[len(baseValues):])
	}
}

func (publication preparedComponentTaskPublication) classify(values []*KeyValue) error {
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
	offset := 3
	for index, candidate := range publication.preparation.Intent.Candidates {
		value := values[offset+index]
		if value == nil {
			return errs.New(errs.KindComponentNotFound, "Component was not found")
		}
		if value.ModRevision != candidate.CurrentRevision {
			return stateConflict("component", candidate.Current.Desired.ID)
		}
	}
	offset += len(publication.preparation.Intent.Candidates)
	for index, address := range publication.preparation.addresses {
		value := values[offset+index]
		if address.Current.Revision == 0 {
			if value != nil {
				return stateConflict("Component address registry", address.Zone.Record.Desired.ID)
			}
			continue
		}
		if value == nil || value.ModRevision != address.Current.Revision {
			return stateConflict("Component address registry", address.Zone.Record.Desired.ID)
		}
	}
	offset += len(publication.preparation.addresses)
	for _, reference := range publication.secrets {
		if values[offset] == nil {
			return errs.New(errs.KindSecretNotFound, "Component Secret was not found")
		}
		if values[offset].ModRevision != reference.revision {
			return stateConflict("secret", reference.secretID)
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
