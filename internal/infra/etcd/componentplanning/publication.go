package componentplanning

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"reflect"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type Publication struct {
	preparation ComponentTaskPreparation
	conditions  []etcdstore.Condition
	mutations   []etcdstore.Mutation
	values      [][]byte
	secrets     []componentTaskSecretReference
}

func (repository *Planner) PrepareComponentTaskPublication(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	task TaskIdentity,
	zoneChanges []blueprints.EnvironmentBlueprintZoneChange,
	preparation ComponentTaskPreparation,
) (Publication, error) {
	publication := Publication{
		preparation: cloneComponentTaskPreparation(preparation),
		conditions: []etcdstore.Condition{{
			Key: environmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID),
		}},
	}
	if ComponentTaskPreparationIsZero(preparation) {
		return publication, nil
	}
	if err := ValidateComponentTaskPreparation(preparation); err != nil {
		return Publication{}, err
	}
	if err := ValidateComponentTaskOwner(task, preparation.Intent); err != nil {
		return Publication{}, err
	}
	if err := validateComponentTaskPublicationZones(preparation, zoneChanges); err != nil {
		return Publication{}, err
	}
	secretReferences, secretConditions, secretMutations, err := PrepareComponentTaskSecretReferences(
		ctx,
		repository.store,
		environment.Record.ProjectID,
		task.ID,
		preparation.Intent.Candidates,
		0,
	)
	if err != nil {
		return Publication{}, err
	}
	publication.secrets = secretReferences
	publication.conditions = append(publication.conditions, etcdstore.Condition{
		Key: environmentchanges.ComponentTaskIntentKey(task.ID),
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
	intentValue, err := environmentchanges.EncodeComponentTaskIntent(preparation.Intent)
	if err != nil {
		return Publication{}, err
	}
	publication.values = append(publication.values, intentValue)
	publication.mutations = append(publication.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentchanges.ComponentTaskIntentKey(task.ID), Value: intentValue},
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: environmentchanges.ComponentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		},
	)
	publication.mutations = append(publication.mutations, secretMutations...)
	for _, address := range preparation.addresses {
		if !address.Mutates {
			continue
		}
		value, encodeErr := networkreservations.EncodeComponentAddressRegistry(address.Zone.Record, address.Next)
		if encodeErr != nil {
			ClearPreparedComponentTaskPublication(publication)
			return Publication{}, encodeErr
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

func ComponentTaskPreparationIsZero(preparation ComponentTaskPreparation) bool {
	return preparation.Intent.TaskID == "" && preparation.Intent.EnvironmentID == "" &&
		preparation.Intent.Status == "" && len(preparation.Intent.Candidates) == 0 &&
		preparation.Intent.RouteProjection == nil &&
		preparation.Intent.CreatedAt.IsZero() && preparation.Intent.TerminalAt == nil &&
		len(preparation.managedRuntimeSources) == 0 &&
		!preparation.appliedProjectionPresent && preparation.appliedProjectionRevision == 0 &&
		preparation.desiredProjectionRevision == 0 &&
		len(preparation.addresses) == 0
}

func (publication Publication) Classify(values []*etcdstore.KeyValue) error {
	if len(values) != len(publication.conditions) {
		return errs.New(errs.KindInternal, "Blueprint Component compare evidence is incomplete")
	}
	if values[0] != nil {
		return errs.New(errs.KindStateConflict, "Environment already has an active Component reconciliation")
	}
	if ComponentTaskPreparationIsZero(publication.preparation) {
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
	if etcdstore.RevisionOf(values[3]) != publication.preparation.desiredProjectionRevision {
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

func ClearPreparedComponentTaskPublication(publication Publication) {
	for _, value := range publication.values {
		clear(value)
	}
}
