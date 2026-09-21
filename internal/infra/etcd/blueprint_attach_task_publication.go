package etcd

import (
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type preparedBlueprintAttachTaskPublication struct {
	preparation BlueprintAttachTaskPreparation
	conditions  []etcdstore.Condition
	mutations   []etcdstore.Mutation
	values      [][]byte
}

func prepareBlueprintAttachTaskPublication(
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	projection EnvironmentComposeProjection,
	task TaskRecord,
	preparation BlueprintAttachTaskPreparation,
) (preparedBlueprintAttachTaskPublication, error) {
	publication := preparedBlueprintAttachTaskPublication{
		preparation: BlueprintAttachTaskPreparation{
			Intent:     preparation.Intent,
			candidates: cloneEnvironmentBlueprintAttachCandidateInputs(preparation.candidates),
		},
	}
	if blueprintAttachTaskPreparationIsZero(preparation) {
		return publication, nil
	}
	if err := validateBlueprintAttachTaskPreparation(preparation); err != nil {
		return preparedBlueprintAttachTaskPublication{}, err
	}
	if task.ID != preparation.Intent.TaskID || task.Target != environment.Record.ID ||
		preparation.Intent.EnvironmentID != environment.Record.ID {
		return preparedBlueprintAttachTaskPublication{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach preparation is owned by another Task",
		)
	}
	serviceIDs := make(map[string]struct{}, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		serviceIDs[service.Desired.ID] = struct{}{}
	}
	intentValue, err := encodeBlueprintAttachTaskIntent(preparation.Intent)
	if err != nil {
		return preparedBlueprintAttachTaskPublication{}, err
	}
	publication.values = append(publication.values, intentValue)
	publication.conditions = append(publication.conditions, etcdstore.Condition{Key: blueprintAttachTaskIntentKey(task.ID)})
	publication.mutations = append(publication.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: blueprintAttachTaskIntentKey(task.ID), Value: intentValue,
	})
	candidateIDs := make(map[string]struct{}, len(preparation.candidates))
	for _, input := range preparation.candidates {
		candidateIDs[input.Record.ID] = struct{}{}
	}
	retainedPublished := make(map[string]struct{})
	appendRetained := func(retained etcdstore.Versioned[attachrecord.Record]) error {
		if _, exists := retainedPublished[retained.Record.ID]; exists {
			return nil
		}
		value, encodeErr := attachrecord.EncodeAttachRecord(retained.Record)
		if encodeErr != nil {
			return encodeErr
		}
		retainedPublished[retained.Record.ID] = struct{}{}
		publication.values = append(publication.values, value)
		publication.conditions = append(publication.conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(retained.Record.ID), ModRevision: retained.Revision},
			etcdstore.Condition{Key: deletionTombstoneKey("attach", retained.Record.ID)},
		)
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(retained.Record.ID), Value: value,
		})
		return nil
	}
	if preparation.Intent.OwnsEnvironmentFence {
		publication.mutations = append(publication.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
		})
	}
	for _, input := range preparation.candidates {
		record := input.Record
		if _, exists := serviceIDs[record.ServiceID]; !exists {
			clearPreparedBlueprintAttachTaskPublication(publication)
			return preparedBlueprintAttachTaskPublication{}, errs.New(
				errs.KindValidationFailed,
				"Blueprint Attach consumer Service is absent from the candidate projection",
			)
		}
		recordValue, encodeErr := attachrecord.EncodeAttachRecord(record)
		if encodeErr != nil {
			clearPreparedBlueprintAttachTaskPublication(publication)
			return preparedBlueprintAttachTaskPublication{}, encodeErr
		}
		publication.values = append(publication.values, recordValue)
		publication.conditions = append(publication.conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(record.ID)},
			etcdstore.Condition{Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name)},
			etcdstore.Condition{Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID)},
			etcdstore.Condition{Key: attachrecord.AttachServiceKey(record.ServiceID, record.ID)},
			etcdstore.Condition{Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID)},
			etcdstore.Condition{Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID)},
			etcdstore.Condition{Key: deletionTombstoneKey("attach", record.ID)},
			etcdstore.Condition{Key: hierarchyrecord.ProjectKey(record.BackingProjectID), ModRevision: input.BackingProject.Revision},
			etcdstore.Condition{Key: hierarchyrecord.EnvironmentKey(record.BackingEnvironmentID), ModRevision: input.BackingEnvironment.Revision},
			servicerecord.ServiceDesiredCondition(input.BackingService),
		)
		publication.mutations = append(
			publication.mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: recordValue},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachNameKey(record.EnvironmentID, record.Name),
				Value: []byte(record.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachServiceKey(record.ServiceID, record.ID), Value: []byte(record.ID)},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID),
				Value: []byte(record.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID),
				Value: []byte(record.ID),
			},
		)
		if input.Facts != nil {
			factValue, factErr := attachrecord.EncodeAttachEncryptedFacts(*input.Facts)
			if factErr != nil {
				clearPreparedBlueprintAttachTaskPublication(publication)
				return preparedBlueprintAttachTaskPublication{}, factErr
			}
			publication.values = append(publication.values, factValue)
			publication.conditions = append(publication.conditions, etcdstore.Condition{Key: attachrecord.AttachFactsKey(record.ID)})
			publication.mutations = append(publication.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: attachrecord.AttachFactsKey(record.ID), Value: factValue,
			})
		}
		if !record.OwnsCredential() {
			if _, candidate := candidateIDs[record.CredentialAttachID]; !candidate {
				if err := appendRetained(*input.RetainedCredentialOwner); err != nil {
					clearPreparedBlueprintAttachTaskPublication(publication)
					return preparedBlueprintAttachTaskPublication{}, err
				}
			}
			publication.conditions = append(publication.conditions, etcdstore.Condition{
				Key: attachrecord.AttachCredentialByKey(record.CredentialAttachID, record.ID),
			})
			publication.mutations = append(publication.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: attachrecord.AttachCredentialByKey(record.CredentialAttachID, record.ID), Value: []byte(record.ID),
			})
		}
		for _, grantID := range record.GrantAttachIDs {
			if _, candidate := candidateIDs[grantID]; !candidate {
				for _, retained := range input.RetainedGrantTargets {
					if retained.Record.ID == grantID {
						if err := appendRetained(retained); err != nil {
							clearPreparedBlueprintAttachTaskPublication(publication)
							return preparedBlueprintAttachTaskPublication{}, err
						}
						break
					}
				}
			}
			publication.conditions = append(
				publication.conditions,
				etcdstore.Condition{Key: attachrecord.AttachGrantedByKey(grantID, record.ID)},
			)
			publication.mutations = append(publication.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: attachrecord.AttachGrantedByKey(grantID, record.ID), Value: []byte(record.ID),
			})
		}
		if len(record.GrantAttachIDs) != 0 {
			dependentValue, dependentErr := attachrecord.EncodeAttachDependentGrantIndex(record.ID, record.GrantAttachIDs)
			if dependentErr != nil {
				clearPreparedBlueprintAttachTaskPublication(publication)
				return preparedBlueprintAttachTaskPublication{}, dependentErr
			}
			publication.values = append(publication.values, dependentValue)
			publication.conditions = append(publication.conditions, etcdstore.Condition{Key: attachrecord.AttachDependentGrantKey(record.ID)})
			publication.mutations = append(publication.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: attachrecord.AttachDependentGrantKey(record.ID), Value: dependentValue,
			})
		}
	}
	conditions, err := uniqueBlueprintAttachPublicationConditions(publication.conditions)
	if err != nil {
		clearPreparedBlueprintAttachTaskPublication(publication)
		return preparedBlueprintAttachTaskPublication{}, err
	}
	publication.conditions = conditions
	return publication, nil
}

func uniqueBlueprintAttachPublicationConditions(conditions []etcdstore.Condition) ([]etcdstore.Condition, error) {
	unique := make([]etcdstore.Condition, 0, len(conditions))
	byKey := make(map[string]etcdstore.Condition, len(conditions))
	for _, condition := range conditions {
		existing, duplicate := byKey[condition.Key]
		if duplicate {
			if existing != condition {
				return nil, errs.New(
					errs.KindStateConflict,
					"Blueprint Attach backing scope changed during preparation",
				)
			}
			continue
		}
		byKey[condition.Key] = condition
		unique = append(unique, condition)
	}
	return unique, nil
}

func classifyEnvironmentBlueprintAttachPublication(
	base idempotencyPlanClassifier,
	publication preparedBlueprintAttachTaskPublication,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		count := len(publication.conditions)
		if len(values) < count {
			return errs.New(errs.KindInternal, "Blueprint Attach compare evidence is incomplete")
		}
		baseValues := values[:len(values)-count]
		if err := base(revision, baseValues); err != nil {
			return err
		}
		for index, value := range values[len(baseValues):] {
			condition := publication.conditions[index]
			if condition.ModRevision > 0 {
				if value == nil || value.ModRevision != condition.ModRevision {
					return errs.New(errs.KindStateConflict, "Blueprint Attach backing scope changed")
				}
				continue
			}
			if value != nil {
				return errs.New(errs.KindStateConflict, "Blueprint Attach publication raced")
			}
		}
		return nil
	}
}

func clearPreparedBlueprintAttachTaskPublication(publication preparedBlueprintAttachTaskPublication) {
	for _, value := range publication.values {
		clear(value)
	}
	clearBlueprintAttachTaskPreparation(&publication.preparation)
}
