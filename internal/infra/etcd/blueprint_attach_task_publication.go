package etcd

import "github.com/AlanD20/groundplane/pkg/errs"

type preparedBlueprintAttachTaskPublication struct {
	preparation BlueprintAttachTaskPreparation
	conditions  []Condition
	mutations   []Mutation
	values      [][]byte
}

func prepareBlueprintAttachTaskPublication(
	environment Versioned[EnvironmentRecord],
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
	publication.conditions = append(publication.conditions, Condition{Key: blueprintAttachTaskIntentKey(task.ID)})
	publication.mutations = append(publication.mutations, Mutation{
		Type: MutationPut, Key: blueprintAttachTaskIntentKey(task.ID), Value: intentValue,
	})
	candidateIDs := make(map[string]struct{}, len(preparation.candidates))
	for _, input := range preparation.candidates {
		candidateIDs[input.Record.ID] = struct{}{}
	}
	retainedPublished := make(map[string]struct{})
	appendRetained := func(retained Versioned[AttachRecord]) error {
		if _, exists := retainedPublished[retained.Record.ID]; exists {
			return nil
		}
		value, encodeErr := encodeAttachRecord(retained.Record)
		if encodeErr != nil {
			return encodeErr
		}
		retainedPublished[retained.Record.ID] = struct{}{}
		publication.values = append(publication.values, value)
		publication.conditions = append(publication.conditions,
			Condition{Key: attachKey(retained.Record.ID), ModRevision: retained.Revision},
			Condition{Key: deletionTombstoneKey("attach", retained.Record.ID)},
		)
		publication.mutations = append(publication.mutations, Mutation{
			Type: MutationPut, Key: attachKey(retained.Record.ID), Value: value,
		})
		return nil
	}
	if preparation.Intent.OwnsEnvironmentFence {
		publication.mutations = append(publication.mutations, Mutation{
			Type: MutationPut, Key: componentTaskActiveEnvironmentKey(environment.Record.ID), Value: []byte(task.ID),
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
		recordValue, encodeErr := encodeAttachRecord(record)
		if encodeErr != nil {
			clearPreparedBlueprintAttachTaskPublication(publication)
			return preparedBlueprintAttachTaskPublication{}, encodeErr
		}
		publication.values = append(publication.values, recordValue)
		publication.conditions = append(publication.conditions,
			Condition{Key: attachKey(record.ID)},
			Condition{Key: attachNameKey(record.EnvironmentID, record.Name)},
			Condition{Key: attachOwnerKey(record.EnvironmentID, record.ID)},
			Condition{Key: attachServiceKey(record.ServiceID, record.ID)},
			Condition{Key: attachBackingServiceKey(record.BackingServiceID, record.ID)},
			Condition{Key: attachBackingProjectKey(record.BackingProjectID, record.ID)},
			Condition{Key: deletionTombstoneKey("attach", record.ID)},
			Condition{Key: projectKey(record.BackingProjectID), ModRevision: input.BackingProject.Revision},
			Condition{Key: environmentKey(record.BackingEnvironmentID), ModRevision: input.BackingEnvironment.Revision},
			serviceDesiredCondition(input.BackingService),
		)
		publication.mutations = append(publication.mutations,
			Mutation{Type: MutationPut, Key: attachKey(record.ID), Value: recordValue},
			Mutation{Type: MutationPut, Key: attachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
			Mutation{Type: MutationPut, Key: attachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
			Mutation{Type: MutationPut, Key: attachServiceKey(record.ServiceID, record.ID), Value: []byte(record.ID)},
			Mutation{Type: MutationPut, Key: attachBackingServiceKey(record.BackingServiceID, record.ID), Value: []byte(record.ID)},
			Mutation{Type: MutationPut, Key: attachBackingProjectKey(record.BackingProjectID, record.ID), Value: []byte(record.ID)},
		)
		if input.Facts != nil {
			factValue, factErr := encodeAttachEncryptedFacts(*input.Facts)
			if factErr != nil {
				clearPreparedBlueprintAttachTaskPublication(publication)
				return preparedBlueprintAttachTaskPublication{}, factErr
			}
			publication.values = append(publication.values, factValue)
			publication.conditions = append(publication.conditions, Condition{Key: attachFactsKey(record.ID)})
			publication.mutations = append(publication.mutations, Mutation{
				Type: MutationPut, Key: attachFactsKey(record.ID), Value: factValue,
			})
		}
		if !record.OwnsCredential() {
			if _, candidate := candidateIDs[record.CredentialAttachID]; !candidate {
				if err := appendRetained(*input.RetainedCredentialOwner); err != nil {
					clearPreparedBlueprintAttachTaskPublication(publication)
					return preparedBlueprintAttachTaskPublication{}, err
				}
			}
			publication.conditions = append(publication.conditions, Condition{
				Key: attachCredentialByKey(record.CredentialAttachID, record.ID),
			})
			publication.mutations = append(publication.mutations, Mutation{
				Type: MutationPut, Key: attachCredentialByKey(record.CredentialAttachID, record.ID), Value: []byte(record.ID),
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
			publication.conditions = append(publication.conditions, Condition{Key: attachGrantedByKey(grantID, record.ID)})
			publication.mutations = append(publication.mutations, Mutation{
				Type: MutationPut, Key: attachGrantedByKey(grantID, record.ID), Value: []byte(record.ID),
			})
		}
		if len(record.GrantAttachIDs) != 0 {
			dependentValue, dependentErr := encodeAttachDependentGrantIndex(record.ID, record.GrantAttachIDs)
			if dependentErr != nil {
				clearPreparedBlueprintAttachTaskPublication(publication)
				return preparedBlueprintAttachTaskPublication{}, dependentErr
			}
			publication.values = append(publication.values, dependentValue)
			publication.conditions = append(publication.conditions, Condition{Key: attachDependentGrantKey(record.ID)})
			publication.mutations = append(publication.mutations, Mutation{
				Type: MutationPut, Key: attachDependentGrantKey(record.ID), Value: dependentValue,
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

func uniqueBlueprintAttachPublicationConditions(conditions []Condition) ([]Condition, error) {
	unique := make([]Condition, 0, len(conditions))
	byKey := make(map[string]Condition, len(conditions))
	for _, condition := range conditions {
		existing, duplicate := byKey[condition.Key]
		if duplicate {
			if existing != condition {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Attach backing scope changed during preparation")
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
	return func(revision int64, values []*KeyValue) error {
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
