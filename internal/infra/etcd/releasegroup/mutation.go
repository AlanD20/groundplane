package releasegroup

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	infraetcd "github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (store *Store) createDirectFixture(ctx context.Context, group domain.Group) (Versioned, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned{}, err
	}
	if err := domain.Validate(group); err != nil {
		return Versioned{}, err
	}

	existing, found, err := store.find(ctx, group.ID)
	if err != nil {
		return Versioned{}, err
	}
	if found {
		if domain.Equal(existing.Group, group) {
			return existing, nil
		}
		return Versioned{}, errs.New(
			errs.KindStateConflict,
			"release group id already identifies different desired state",
		)
	}

	evidence, err := store.loadMutationEvidence(ctx, group, "", 0)
	if err != nil {
		return Versioned{}, err
	}
	value, err := encodeGroup(group)
	if err != nil {
		return Versioned{}, err
	}
	result, err := store.backend.Transact(ctx, evidence.conditions, []infraetcd.Mutation{
		{Type: infraetcd.MutationPut, Key: recordKey(group.ID), Value: value},
		{Type: infraetcd.MutationPut, Key: ownerKey(group.EnvironmentID, group.ID), Value: []byte(group.ID)},
		{Type: infraetcd.MutationPut, Key: nameKey(group.EnvironmentID, group.Name), Value: []byte(group.ID)},
		{
			Type:  infraetcd.MutationPut,
			Key:   environmentMutationEpochKey(group.EnvironmentID),
			Value: evidence.epochValue,
		},
	})
	clear(value)
	if err != nil {
		return Versioned{}, err
	}
	if !result.Succeeded {
		return Versioned{}, store.classifyConflict(ctx, group, 0)
	}
	return Versioned{Group: domain.Clone(group), Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (store *Store) updateDirectFixture(
	ctx context.Context,
	current Versioned,
	replacement domain.Group,
) (Versioned, error) {
	if err := validateVersion(current); err != nil {
		return Versioned{}, err
	}
	if err := domain.Validate(replacement); err != nil {
		return Versioned{}, err
	}
	if current.Group.ID != replacement.ID || current.Group.EnvironmentID != replacement.EnvironmentID {
		return Versioned{}, errs.New(
			errs.KindValidationFailed,
			"release group update changed stable identity or ownership",
		)
	}
	stored, found, err := store.find(ctx, current.Group.ID)
	if err != nil {
		return Versioned{}, err
	}
	if !found {
		return Versioned{}, errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
	}
	if domain.Equal(stored.Group, replacement) && stored.Revision >= current.Revision {
		return stored, nil
	}
	if stored.Revision != current.Revision || !domain.Equal(stored.Group, current.Group) {
		return Versioned{}, errs.New(errs.KindStateConflict, "release group changed before update")
	}
	evidence, err := store.loadMutationEvidence(ctx, replacement, current.Group.Name, current.Revision)
	if err != nil {
		return Versioned{}, err
	}
	value, err := encodeGroup(replacement)
	if err != nil {
		return Versioned{}, err
	}
	mutations := []infraetcd.Mutation{{Type: infraetcd.MutationPut, Key: recordKey(replacement.ID), Value: value}}
	mutations = append(mutations, infraetcd.Mutation{
		Type: infraetcd.MutationPut, Key: environmentMutationEpochKey(replacement.EnvironmentID), Value: evidence.epochValue,
	})
	if replacement.Name != current.Group.Name {
		mutations = append(
			mutations,
			infraetcd.Mutation{
				Type: infraetcd.MutationDelete,
				Key:  nameKey(replacement.EnvironmentID, current.Group.Name),
			},
			infraetcd.Mutation{
				Type:  infraetcd.MutationPut,
				Key:   nameKey(replacement.EnvironmentID, replacement.Name),
				Value: []byte(replacement.ID),
			},
		)
	}
	result, err := store.backend.Transact(ctx, evidence.conditions, mutations)
	clear(value)
	if err != nil {
		return Versioned{}, err
	}
	if !result.Succeeded {
		return Versioned{}, store.classifyConflict(ctx, replacement, current.Revision)
	}
	return Versioned{Group: domain.Clone(replacement), Revision: result.Revision, ReadRevision: result.Revision}, nil
}

func (store *Store) removeDirectForbidden(ctx context.Context, current Versioned) (Removal, error) {
	if err := validateVersion(current); err != nil {
		return Removal{}, err
	}
	return Removal{}, errs.New(
		errs.KindInternal,
		"release group removal requires the protected task and tombstone finalizer",
	)
}

type mutationEvidence struct {
	conditions []infraetcd.Condition
	epochValue []byte
}

func (store *Store) loadMutationEvidence(
	ctx context.Context,
	group domain.Group,
	oldName string,
	revision int64,
) (mutationEvidence, error) {
	baseKeys := []string{
		environmentKey(group.EnvironmentID), environmentMutationEpochKey(group.EnvironmentID),
		environmentOperationLockKey(group.EnvironmentID), environmentDeletionKey(group.EnvironmentID),
		recordKey(group.ID), ownerKey(group.EnvironmentID, group.ID), nameKey(group.EnvironmentID, group.Name),
		environmentComposeKey(group.EnvironmentID), deletionKey("release_group", group.ID),
	}
	if oldName != "" && oldName != group.Name {
		baseKeys = append(baseKeys, nameKey(group.EnvironmentID, oldName))
	}
	result, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: baseKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if result == nil || len(result.Values) != len(baseKeys) || result.Values[0] == nil {
		return mutationEvidence{}, errs.New(errs.KindEnvironmentNotFound, "release group environment was not found")
	}
	environment, err := decodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != group.EnvironmentID {
		return mutationEvidence{}, corruptRecord()
	}
	if result.Values[1] == nil {
		return mutationEvidence{}, corruptRecord()
	}
	epochValue, err := decodeEpoch(result.Values[1].Value, group.EnvironmentID)
	if err != nil {
		return mutationEvidence{}, err
	}
	defer func() {
		if err != nil {
			clear(epochValue)
		}
	}()
	if result.Values[2] != nil || result.Values[3] != nil {
		return mutationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"release group environment has an active operation or deletion",
		)
	}
	if result.Values[7] == nil {
		return mutationEvidence{}, errs.New(
			errs.KindResourceInUse,
			"release group environment has no enabled compose project",
		)
	}
	projection, err := decodeComposeProjection(result.Values[7].Value)
	if err != nil || projection.EnvironmentID != group.EnvironmentID {
		return mutationEvidence{}, corruptRecord()
	}
	if result.Values[8] != nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group deletion is already in progress")
	}
	conditions := []infraetcd.Condition{
		{Key: baseKeys[0], ModRevision: result.Values[0].ModRevision},
		{Key: baseKeys[1], ModRevision: result.Values[1].ModRevision},
		{Key: baseKeys[2]}, {Key: baseKeys[3]},
		{Key: baseKeys[7], ModRevision: result.Values[7].ModRevision}, {Key: baseKeys[8]},
	}
	if revision == 0 {
		if result.Values[4] != nil || result.Values[5] != nil {
			return mutationEvidence{}, errs.New(errs.KindStateConflict, "release group stable id already exists")
		}
		if result.Values[6] != nil {
			return mutationEvidence{}, errs.New(
				errs.KindNameConflict,
				"release group name already exists in the environment",
			)
		}
		conditions = append(
			conditions,
			infraetcd.Condition{Key: baseKeys[4]},
			infraetcd.Condition{Key: baseKeys[5]},
			infraetcd.Condition{Key: baseKeys[6]},
		)
	} else {
		if result.Values[4] == nil || result.Values[4].ModRevision != revision || result.Values[5] == nil {
			return mutationEvidence{}, errs.New(errs.KindStateConflict, "release group changed before mutation")
		}
		stored, decodeErr := decodeGroup(result.Values[4].Value)
		if decodeErr != nil || stored.ID != group.ID || stored.EnvironmentID != group.EnvironmentID ||
			stored.Name != oldName || !bytes.Equal(result.Values[5].Value, []byte(group.ID)) {
			return mutationEvidence{}, corruptRecord()
		}
		conditions = append(conditions,
			infraetcd.Condition{Key: baseKeys[4], ModRevision: revision},
			infraetcd.Condition{Key: baseKeys[5], ModRevision: result.Values[5].ModRevision},
		)
		if oldName != group.Name {
			if result.Values[6] != nil {
				return mutationEvidence{}, errs.New(errs.KindNameConflict, "release group name already exists in the environment")
			}
			oldNameIndex := result.Values[9]
			if oldNameIndex == nil || !bytes.Equal(oldNameIndex.Value, []byte(group.ID)) {
				return mutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions,
				infraetcd.Condition{Key: baseKeys[6]},
				infraetcd.Condition{Key: baseKeys[9], ModRevision: oldNameIndex.ModRevision},
			)
		} else {
			if result.Values[6] == nil || !bytes.Equal(result.Values[6].Value, []byte(group.ID)) {
				return mutationEvidence{}, corruptRecord()
			}
			conditions = append(conditions, infraetcd.Condition{Key: baseKeys[6], ModRevision: result.Values[6].ModRevision})
		}
	}

	projectResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: []string{
		projectKey(environment.ProjectID), environmentOwnerKey(environment.ProjectID, environment.ID),
		deletionKey("project", environment.ProjectID),
	}})
	if err != nil {
		return mutationEvidence{}, err
	}
	if projectResult == nil || len(projectResult.Values) != 3 || projectResult.Values[0] == nil ||
		projectResult.Values[1] == nil {
		return mutationEvidence{}, corruptRecord()
	}
	project, err := decodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID ||
		!bytes.Equal(projectResult.Values[1].Value, []byte(environment.ID)) {
		return mutationEvidence{}, corruptRecord()
	}
	if projectResult.Values[2] != nil {
		return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group project deletion is in progress")
	}
	conditions = append(
		conditions,
		infraetcd.Condition{Key: projectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision},
		infraetcd.Condition{
			Key:         environmentOwnerKey(project.ID, environment.ID),
			ModRevision: projectResult.Values[1].ModRevision,
		},
		infraetcd.Condition{Key: deletionKey("project", project.ID)},
	)
	ownerIndex := projectOwnerKey(project)
	ownerKeys := []string{ownerIndex}
	if project.Kind == infraetcd.ProjectKindTenant {
		ownerKeys = append(ownerKeys, tenantKey(project.TenantID), deletionKey("tenant", project.TenantID))
	}
	ownerResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: ownerKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if ownerResult == nil || len(ownerResult.Values) != len(ownerKeys) || ownerResult.Values[0] == nil ||
		!bytes.Equal(ownerResult.Values[0].Value, []byte(project.ID)) {
		return mutationEvidence{}, corruptRecord()
	}
	conditions = append(
		conditions,
		infraetcd.Condition{Key: ownerIndex, ModRevision: ownerResult.Values[0].ModRevision},
	)
	if project.Kind == infraetcd.ProjectKindTenant {
		if ownerResult.Values[1] == nil {
			return mutationEvidence{}, corruptRecord()
		}
		tenant, tenantErr := decodeTenant(ownerResult.Values[1].Value)
		if tenantErr != nil || tenant.ID != project.TenantID {
			return mutationEvidence{}, corruptRecord()
		}
		if ownerResult.Values[2] != nil {
			return mutationEvidence{}, errs.New(errs.KindResourceInUse, "release group tenant deletion is in progress")
		}
		conditions = append(conditions,
			infraetcd.Condition{Key: tenantKey(project.TenantID), ModRevision: ownerResult.Values[1].ModRevision},
			infraetcd.Condition{Key: deletionKey("tenant", project.TenantID)},
		)
	}

	projectionMembers := make(map[string]struct{}, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		if ids.Validate(ids.KindService, desired.Desired.ID) != nil {
			return mutationEvidence{}, corruptRecord()
		}
		if _, duplicate := projectionMembers[desired.Desired.ID]; duplicate {
			return mutationEvidence{}, corruptRecord()
		}
		projectionMembers[desired.Desired.ID] = struct{}{}
	}
	serviceKeys := make([]string, 0, len(group.ServiceIDs)*3)
	for _, serviceID := range group.ServiceIDs {
		serviceKeys = append(
			serviceKeys,
			serviceKey(serviceID),
			serviceOwnerKey(group.EnvironmentID, serviceID),
			deletionKey("service", serviceID),
		)
	}
	serviceResult, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: serviceKeys})
	if err != nil {
		return mutationEvidence{}, err
	}
	if serviceResult == nil || len(serviceResult.Values) != len(serviceKeys) {
		return mutationEvidence{}, corruptRecord()
	}
	for index, serviceID := range group.ServiceIDs {
		recordValue := serviceResult.Values[index*3]
		ownerValue := serviceResult.Values[index*3+1]
		deletionValue := serviceResult.Values[index*3+2]
		if recordValue == nil || ownerValue == nil {
			return mutationEvidence{}, errs.New(
				errs.KindServiceNotFound,
				"release group member service was not found in the environment",
			)
		}
		service, decodeErr := decodeService(recordValue.Value)
		_, enabled := projectionMembers[serviceID]
		if !enabled {
			for _, component := range projection.Components {
				if component.Desired.OwnerID != group.EnvironmentID {
					continue
				}
				for _, generatedServiceID := range component.Runtime.GeneratedServices {
					if generatedServiceID == serviceID {
						enabled = true
						break
					}
				}
				if enabled {
					break
				}
			}
		}
		if decodeErr != nil || service.Desired.ID != serviceID || service.EnvironmentID != group.EnvironmentID ||
			!bytes.Equal(ownerValue.Value, []byte(serviceID)) {
			return mutationEvidence{}, corruptRecord()
		}
		if !enabled {
			return mutationEvidence{}, errs.New(
				errs.KindResourceInUse,
				"release group member service is not enabled in the environment compose project",
			)
		}
		if deletionValue != nil {
			return mutationEvidence{}, errs.New(
				errs.KindResourceInUse,
				"release group member service deletion is in progress",
			)
		}
		conditions = append(conditions, infraetcd.Condition{Key: deletionKey("service", serviceID)})
	}
	return mutationEvidence{conditions: conditions, epochValue: epochValue}, nil
}

func (store *Store) find(ctx context.Context, id string) (Versioned, bool, error) {
	result, err := store.backend.Get(ctx, recordKey(id))
	if err != nil {
		return Versioned{}, false, err
	}
	if result == nil {
		return Versioned{}, false, errs.New(errs.KindInternal, "release group read result is empty")
	}
	if result.Entry == nil {
		return Versioned{ReadRevision: result.ReadRevision}, false, nil
	}
	group, err := decodeGroup(result.Entry.Value)
	if err != nil {
		return Versioned{}, false, err
	}
	if group.ID != id {
		return Versioned{}, false, corruptRecord()
	}
	indexes, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{
		Keys:     []string{ownerKey(group.EnvironmentID, id), nameKey(group.EnvironmentID, group.Name)},
		Revision: result.ReadRevision,
	})
	if err != nil {
		return Versioned{}, false, err
	}
	if indexes == nil || indexes.ReadRevision != result.ReadRevision || len(indexes.Values) != 2 ||
		indexes.Values[0] == nil || indexes.Values[1] == nil ||
		!bytes.Equal(indexes.Values[0].Value, []byte(id)) ||
		!bytes.Equal(indexes.Values[1].Value, []byte(id)) {
		return Versioned{}, false, corruptRecord()
	}
	return Versioned{Group: group, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision}, true, nil
}

func (store *Store) classifyConflict(ctx context.Context, desired domain.Group, expectedRevision int64) error {
	lock, err := store.backend.GetMany(ctx, infraetcd.GetManyRequest{Keys: []string{
		environmentOperationLockKey(desired.EnvironmentID), environmentDeletionKey(desired.EnvironmentID),
		nameKey(desired.EnvironmentID, desired.Name), recordKey(desired.ID),
	}})
	if err != nil {
		return err
	}
	if lock != nil && len(lock.Values) == 4 {
		if lock.Values[0] != nil || lock.Values[1] != nil {
			return errs.New(errs.KindResourceInUse, "release group environment has an active operation or deletion")
		}
		if lock.Values[2] != nil && !bytes.Equal(lock.Values[2].Value, []byte(desired.ID)) {
			return errs.New(errs.KindNameConflict, "release group name already exists in the environment")
		}
		if expectedRevision > 0 && lock.Values[3] == nil {
			return errs.New(errs.KindReleaseGroupNotFound, "release group was not found")
		}
	}
	return errs.New(errs.KindStateConflict, "release group durable state changed")
}
