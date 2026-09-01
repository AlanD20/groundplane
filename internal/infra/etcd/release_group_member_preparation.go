package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareReleaseGroupProjectEvidence(
	ctx context.Context,
	environment EnvironmentRecord,
) ([]Condition, error) {
	projectResult, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		projectKey(environment.ProjectID),
		environmentOwnerKey(environment.ProjectID, environment.ID),
		deletionTombstoneKey(string(DeletionTargetProject), environment.ProjectID),
	}})
	if err != nil {
		return nil, err
	}
	if projectResult == nil || len(projectResult.Values) != 3 ||
		projectResult.Values[0] == nil || projectResult.Values[1] == nil {
		return nil, corruptRecord()
	}
	project, err := decodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID ||
		!bytes.Equal(projectResult.Values[1].Value, []byte(environment.ID)) {
		return nil, corruptRecord()
	}
	if projectResult.Values[2] != nil {
		return nil, errs.New(
			errs.KindResourceInUse,
			"release group project deletion is in progress",
		)
	}
	conditions := []Condition{
		{Key: projectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision},
		{Key: environmentOwnerKey(project.ID, environment.ID), ModRevision: projectResult.Values[1].ModRevision},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.ID)},
	}
	ownerIndex := projectOwnerKey(project)
	ownerKeys := []string{ownerIndex}
	if project.Kind == ProjectKindTenant {
		ownerKeys = append(ownerKeys,
			tenantKey(project.TenantID),
			deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID),
		)
	}
	ownerResult, err := repository.store.GetMany(ctx, GetManyRequest{Keys: ownerKeys})
	if err != nil {
		return nil, err
	}
	if ownerResult == nil || len(ownerResult.Values) != len(ownerKeys) ||
		ownerResult.Values[0] == nil ||
		!bytes.Equal(ownerResult.Values[0].Value, []byte(project.ID)) {
		return nil, corruptRecord()
	}
	conditions = append(conditions,
		Condition{Key: ownerIndex, ModRevision: ownerResult.Values[0].ModRevision},
	)
	if project.Kind == ProjectKindTenant {
		if ownerResult.Values[1] == nil {
			return nil, corruptRecord()
		}
		tenant, tenantErr := decodeTenant(ownerResult.Values[1].Value)
		if tenantErr != nil || tenant.ID != project.TenantID {
			return nil, corruptRecord()
		}
		if ownerResult.Values[2] != nil {
			return nil, errs.New(
				errs.KindResourceInUse,
				"release group tenant deletion is in progress",
			)
		}
		conditions = append(conditions,
			Condition{Key: tenantKey(project.TenantID), ModRevision: ownerResult.Values[1].ModRevision},
			Condition{Key: deletionTombstoneKey(string(DeletionTargetTenant), project.TenantID)},
		)
	}
	return conditions, nil
}

func (repository *TaskRepository) prepareReleaseGroupMemberEvidence(
	ctx context.Context,
	group domain.Group,
	projection EnvironmentComposeProjection,
) ([]Condition, error) {
	desiredMembers := make(map[string]EnvironmentServiceProjection, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		if ids.Validate(ids.KindService, desired.Desired.ID) != nil {
			return nil, corruptRecord()
		}
		if _, duplicate := desiredMembers[desired.Desired.ID]; duplicate {
			return nil, corruptRecord()
		}
		desiredMembers[desired.Desired.ID] = desired
	}
	keys := make([]string, 0, len(group.ServiceIDs))
	for _, serviceID := range group.ServiceIDs {
		keys = append(keys, deletionTombstoneKey("service", serviceID))
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return nil, corruptRecord()
	}
	conditions := make([]Condition, 0, len(group.ServiceIDs))
	for index, serviceID := range group.ServiceIDs {
		deletionValue := result.Values[index]
		desired, found := desiredMembers[serviceID]
		generated := releaseGroupTargetsGeneratedService(projection.Components, serviceID, group.EnvironmentID)
		if (!found || desired.EnvironmentID != group.EnvironmentID) && !generated {
			return nil, errs.New(
				errs.KindServiceNotFound,
				"release group member service was not found in the environment",
			)
		}
		if deletionValue != nil {
			return nil, errs.New(
				errs.KindResourceInUse,
				"release group member service deletion is in progress",
			)
		}
		// The Environment mutation epoch fences Service record and owner changes.
		// Only deletion start has independent authority, so compare its exact
		// selected member key and no unrelated Service deletion namespace.
		conditions = append(conditions, Condition{
			Key: deletionTombstoneKey("service", serviceID),
		})
	}
	return conditions, nil
}

func releaseGroupTargetsGeneratedService(
	components []ComponentRecord,
	serviceID string,
	environmentID string,
) bool {
	for _, component := range components {
		if component.Desired.OwnerID != environmentID {
			continue
		}
		for _, generatedServiceID := range component.Runtime.GeneratedServices {
			if generatedServiceID == serviceID {
				return true
			}
		}
	}
	return false
}
