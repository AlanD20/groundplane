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
	projectionMembers := make(map[string]struct{}, len(projection.Services))
	for _, identity := range projection.Services {
		if ids.Validate(ids.KindService, identity.ID) != nil {
			return nil, corruptRecord()
		}
		if _, duplicate := projectionMembers[identity.ID]; duplicate {
			return nil, corruptRecord()
		}
		projectionMembers[identity.ID] = struct{}{}
	}
	keys := make([]string, 0, len(group.ServiceIDs)*3)
	for _, serviceID := range group.ServiceIDs {
		keys = append(keys,
			serviceKey(serviceID),
			serviceOwnerKey(group.EnvironmentID, serviceID),
			deletionTombstoneKey("service", serviceID),
		)
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
		recordValue := result.Values[index*3]
		ownerValue := result.Values[index*3+1]
		deletionValue := result.Values[index*3+2]
		if recordValue == nil || ownerValue == nil {
			return nil, errs.New(
				errs.KindServiceNotFound,
				"release group member service was not found in the environment",
			)
		}
		service, decodeErr := decodeServiceRecord(recordValue.Value)
		_, enabled := projectionMembers[serviceID]
		if decodeErr != nil || service.Desired.ID != serviceID ||
			service.EnvironmentID != group.EnvironmentID ||
			!bytes.Equal(ownerValue.Value, []byte(serviceID)) {
			return nil, corruptRecord()
		}
		if !enabled {
			return nil, errs.New(
				errs.KindResourceInUse,
				"release group member service is not enabled in the environment compose project",
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
