package releasegroups

import (
	"bytes"
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/releasegroup"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Preparer) prepareReleaseGroupProjectEvidence(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
) ([]etcdstore.Condition, error) {
	projectResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.ProjectKey(environment.ProjectID),
		hierarchyrecord.EnvironmentOwnerKey(environment.ProjectID, environment.ID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), environment.ProjectID),
	}})
	if err != nil {
		return nil, err
	}
	if projectResult == nil || len(projectResult.Values) != 3 ||
		projectResult.Values[0] == nil || projectResult.Values[1] == nil {
		return nil, recordcodec.CorruptRecord()
	}
	project, err := hierarchyrecord.DecodeProject(projectResult.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID ||
		!bytes.Equal(projectResult.Values[1].Value, []byte(environment.ID)) {
		return nil, recordcodec.CorruptRecord()
	}
	if projectResult.Values[2] != nil {
		return nil, errs.New(
			errs.KindResourceInUse,
			"release group project deletion is in progress",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: hierarchyrecord.ProjectKey(project.ID), ModRevision: projectResult.Values[0].ModRevision},
		{
			Key:         hierarchyrecord.EnvironmentOwnerKey(project.ID, environment.ID),
			ModRevision: projectResult.Values[1].ModRevision,
		},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), project.ID)},
	}
	ownerIndex := hierarchyrecord.ProjectOwnerKey(project)
	ownerKeys := []string{ownerIndex}
	if project.Kind == hierarchyrecord.ProjectKindTenant {
		ownerKeys = append(ownerKeys,
			hierarchyrecord.TenantKey(project.TenantID),
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
		)
	}
	ownerResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: ownerKeys})
	if err != nil {
		return nil, err
	}
	if ownerResult == nil || len(ownerResult.Values) != len(ownerKeys) ||
		ownerResult.Values[0] == nil ||
		!bytes.Equal(ownerResult.Values[0].Value, []byte(project.ID)) {
		return nil, recordcodec.CorruptRecord()
	}
	conditions = append(conditions,
		etcdstore.Condition{Key: ownerIndex, ModRevision: ownerResult.Values[0].ModRevision},
	)
	if project.Kind == hierarchyrecord.ProjectKindTenant {
		if ownerResult.Values[1] == nil {
			return nil, recordcodec.CorruptRecord()
		}
		tenant, tenantErr := hierarchyrecord.DecodeTenant(ownerResult.Values[1].Value)
		if tenantErr != nil || tenant.ID != project.TenantID {
			return nil, recordcodec.CorruptRecord()
		}
		if ownerResult.Values[2] != nil {
			return nil, errs.New(
				errs.KindResourceInUse,
				"release group tenant deletion is in progress",
			)
		}
		conditions = append(
			conditions,
			etcdstore.Condition{
				Key:         hierarchyrecord.TenantKey(project.TenantID),
				ModRevision: ownerResult.Values[1].ModRevision,
			},
			etcdstore.Condition{
				Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), project.TenantID),
			},
		)
	}
	return conditions, nil
}

func (repository *Preparer) prepareReleaseGroupMemberEvidence(
	ctx context.Context,
	group domain.Group,
	projection projectionrecord.EnvironmentComposeProjection,
) ([]etcdstore.Condition, error) {
	desiredMembers := make(map[string]servicerecord.EnvironmentServiceProjection, len(projection.DesiredServices))
	for _, desired := range projection.DesiredServices {
		if ids.Validate(ids.KindService, desired.Desired.ID) != nil {
			return nil, recordcodec.CorruptRecord()
		}
		if _, duplicate := desiredMembers[desired.Desired.ID]; duplicate {
			return nil, recordcodec.CorruptRecord()
		}
		desiredMembers[desired.Desired.ID] = desired
	}
	keys := make([]string, 0, len(group.ServiceIDs))
	for _, serviceID := range group.ServiceIDs {
		keys = append(keys, deletionrecord.TombstoneKey("service", serviceID))
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return nil, err
	}
	if result == nil || len(result.Values) != len(keys) {
		return nil, recordcodec.CorruptRecord()
	}
	conditions := make([]etcdstore.Condition, 0, len(group.ServiceIDs))
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
		conditions = append(conditions, etcdstore.Condition{
			Key: deletionrecord.TombstoneKey("service", serviceID),
		})
	}
	return conditions, nil
}

func releaseGroupTargetsGeneratedService(
	components []componentrecord.Record,
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
