package etcd

import (
	"context"
	"fmt"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentVolumeAuditPageSize = 128

// ValidateEnvironmentVolumeDirs scans every persisted Environment before the
// Controller serves requests. A record authored under another root or owner
// shape is corruption, not a path to adopt or rewrite automatically.
func (repository *HierarchyRepository) ValidateEnvironmentVolumeDirs(
	ctx context.Context,
	volumeRoot string,
) error {
	if err := environmentpath.ValidateRoot(volumeRoot); err != nil {
		return err
	}
	tenantCursor := ""
	for {
		tenants, err := repository.ListTenants(ctx, PageRequest{
			Limit: environmentVolumeAuditPageSize, Cursor: tenantCursor,
		})
		if err != nil {
			return err
		}
		for _, tenant := range tenants.Items {
			if err := repository.validateTenantEnvironmentVolumeDirs(ctx, volumeRoot, tenant.Record.ID); err != nil {
				return err
			}
		}
		if tenants.NextCursor == "" {
			break
		}
		tenantCursor = tenants.NextCursor
	}

	backingCursor := ""
	for {
		projects, err := repository.ListBackingProjects(ctx, PageRequest{
			Limit: environmentVolumeAuditPageSize, Cursor: backingCursor,
		})
		if err != nil {
			return err
		}
		for _, project := range projects.Items {
			if err := repository.validateProjectEnvironmentVolumeDirs(ctx, volumeRoot, project.Record); err != nil {
				return err
			}
		}
		if projects.NextCursor == "" {
			return nil
		}
		backingCursor = projects.NextCursor
	}
}

func (repository *HierarchyRepository) validateTenantEnvironmentVolumeDirs(
	ctx context.Context,
	volumeRoot string,
	tenantID string,
) error {
	projectCursor := ""
	for {
		projects, err := repository.ListTenantProjects(ctx, tenantID, PageRequest{
			Limit: environmentVolumeAuditPageSize, Cursor: projectCursor,
		})
		if err != nil {
			return err
		}
		for _, project := range projects.Items {
			if err := repository.validateProjectEnvironmentVolumeDirs(ctx, volumeRoot, project.Record); err != nil {
				return err
			}
		}
		if projects.NextCursor == "" {
			return nil
		}
		projectCursor = projects.NextCursor
	}
}

func (repository *HierarchyRepository) validateProjectEnvironmentVolumeDirs(
	ctx context.Context,
	volumeRoot string,
	project hierarchyrecord.ProjectRecord,
) error {
	environmentCursor := ""
	for {
		environments, err := repository.ListEnvironments(ctx, project.ID, PageRequest{
			Limit: environmentVolumeAuditPageSize, Cursor: environmentCursor,
		})
		if err != nil {
			return err
		}
		for _, environment := range environments.Items {
			if err := hierarchyrecord.ValidateEnvironmentVolumeDir(volumeRoot, project, environment.Record); err != nil {
				return errs.Wrap(errs.KindInternal, fmt.Errorf(
					"persisted environment %s volume_dir invariant failed: %w",
					environment.Record.ID,
					err,
				))
			}
		}
		if environments.NextCursor == "" {
			return nil
		}
		environmentCursor = environments.NextCursor
	}
}
