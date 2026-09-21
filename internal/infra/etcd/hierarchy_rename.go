package etcd

import (
	"context"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyRepository) RenameTenant(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.TenantRecord], error) {
	current, err := repository.GetTenant(ctx, id)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, recordcodec.StateConflict("tenant", id)
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := hierarchyrecord.ValidateTenant(replacement); err != nil {
		return etcdstore.Versioned[hierarchyrecord.TenantRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		hierarchyrecord.TenantKey(id),
		hierarchyrecord.TenantSlugKey(current.Record.Slug),
		hierarchyrecord.TenantSlugKey(slug),
		nil,
		deletions.TombstoneKey("tenant", id),
		"tenant",
		id,
		errs.KindTenantNotFound,
		hierarchyrecord.EncodeTenant,
	)
}

// RenameTenantProject renames only ordinary tenant-owned Projects. Backing
// Project lifecycle is owned by its facade and is not exposed through this seam.
func (repository *HierarchyRepository) RenameTenantProject(
	ctx context.Context,
	id string,
	expectedRevision int64,
	slug string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	current, err := repository.GetProject(ctx, id)
	if err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	if current.Revision != expectedRevision {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, recordcodec.StateConflict("project", id)
	}
	if current.Record.Kind != hierarchyrecord.ProjectKindTenant {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	replacement := current.Record
	replacement.Slug = slug
	if err := hierarchyrecord.ValidateProject(replacement); err != nil {
		return etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, err
	}
	return renameRecord(
		ctx,
		repository.store,
		current,
		replacement,
		hierarchyrecord.ProjectKey(id),
		hierarchyrecord.ProjectSlugKey(current.Record),
		hierarchyrecord.ProjectSlugKey(replacement),
		[]string{hierarchyrecord.ProjectOwnerKey(current.Record)},
		deletions.TombstoneKey("project", id),
		"project",
		id,
		errs.KindProjectNotFound,
		hierarchyrecord.EncodeProject,
	)
}
