package backupruntime

import (
	"context"

	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// BackupOrphanMutationBinding fences an ordinary orphan mutation against an
// Environment or ancestor hierarchy deletion and advances the same ancestry
// epochs used by deletion publication.
type BackupOrphanMutationBinding struct {
	conditions []keyvalue.Condition
	mutations  []keyvalue.Mutation
	clear      func()
}

func (binding BackupOrphanMutationBinding) Conditions() []keyvalue.Condition {
	return binding.conditions
}
func (binding BackupOrphanMutationBinding) Mutations() []keyvalue.Mutation { return binding.mutations }
func (binding *BackupOrphanMutationBinding) Clear() {
	if binding != nil && binding.clear != nil {
		binding.clear()
	}
	binding.conditions = nil
	binding.mutations = nil
	binding.clear = nil
}

func (repository *Writer) BindBackupOrphanMutation(ctx context.Context,
	environmentID string,
	revision int64,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (BackupOrphanMutationBinding, error) {
	hierarchyRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchyrecord.EnvironmentKey(environmentID)}, Revision: revision,
	})
	if err != nil {
		return BackupOrphanMutationBinding{}, err
	}
	if hierarchyRead == nil || hierarchyRead.ReadRevision != revision || len(hierarchyRead.Values) != 1 ||
		hierarchyRead.Values[0] == nil {
		return BackupOrphanMutationBinding{}, CorruptBackupRuntimeRecord()
	}
	environment, err := hierarchyrecord.DecodeEnvironment(hierarchyRead.Values[0].Value)
	keyvalue.ClearValues(hierarchyRead.Values)
	if err != nil || environment.ID != environmentID {
		return BackupOrphanMutationBinding{}, CorruptBackupRuntimeRecord()
	}
	projectRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchyrecord.ProjectKey(environment.ProjectID)}, Revision: revision,
	})
	if err != nil {
		return BackupOrphanMutationBinding{}, err
	}
	if projectRead == nil || projectRead.ReadRevision != revision || len(projectRead.Values) != 1 ||
		projectRead.Values[0] == nil {
		return BackupOrphanMutationBinding{}, CorruptBackupRuntimeRecord()
	}
	project, err := hierarchyrecord.DecodeProject(projectRead.Values[0].Value)
	keyvalue.ClearValues(projectRead.Values)
	if err != nil || project.ID != environment.ProjectID {
		return BackupOrphanMutationBinding{}, CorruptBackupRuntimeRecord()
	}
	projectKind := hierarchydeletion.HierarchyDeletionTargetProject
	tenantID := project.TenantID
	if project.Kind == hierarchyrecord.ProjectKindBacking {
		projectKind = hierarchydeletion.HierarchyDeletionTargetBacking
		tenantID = ""
	}
	conditions = append(append([]keyvalue.Condition(nil), conditions...),
		keyvalue.Condition{Key: hierarchydeletion.HierarchyDeletionTombstoneKey("environment", environmentID)},
		keyvalue.Condition{Key: hierarchydeletion.HierarchyDeletionTombstoneKey(string(projectKind), project.ID)},
	)
	if tenantID != "" {
		conditions = append(conditions, keyvalue.Condition{
			Key: hierarchydeletion.HierarchyDeletionTombstoneKey("tenant", tenantID),
		})
	}
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, revision,
		hierarchydeletion.MutationScope{TenantID: tenantID, ProjectID: project.ID}, conditions, mutations)
	if err != nil {
		return BackupOrphanMutationBinding{}, err
	}
	return BackupOrphanMutationBinding{conditions: ancestry.Conditions(), mutations: ancestry.Mutations(),
		clear: ancestry.Clear}, nil
}
