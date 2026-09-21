package hierarchydeletionplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
)

func (repository *Planner) freezeTenantMembership(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
) ([]HierarchyDeletionMembershipNode, error) {
	projects, err := repository.hierarchyDeletionIndexedTargets(
		ctx, operation.SnapshotRevision, hierarchyrecord.ProjectTenantOwnerPrefix(operation.TargetID),
		hierarchyrecord.ProjectKey, ids.KindProject, func(value []byte, id, owner string) error {
			record, decodeErr := hierarchyrecord.DecodeProject(value)
			if decodeErr != nil || record.ID != id || record.TenantID != owner || record.Kind != hierarchyrecord.ProjectKindTenant {
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			return nil
		}, operation.TargetID,
	)
	if err != nil {
		return nil, err
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	for _, project := range projects {
		children, freezeErr := repository.freezeProjectMembership(ctx, operation, project.id, false)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, children...)
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"project:"+project.id+":finalize", "project", project.id,
			hierarchydeletion.HierarchyDeletionProjectFinalize, project.revision,
			terminalHierarchyDeletionNodes(children), "project.finalize", project.digest,
		))
	}
	runners, err := repository.freezeIndexedResource(ctx, operation, operation.TargetID,
		hierarchyDeletionIndexedResource{
			targetKind: "runner", actionKind: hierarchydeletion.HierarchyDeletionRunnerLocalRemove,
			ownerPrefix: func(owner string) string {
				return runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerTenant, owner)
			},
			primaryKey: runnerrecord.RunnerKey, stableIDKind: ids.KindRunner, controller: true,
			validateOwner: validateHierarchyDeletionRunnerOwner,
		})
	if err != nil {
		return nil, err
	}
	return append(nodes, runners...), nil
}

func (repository *Planner) freezeProjectMembership(
	ctx context.Context,
	operation hierarchydeletion.HierarchyDeletionTombstone,
	projectID string,
	root bool,
) ([]HierarchyDeletionMembershipNode, error) {
	environments, err := repository.hierarchyDeletionIndexedTargets(
		ctx, operation.SnapshotRevision, hierarchyrecord.EnvironmentOwnerPrefix(projectID), hierarchyrecord.EnvironmentKey,
		ids.KindEnvironment, func(value []byte, id, owner string) error {
			record, decodeErr := hierarchyrecord.DecodeEnvironment(value)
			if decodeErr != nil || record.ID != id || record.ProjectID != owner {
				return hierarchydeletion.CorruptHierarchyDeletion()
			}
			return nil
		}, projectID,
	)
	if err != nil {
		return nil, err
	}
	nodes := make([]HierarchyDeletionMembershipNode, 0)
	for _, environment := range environments {
		children, freezeErr := repository.freezeEnvironmentMembership(
			ctx,
			operation,
			environment.id,
			environment.revision,
			environment.digest,
		)
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, children...)
		nodes = append(nodes, hierarchyDeletionControllerNode(
			"environment:"+environment.id+":finalize", "environment", environment.id,
			hierarchydeletion.HierarchyDeletionEnvironmentFinalize, environment.revision,
			terminalHierarchyDeletionNodes(children), "environment.finalize", environment.digest,
		))
	}
	if operation.OperationKind != hierarchydeletion.HierarchyDeletionOperationBacking || !root {
		runners, freezeErr := repository.freezeIndexedResource(ctx, operation, projectID,
			hierarchyDeletionIndexedResource{
				targetKind: "runner", actionKind: hierarchydeletion.HierarchyDeletionRunnerLocalRemove,
				ownerPrefix: func(owner string) string {
					return runnerrecord.RunnerOwnerPrefix(runnerrecord.RunnerOwnerProject, owner)
				},
				primaryKey: runnerrecord.RunnerKey, stableIDKind: ids.KindRunner, controller: true,
				validateOwner: validateHierarchyDeletionRunnerOwner,
			})
		if freezeErr != nil {
			return nil, freezeErr
		}
		nodes = append(nodes, runners...)
	}
	secrets, err := repository.freezeIndexedResource(ctx, operation, projectID,
		hierarchyDeletionIndexedResource{
			targetKind: "secret", actionKind: hierarchydeletion.HierarchyDeletionProjectSecretRemove,
			ownerPrefix: func(owner string) string {
				return secretrecord.SecretOwnerCollectionPrefix(core.SecretScopeProject, owner)
			},
			primaryKey: secretrecord.RecordKey, stableIDKind: ids.KindSecret, controller: true,
			validateOwner: validateHierarchyDeletionSecretOwner,
		})
	if err != nil {
		return nil, err
	}
	return append(nodes, secrets...), nil
}
