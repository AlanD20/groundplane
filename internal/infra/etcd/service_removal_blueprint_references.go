package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalreferences"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The retained declaration identifies what to remove, not whether removal is
// permitted. The accepted Blueprint and live references are checked separately.
// Callers join these guards with the Environment epoch and deletion fence.
func (repository *ServiceRepository) prepareBlueprintRemovalReferences(ctx context.Context, input serviceremovalwork.Input, revision int64) ([]keyvalue.Condition, error) {
	desired, found, err := blueprints.ReadDesiredInputRevision(ctx, repository.store,
		input.Scope.EnvironmentID, input.DesiredRevisionID, revision)
	if err != nil {
		return nil, err
	}
	if !found || desired.Record.RenderGeneration != input.Generation {
		return nil, errs.New(errs.KindStateConflict, "Service removal's captured Blueprint is unavailable")
	}
	if err := requireServiceDeclarationUnreferenced(desired.Record, input.ServiceName); err != nil {
		return nil, err
	}
	references, err := serviceremovalreferences.PrepareDeclarationRemoval(ctx, repository.store,
		input.Scope.EnvironmentID, input.ServiceID, revision)
	if err != nil {
		return nil, err
	}
	return append(references.Conditions(), keyvalue.Condition{
		Key: blueprints.EnvironmentBlueprintRootKey(input.Scope.EnvironmentID, input.DesiredRevisionID), ModRevision: desired.Revision,
	}), nil
}
