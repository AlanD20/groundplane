package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type EntryWorkPublication struct {
	Project                         keyvalue.Versioned[hierarchyrecord.ProjectRecord]
	Environment                     keyvalue.Versioned[hierarchyrecord.EnvironmentRecord]
	Claim                           blueprints.EnvironmentBlueprintStageClaim
	Projection                      projectionrecord.EnvironmentComposeProjection
	EpochRevision                   int64
	Task                            workexecution.Task
	Item                            workexecution.Item
	InputID                         string
	Input                           *agentpb.EntryConfigurationWorkInput
	Capture                         entrywork.Capture
	Secrets                         tasksecretpins.Prepared
	Marker                          idempotency.IdempotencyMarker
	PreparationConditions           []keyvalue.Condition
	DefinitionConfigurationRevision int64
}

// PublishEntryConfigurationWork atomically accepts a direct Entry action.
// Removal keeps the staged desired head private until acknowledged file cleanup.
func (repository *HierarchyRepository) PublishEntryConfigurationWork(ctx context.Context, request EntryWorkPublication) (IdempotencyTransactionResult, error) {
	project, environment, claim := request.Project, request.Environment, request.Claim
	task, item := request.Task, request.Item
	if ctx == nil || repository == nil || hierarchyrecord.ValidateProject(project.Record) != nil ||
		hierarchyrecord.ValidateEnvironment(environment.Record) != nil || task.Validate() != nil || item.Validate() != nil ||
		blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil || project.Revision <= 0 || environment.Revision <= 0 ||
		environment.Record.ProjectID != project.Record.ID || environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		task.Scope.ProjectID != project.Record.ID || task.Scope.EnvironmentID != environment.Record.ID ||
		claim.EnvironmentID != environment.Record.ID || claim.TaskID != task.ID || claim.RevisionID != task.ID ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation || request.EpochRevision <= 0 ||
		request.Projection.EnvironmentID != environment.Record.ID || request.Projection.RevisionID != task.ID ||
		request.Projection.RenderGeneration != claim.RenderGeneration ||
		(request.Input == nil && (task.Action != workexecution.ActionRemove || request.Capture.Removal == nil ||
			item.Placement.Executor != workexecution.Controller || !request.Secrets.IsZero())) ||
		request.Marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		request.Marker.Locator.ScopeID != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "Entry publication authority is inconsistent")
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil || found {
		return existing, err
	}
	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, project, environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if epoch, found := fence.EpochRevision(); !found || epoch != request.EpochRevision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Entry running consumers changed before publication")
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(ctx, claim,
		blueprints.EnvironmentDesiredRevisionIdentity{EnvironmentID: environment.Record.ID, RevisionID: task.ID},
		request.Projection, desiredPublicationOwner{TaskID: task.ID, EnvironmentID: environment.Record.ID,
			RevisionID: task.ID, RenderGeneration: claim.RenderGeneration}, request.Marker, claim.BaselineHeadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	identities, err := prepareEnvironmentDesiredIdentityPublication(ctx, repository.store, request.Projection,
		claim.SourceKind, claim.BaselineHeadRevision, fence.ReadRevision())
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer identities.clear()
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	guards := fence.TransactionConditions()
	guards = append(guards, request.PreparationConditions...)
	guards = append(guards, identities.conditions...)
	if request.Capture.Removal != nil {
		scripts, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, request.Capture.Removal.EntryID, fence.ReadRevision())
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		guards = append(guards, scripts...)
	}
	guards = append(guards,
		keyvalue.Condition{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		keyvalue.Condition{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		keyvalue.Condition{Key: blueprints.EnvironmentBlueprintRootKey(environment.Record.ID, task.ID), ModRevision: publication.rootRevision},
		keyvalue.Condition{Key: blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), ModRevision: claim.BaselineHeadRevision})
	if request.Input != nil {
		configuration, err := sources.WorkConditions(ctx, request.Capture.Configuration, fence.ReadRevision())
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		for _, condition := range configuration {
			guards = append(guards, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
		}
	}
	inputs, err := entrywork.NewInputRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	var change workqueue.Change
	if request.Input == nil {
		change, err = repository.prepareEntryDefinitionPublication(ctx, request, inputs, sources, fence.ReadRevision(), guards)
	} else {
		_, change, err = inputs.PreparePublication(ctx, task, item, request.InputID, request.Input,
			request.Capture, request.Secrets, request.Marker, guards, time.Now().UTC())
	}
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	for _, mutation := range identities.mutations {
		mutation.Value = append([]byte(nil), mutation.Value...)
		change.Mutations = append(change.Mutations, mutation)
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	change.Mutations = append(change.Mutations, epoch)
	if request.Input != nil && !request.Input.Removal {
		reference, err := idempotency.EncodeTaskReference(task.ID)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		change.Mutations = append(change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: blueprints.EnvironmentBlueprintHeadKey(environment.Record.ID), Value: reference},
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: publication.descriptorKey, Value: append([]byte(nil), publication.publishedDescriptor...)},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: publication.locatorKey})
	}
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	result, err := queue.Commit(ctx, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if result.Succeeded {
		return IdempotencyTransactionResult{kind: idempotencyTransactionApplied, revision: result.Revision}, nil
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil || found {
		return existing, err
	}
	return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Entry publication changed before atomic acceptance")
}
