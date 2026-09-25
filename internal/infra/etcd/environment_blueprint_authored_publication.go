package etcd

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintAuthoredPublication accepts immutable operator input and one
// visible reconciliation parent, without claiming that fact-dependent runtime
// files or host effects already exist. The coordinator publishes private unit
// Tasks only after this head and parent are atomically visible.
type BlueprintAuthoredPublication struct {
	Project                keyvalue.Versioned[hierarchy.ProjectRecord]
	Environment            keyvalue.Versioned[hierarchy.EnvironmentRecord]
	ExpectedHeadRevision   int64
	Claim                  blueprints.EnvironmentBlueprintStageClaim
	DesiredInput           projectionrecord.EnvironmentDesiredInput
	OwnedIdentities        projectionrecord.EnvironmentOwnedIdentities
	AttachInputGenerations []BlueprintAttachInputGenerationPublication
	Parent                 TaskRecord
	Marker                 idempotency.IdempotencyMarker
}

func (repository *EnvironmentBlueprintRepository) PublishEnvironmentBlueprintAuthoredRevision(
	ctx context.Context, input BlueprintAuthoredPublication,
) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	project, environment, parent, claim := input.Project, input.Environment, input.Parent, input.Claim
	if err := hierarchy.ValidateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchy.ValidateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Revision <= 0 || environment.Revision <= 0 ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != hierarchy.EnvironmentProvisioningReady ||
		input.ExpectedHeadRevision < 0 || claim.BaselineHeadRevision != input.ExpectedHeadRevision ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceApply ||
		claim.EnvironmentID != environment.Record.ID || claim.RevisionID != parent.ID || claim.TaskID != parent.ID ||
		parent.Executor != taskjournal.TaskExecutorBlueprint || parent.Actor != taskjournal.TaskActorOperator ||
		parent.Type != taskjournal.TaskUpdate || parent.Target != environment.Record.ID ||
		parent.Status != taskjournal.TaskStatusPending ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != claim.RevisionID ||
		input.DesiredInput.EnvironmentID != claim.EnvironmentID ||
		input.DesiredInput.RevisionID != claim.RevisionID ||
		input.DesiredInput.RenderGeneration != claim.RenderGeneration ||
		input.OwnedIdentities.EnvironmentID != claim.EnvironmentID ||
		input.OwnedIdentities.RevisionID != claim.RevisionID ||
		input.OwnedIdentities.RenderGeneration != claim.RenderGeneration ||
		input.Marker.TaskID != parent.ID || input.Marker.Locator != claim.Locator ||
		!input.Marker.CreatedAt.Equal(parent.CreatedAt) ||
		!input.Marker.UpdatedAt.Equal(parent.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint authored publication identity is invalid",
		)
	}
	if err := ValidateTaskRecord(parent); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	ownedIdentityValue, err := projectionrecord.EncodeEnvironmentOwnedIdentities(input.OwnedIdentities)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(ownedIdentityValue)
	initialAbsence, err := blueprintunits.PrepareInitialAbsencePublication(
		claim.EnvironmentID,
		claim.RevisionID,
		newbornBlueprintUnitTargets(input.OwnedIdentities, claim.RevisionID),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer initialAbsence.Clear()
	if err := idempotency.ValidateIdempotencyMarker(input.Marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, input.Marker); err != nil ||
		found {
		return existing, err
	}
	desiredValue, err := projectionrecord.EncodeEnvironmentDesiredInputStorage(input.DesiredInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(desiredValue)
	revision := blueprints.EnvironmentDesiredRevisionIdentity{
		EnvironmentID: environment.Record.ID, RevisionID: claim.RevisionID,
	}
	publication, err := repository.prepareEnvironmentBlueprintPublicationWithDigest(
		ctx, claim, revision, sha256.Sum256(desiredValue), parent, input.Marker,
		input.ExpectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	parent = cloneTaskRecord(parent)
	if parent.IdempotencyKey == "" {
		parent.IdempotencyKey = input.Marker.Locator.Key
	}
	parent.idempotencyMarker = cloneIdempotencyLocator(&input.Marker.Locator)
	parentValue, err := EncodeTaskRecord(parent)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(parentValue)
	reference, err := idempotency.EncodeTaskReference(parent.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(parent.ID)},
		{Key: taskjournal.TaskOperationIndexKey(parent.OperationID, parent.ID)},
		{Key: taskjournal.TaskActiveOperationKey(parent.OperationID)},
		{Key: taskjournal.TaskQueueKey(parent.Executor, parent.ID)},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		{Key: blueprints.EnvironmentBlueprintOwnedIdentitiesKey(revision.EnvironmentID, revision.RevisionID)},
		{Key: removalrecord.EnvironmentLockKey(environment.Record.ID)},
	}
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(parent.ID), Value: parentValue},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(parent.OperationID, parent.ID),
			Value: reference,
		},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskActiveOperationKey(parent.OperationID), Value: reference},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskQueueKey(parent.Executor, parent.ID), Value: reference},
		{Type: keyvalue.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: keyvalue.MutationDelete, Key: publication.locatorKey},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintHeadKey(revision.EnvironmentID),
			Value: reference,
		},
		{
			Type:  keyvalue.MutationPut,
			Key:   blueprints.EnvironmentBlueprintOwnedIdentitiesKey(revision.EnvironmentID, revision.RevisionID),
			Value: ownedIdentityValue,
		},
	}
	conditions = append(conditions, initialAbsence.Conditions()...)
	mutations = append(mutations, initialAbsence.Mutations()...)
	tenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx, repository.store, project, project.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(tenant, project, environment, parent.Actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	attachInputs, err := repository.prepareBlueprintAttachInputGenerations(
		ctx, parent, input.AttachInputGenerations,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer attachInputs.clear()
	conditions = append(conditions, attachInputs.conditions...)
	mutations = append(mutations, attachInputs.mutations...)
	baseCount := len(conditions)
	classifier := func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) < baseCount {
			return errs.New(errs.KindInternal, "Blueprint authored publication compare evidence is incomplete")
		}
		return errs.New(errs.KindStateConflict, "Blueprint authored publication raced")
	}
	plan, err := newTaskIdempotencyMutationPlan(parent, initiation, conditions, mutations, classifier)
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, IdempotencyTransactionResult{}, err)
	}
	idempotencyRepository, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return attachInputs.finish(ctx, repository.store, parent.ID, IdempotencyTransactionResult{}, err)
	}
	result, publicationErr := idempotencyRepository.applyEnvironmentBlueprint(
		ctx, input.Marker, plan, repository.transactions,
	)
	return attachInputs.finish(ctx, repository.store, parent.ID, result, publicationErr)
}

func newbornBlueprintUnitTargets(
	identities projectionrecord.EnvironmentOwnedIdentities, birthRevisionID string,
) []blueprintunits.ResourceKey {
	targets := make([]blueprintunits.ResourceKey, 0,
		len(identities.Services)+len(identities.Networks)+len(identities.Volumes)+
			len(identities.Entries)+len(identities.Routes)+len(identities.Attaches)+
			len(identities.Components)+len(identities.Scripts))
	for _, identity := range identities.Services {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindService, ID: identity.ID})
		}
	}
	for _, identity := range identities.Networks {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindNetwork, ID: identity.ID})
		}
	}
	for _, identity := range identities.Volumes {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindVolume, ID: identity.ID})
		}
	}
	for _, identity := range identities.Entries {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: identity.ID})
		}
	}
	for _, identity := range identities.Routes {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindRoute, ID: identity.ID})
		}
	}
	for _, identity := range identities.Attaches {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindAttach, ID: identity.ID})
		}
	}
	for _, identity := range identities.Components {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: identity.ID})
		}
	}
	for _, identity := range identities.Scripts {
		if identity.BirthRevisionID == birthRevisionID {
			targets = append(targets, blueprintunits.ResourceKey{Kind: ids.KindScript, ID: identity.ID})
		}
	}
	return targets
}
