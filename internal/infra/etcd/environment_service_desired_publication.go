package etcd

import (
	"context"
	"maps"
	"slices"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentServiceDesiredPublication struct {
	Project              Versioned[ProjectRecord]
	Environment          Versioned[EnvironmentRecord]
	ExpectedHeadRevision int64
	Claim                EnvironmentBlueprintStageClaim
	Revision             EnvironmentDesiredRevisionIdentity
	Projection           EnvironmentComposeProjection
	Change               EnvironmentBlueprintServiceChange
	References           ServiceMutationReferences
	Marker               IdempotencyMarker
}

func (repository *HierarchyRepository) PublishEnvironmentServiceDesiredRevisionDirect(
	ctx context.Context,
	input EnvironmentServiceDesiredPublication,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(input.Project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(input.Environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if input.Project.Record.Kind != ProjectKindTenant || input.Project.Revision <= 0 ||
		input.Environment.Revision <= 0 || input.Project.ReadRevision < input.Project.Revision ||
		input.Environment.ReadRevision < input.Environment.Revision ||
		input.Environment.Record.ProjectID != input.Project.Record.ID ||
		input.Environment.Record.ProvisioningState != EnvironmentProvisioningReady ||
		input.ExpectedHeadRevision < 0 || input.Claim.SourceKind != EnvironmentBlueprintSourceMutation ||
		input.Revision.EnvironmentID != input.Environment.Record.ID ||
		input.Revision.RevisionID != input.Claim.RevisionID || input.Claim.TaskID != input.Claim.RevisionID ||
		input.Projection.EnvironmentID != input.Revision.EnvironmentID ||
		input.Projection.RevisionID != input.Revision.RevisionID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "direct Service desired publication identity is invalid",
		)
	}
	if err := validateServiceMutationMarker(input.Marker, input.Environment.Record.ID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if input.Marker.Locator != input.Claim.Locator ||
		!sameBlueprintProtectedIntent(input.Marker.Intent, input.Claim.Intent) ||
		!input.Marker.CreatedAt.Equal(input.Claim.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "direct Service desired publication marker is invalid",
		)
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, input.Marker); err != nil || found {
		return existing, err
	}
	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, input.Project, input.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := repository.validateDirectServiceHierarchy(ctx, input, fence.readAtRevision()); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previous, found, err := repository.getEnvironmentBlueprintProjectionAtRevision(
		ctx, input.Environment.Record.ID, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if (input.ExpectedHeadRevision == 0 && found) ||
		(input.ExpectedHeadRevision > 0 && (!found || previous.Revision != input.ExpectedHeadRevision)) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if err := validateEnvironmentComposeProjectionAdvance(previous.Record, found, input.Projection); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDirectEnvironmentServiceChange(input); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	referenceConditions, err := serviceMutationReferenceConditions(input.Change.Record, input.References)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := repository.prepareEnvironmentDirectPublication(
		ctx, input.Claim, input.Revision, input.Projection, input.Marker, input.ExpectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)
	headReference, err := encodeTaskReference(input.Revision.RevisionID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(headReference)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)

	serviceID := input.Change.Record.Desired.ID
	conditions := []Condition{
		{Key: environmentBlueprintRootKey(input.Revision.EnvironmentID, input.Revision.RevisionID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(input.Revision.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		{Key: deletionTombstoneKey("service", serviceID)},
	}
	if input.Change.Current == nil {
		conditions = append(conditions, Condition{Key: serviceRuntimeKey(serviceID)})
	}
	conditions = append(conditions, referenceConditions...)
	referenceOffset := len(conditions) - len(referenceConditions)
	fenceOffset := len(conditions)
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []Mutation{
		{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: MutationDelete, Key: publication.locatorKey},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(input.Revision.EnvironmentID), Value: headReference},
	}
	if input.Change.Current == nil {
		runtimeValue, runtimeErr := encodeServiceRuntimeRecord(newServiceRuntimeRecord(input.Change.Record))
		if runtimeErr != nil {
			return IdempotencyTransactionResult{}, runtimeErr
		}
		defer clear(runtimeValue)
		mutations = append(mutations, Mutation{Type: MutationPut, Key: serviceRuntimeKey(serviceID), Value: runtimeValue})
	}
	mutations = append(mutations, epochMutation)
	classifier := func(_ int64, values []*KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "direct Service publication compare evidence is incomplete")
		}
		for index := 0; index < 3; index++ {
			if values[index] == nil {
				return errs.New(errs.KindStateConflict, "Environment sealed staging evidence changed")
			}
		}
		if (input.ExpectedHeadRevision == 0 && values[3] != nil) ||
			(input.ExpectedHeadRevision > 0 && (values[3] == nil || values[3].ModRevision != input.ExpectedHeadRevision)) {
			return errs.New(errs.KindStateConflict, "Environment desired state changed")
		}
		if values[4] != nil {
			return errs.New(errs.KindResourceInUse, "Service removal is in progress")
		}
		if input.Change.Current == nil && values[5] != nil {
			return errs.New(errs.KindStateConflict, "Service runtime identity is already in use")
		}
		if err := classifyServiceMutationReferenceConflict(values[referenceOffset:fenceOffset], input.References); err != nil {
			return err
		}
		if conflict := fence.classifyCAS(values[fenceOffset:]); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "direct Service desired publication raced")
	}
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(validateEnvironmentDesiredPublicationBudget); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, input.Marker, plan)
}

func (repository *HierarchyRepository) validateDirectServiceHierarchy(
	ctx context.Context,
	input EnvironmentServiceDesiredPublication,
	readRevision int64,
) error {
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(input.Environment.Record.ID),
			projectKey(input.Project.Record.ID),
		},
		Revision: readRevision,
	})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		return errs.New(errs.KindStateConflict, "Environment Blueprint hierarchy changed")
	}
	defer clearKeyValues(result.Values)
	durableEnvironment, environmentErr := decodeEnvironment(result.Values[0].Value)
	durableProject, projectErr := decodeProject(result.Values[1].Value)
	if environmentErr != nil || projectErr != nil ||
		!equalDirectEnvironmentRecord(durableEnvironment, input.Environment.Record) ||
		!equalDirectProjectRecord(durableProject, input.Project.Record) {
		return errs.New(errs.KindStateConflict, "Environment Blueprint hierarchy changed")
	}
	return nil
}

func equalDirectEnvironmentRecord(left EnvironmentRecord, right EnvironmentRecord) bool {
	return left.ID == right.ID && left.ProjectID == right.ProjectID && left.Name == right.Name &&
		left.NetworkPool == right.NetworkPool && left.VolumeDir == right.VolumeDir &&
		left.ProvisioningState == right.ProvisioningState && left.CreateTaskID == right.CreateTaskID &&
		left.CreatedAt.Equal(right.CreatedAt) && left.DeletionTaskID == right.DeletionTaskID
}

func equalDirectProjectRecord(left ProjectRecord, right ProjectRecord) bool {
	return left.ID == right.ID && left.TenantID == right.TenantID && left.Slug == right.Slug &&
		left.Name == right.Name && left.Description == right.Description && left.Kind == right.Kind &&
		left.DeletionTaskID == right.DeletionTaskID
}

func validateDirectEnvironmentServiceChange(input EnvironmentServiceDesiredPublication) error {
	change := input.Change
	if err := validateServiceRecord(change.Record); err != nil {
		return err
	}
	matched := false
	for _, desired := range input.Projection.DesiredServices {
		if desired.EnvironmentID == input.Environment.Record.ID &&
			desired.BackingNetworkID == change.Record.BackingNetworkID &&
			equalDirectServiceDesired(desired.Desired, change.Record.Desired) {
			matched = true
			break
		}
	}
	if !matched || change.Record.EnvironmentID != input.Environment.Record.ID || change.Record.BackingNetworkID != "" {
		return errs.New(errs.KindValidationFailed, "direct Service change does not match its desired projection")
	}
	if change.Current == nil {
		return nil
	}
	if err := validateServiceVersion(*change.Current); err != nil {
		return err
	}
	if change.Current.Revision != input.ExpectedHeadRevision ||
		change.Current.Record.EnvironmentID != change.Record.EnvironmentID ||
		change.Current.Record.Desired.ID != change.Record.Desired.ID ||
		change.Current.Record.Desired.Name != change.Record.Desired.Name ||
		change.Current.Record.Runtime != change.Record.Runtime ||
		change.Current.Record.BackingNetworkID != change.Record.BackingNetworkID {
		return errs.New(errs.KindValidationFailed, "direct Service replacement changed runtime or identity")
	}
	return nil
}

func equalDirectServiceDesired(left core.Service, right core.Service) bool {
	if left.ID != right.ID || left.Name != right.Name || left.Image != right.Image ||
		left.Strategy != right.Strategy || left.OnFailure != right.OnFailure ||
		left.Healthcheck != right.Healthcheck || left.Resources != right.Resources ||
		left.Restart != right.Restart || left.Logging != right.Logging || left.Replicas != right.Replicas ||
		left.Adapter != right.Adapter || left.FactsPrefix != right.FactsPrefix || left.Label != right.Label ||
		!slices.Equal(left.Zones, right.Zones) || !slices.Equal(left.Command, right.Command) ||
		!slices.Equal(left.Mounts, right.Mounts) || !slices.Equal(left.Expose, right.Expose) ||
		!slices.EqualFunc(left.Environment, right.Environment, equalDirectServiceEnvironmentEntry) {
		return false
	}
	if !maps.EqualFunc(left.Aliases, right.Aliases, func(leftAliases []string, rightAliases []string) bool {
		return slices.Equal(leftAliases, rightAliases)
	}) {
		return false
	}
	return maps.EqualFunc(
		left.DependsOn,
		right.DependsOn,
		func(leftDependency core.ServiceDependency, rightDependency core.ServiceDependency) bool {
			return leftDependency.Condition == rightDependency.Condition &&
				slices.Equal(leftDependency.Phases, rightDependency.Phases)
		},
	)
}

func equalDirectServiceEnvironmentEntry(left core.EnvEntry, right core.EnvEntry) bool {
	return left.ID == right.ID && left.Kind == right.Kind && left.Key == right.Key && left.Path == right.Path &&
		equalDirectServiceUint32Pointer(left.UID, right.UID) &&
		equalDirectServiceUint32Pointer(left.GID, right.GID) &&
		left.Source.Kind == right.Source.Kind && left.Source.Literal == right.Source.Literal &&
		left.Source.SecretRef == right.Source.SecretRef &&
		equalDirectServiceFactReference(left.Source.Fact, right.Source.Fact) &&
		slices.Equal(left.Exposure, right.Exposure) && left.Secret == right.Secret
}

func equalDirectServiceUint32Pointer(left *uint32, right *uint32) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func equalDirectServiceFactReference(left *core.FactRef, right *core.FactRef) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func (repository *HierarchyRepository) prepareEnvironmentDirectPublication(
	ctx context.Context,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection EnvironmentComposeProjection,
	marker IdempotencyMarker,
	expectedHeadRevision int64,
) (environmentBlueprintPublicationEvidence, error) {
	digest, err := EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := validateEnvironmentBlueprintStageClaim(claim); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := validateEnvironmentDesiredRevisionIdentity(revision); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorKey := environmentBlueprintDescriptorKeyByID(claim.DescriptorID)
	descriptorRead, err := repository.store.Get(ctx, descriptorKey)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if descriptorRead == nil || descriptorRead.Entry == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(errs.KindStateConflict, "desired revision staging evidence is unavailable")
	}
	defer clear(descriptorRead.Entry.Value)
	descriptor, err := decodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	locatorKey, _, err := environmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	rootKey := environmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID)
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{rootKey, descriptorKey, locatorKey}, Revision: descriptorRead.ReadRevision,
	})
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if result == nil || len(result.Values) != 3 || result.Values[0] == nil || result.Values[1] == nil || result.Values[2] == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(errs.KindStateConflict, "desired revision staging evidence is unavailable")
	}
	defer clearKeyValues(result.Values)
	seal, err := decodeEnvironmentBlueprintSeal(result.Values[0].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptor, err = decodeEnvironmentBlueprintStageDescriptor(result.Values[1].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorID, locatorDigest, err := decodeEnvironmentBlueprintStageLocator(result.Values[2].Value)
	protectedDigest, digestErr := protectedBlueprintIntentDigest(descriptor.Claim.Intent)
	if err != nil || digestErr != nil || !sameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) ||
		descriptorID != claim.DescriptorID || locatorDigest != protectedDigest ||
		descriptor.State != EnvironmentBlueprintStageSealed || seal != environmentBlueprintSealFromDescriptor(descriptor) ||
		seal.EnvironmentID != revision.EnvironmentID || seal.RevisionID != revision.RevisionID ||
		seal.BaselineHeadRevision != expectedHeadRevision || seal.DependencyDigest != digest ||
		claim.SourceKind != EnvironmentBlueprintSourceMutation || claim.TaskID != revision.RevisionID ||
		projection.RevisionID != revision.RevisionID || projection.RenderGeneration != claim.RenderGeneration ||
		marker.Locator != claim.Locator || !sameBlueprintProtectedIntent(marker.Intent, claim.Intent) {
		return environmentBlueprintPublicationEvidence{}, errs.New(errs.KindStateConflict, "desired revision staging evidence changed")
	}
	published := descriptor
	published.State = EnvironmentBlueprintStagePublished
	published.UpdatedAt = nextBlueprintProgressTime(descriptor.UpdatedAt)
	publishedValue, err := encodeEnvironmentBlueprintStageDescriptor(published)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	return environmentBlueprintPublicationEvidence{
		seal: seal, rootRevision: result.Values[0].ModRevision,
		descriptorRevision: result.Values[1].ModRevision, descriptorKey: descriptorKey,
		locatorRevision: result.Values[2].ModRevision, locatorKey: locatorKey,
		publishedDescriptor: publishedValue,
	}, nil
}
