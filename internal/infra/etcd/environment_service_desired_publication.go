package etcd

import (
	"bytes"
	"context"

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
	prepared, err := repository.prepareDirectEnvironmentServiceChangeAtRevision(
		ctx, input.Environment, input.Projection, input.Change, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(prepared.value)
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
	primary := Condition{Key: serviceKey(serviceID)}
	name := Condition{Key: serviceNameKey(input.Environment.Record.ID, input.Change.Record.Desired.Name)}
	owner := Condition{Key: serviceOwnerKey(input.Environment.Record.ID, serviceID)}
	if input.Change.Current != nil {
		primary.ModRevision = input.Change.Current.Revision
		name.ModRevision = prepared.nameRevision
		owner.ModRevision = prepared.ownerRevision
	}
	conditions := []Condition{
		{Key: environmentBlueprintRootKey(input.Revision.EnvironmentID, input.Revision.RevisionID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(input.Revision.EnvironmentID), ModRevision: input.ExpectedHeadRevision},
		primary, name, owner, {Key: deletionTombstoneKey("service", serviceID)},
	}
	conditions = append(conditions, referenceConditions...)
	referenceOffset := 8
	fenceOffset := len(conditions)
	conditions = append(conditions, fence.transactionConditions()...)
	mutations := []Mutation{
		{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: MutationDelete, Key: publication.locatorKey},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(input.Revision.EnvironmentID), Value: headReference},
		{Type: MutationPut, Key: serviceKey(serviceID), Value: prepared.value},
	}
	if input.Change.Current == nil {
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: serviceNameKey(input.Environment.Record.ID, input.Change.Record.Desired.Name), Value: []byte(serviceID)},
			Mutation{Type: MutationPut, Key: serviceOwnerKey(input.Environment.Record.ID, serviceID), Value: []byte(serviceID)},
		)
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
		if input.Change.Current == nil {
			if values[4] != nil || values[5] != nil || values[6] != nil {
				return errs.New(errs.KindNameConflict, "Service identity is already in use")
			}
		} else if values[4] == nil || values[4].ModRevision != input.Change.Current.Revision ||
			values[5] == nil || values[5].ModRevision != prepared.nameRevision ||
			values[6] == nil || values[6].ModRevision != prepared.ownerRevision {
			return errs.New(errs.KindStateConflict, "Service desired state changed")
		}
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Service removal is in progress")
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
	if err := validateEnvironmentDesiredPublicationBudget(plan, input.Marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, input.Marker, plan)
}

func (repository *HierarchyRepository) prepareDirectEnvironmentServiceChangeAtRevision(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	projection EnvironmentComposeProjection,
	change EnvironmentBlueprintServiceChange,
	readRevision int64,
) (preparedEnvironmentBlueprintService, error) {
	if err := validateServiceRecord(change.Record); err != nil {
		return preparedEnvironmentBlueprintService{}, err
	}
	serviceID := change.Record.Desired.ID
	matched := false
	for _, identity := range projection.Services {
		if identity.ID == serviceID && identity.Name == change.Record.Desired.Name {
			matched = true
			break
		}
	}
	if !matched || change.Record.EnvironmentID != environment.Record.ID || change.Record.BackingNetworkID != "" {
		return preparedEnvironmentBlueprintService{}, errs.New(
			errs.KindValidationFailed, "direct Service change does not match its desired projection",
		)
	}
	prepared := preparedEnvironmentBlueprintService{change: change}
	if change.Current != nil {
		if err := validateServiceVersion(*change.Current); err != nil {
			return preparedEnvironmentBlueprintService{}, err
		}
		if change.Current.Record.EnvironmentID != environment.Record.ID ||
			change.Current.Record.Desired.ID != serviceID ||
			change.Current.Record.Desired.Name != change.Record.Desired.Name ||
			change.Current.Record.Runtime != change.Record.Runtime {
			return preparedEnvironmentBlueprintService{}, errs.New(
				errs.KindValidationFailed, "direct Service replacement changed runtime or identity",
			)
		}
		indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			serviceNameKey(environment.Record.ID, change.Record.Desired.Name),
			serviceOwnerKey(environment.Record.ID, serviceID),
		}, Revision: readRevision})
		if err != nil {
			return preparedEnvironmentBlueprintService{}, err
		}
		if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
			!bytes.Equal(indexes.Values[0].Value, []byte(serviceID)) ||
			!bytes.Equal(indexes.Values[1].Value, []byte(serviceID)) {
			return preparedEnvironmentBlueprintService{}, errs.New(errs.KindInternal, "Service indexes are missing or corrupt")
		}
		prepared.nameRevision = indexes.Values[0].ModRevision
		prepared.ownerRevision = indexes.Values[1].ModRevision
	}
	value, err := encodeServiceRecord(change.Record)
	if err != nil {
		return preparedEnvironmentBlueprintService{}, err
	}
	prepared.value = value
	return prepared, nil
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
