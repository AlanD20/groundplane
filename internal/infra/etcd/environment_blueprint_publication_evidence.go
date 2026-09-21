package etcd

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintTransactionSizer interface {
	transactionSize([]etcdstore.Condition, []etcdstore.Mutation) (int, error)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintPublication(
	ctx context.Context,
	claim blueprints.EnvironmentBlueprintStageClaim,
	revision blueprints.EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	expectedHeadRevision int64,
) (environmentBlueprintPublicationEvidence, error) {
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := blueprints.ValidateEnvironmentBlueprintStageClaim(claim); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if err := blueprints.ValidateEnvironmentDesiredRevisionIdentity(revision); err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorKey := blueprints.EnvironmentBlueprintDescriptorKeyByID(claim.DescriptorID)
	descriptorRead, err := repository.store.Get(ctx, descriptorKey)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if descriptorRead == nil || descriptorRead.Entry == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence is unavailable",
		)
	}
	defer clear(descriptorRead.Entry.Value)
	descriptor, err := blueprints.DecodeEnvironmentBlueprintStageDescriptor(descriptorRead.Entry.Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	locatorKey, _, err := blueprints.EnvironmentBlueprintLocatorKey(descriptor.Claim.Locator)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	rootKey := blueprints.EnvironmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID)
	keys := []string{rootKey, descriptorKey, locatorKey}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: descriptorRead.ReadRevision})
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	if result == nil || len(result.Values) != len(keys) || result.Values[0] == nil ||
		result.Values[1] == nil || result.Values[2] == nil {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence is unavailable",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(result.Values[0].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptor, err = blueprints.DecodeEnvironmentBlueprintStageDescriptor(result.Values[1].Value)
	if err != nil {
		return environmentBlueprintPublicationEvidence{}, err
	}
	descriptorID, locatorDigest, err := blueprints.DecodeEnvironmentBlueprintStageLocator(result.Values[2].Value)
	protectedDigest, digestErr := blueprints.ProtectedBlueprintIntentDigest(descriptor.Claim.Intent)
	taskEnvironmentID, materializes, taskEnvironmentErr := desiredRevisionTaskEnvironment(task)
	if err != nil || digestErr != nil || !blueprints.SameEnvironmentBlueprintStageClaim(descriptor.Claim, claim) ||
		descriptorID != descriptor.Claim.DescriptorID || locatorDigest != protectedDigest ||
		descriptor.State != blueprints.EnvironmentBlueprintStageSealed || seal != blueprints.EnvironmentBlueprintSealFromDescriptor(descriptor) ||
		seal.EnvironmentID != revision.EnvironmentID || seal.RevisionID != revision.RevisionID ||
		seal.BaselineHeadRevision != expectedHeadRevision || seal.DependencyDigest != digest ||
		descriptor.Claim.TaskID != task.ID || taskEnvironmentErr != nil || !materializes ||
		taskEnvironmentID != revision.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != revision.RevisionID ||
		uint64(task.RenderGeneration) != descriptor.Claim.RenderGeneration || marker.TaskID != task.ID ||
		!blueprints.SameBlueprintProtectedIntent(marker.Intent, descriptor.Claim.Intent) {
		return environmentBlueprintPublicationEvidence{}, errs.New(
			errs.KindStateConflict,
			"Blueprint sealed staging evidence changed",
		)
	}
	published := descriptor
	published.State = blueprints.EnvironmentBlueprintStagePublished
	published.UpdatedAt = blueprints.NextBlueprintProgressTime(descriptor.UpdatedAt)
	publishedValue, err := blueprints.EncodeEnvironmentBlueprintStageDescriptor(published)
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

func ValidateBlueprintTransaction(
	store hierarchyStore,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	maximumOperations int,
	maximumBytes int,
) error {
	operations := len(conditions) + len(mutations)
	if operations == 0 || operations > maximumOperations {
		return errs.New(errs.KindInternal, "Blueprint transaction operation budget exceeded")
	}
	valueBytes := 0
	for _, condition := range conditions {
		if len(condition.Key) == 0 || len(condition.Key) > blueprints.EnvironmentBlueprintKeyMaxBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction key exceeds 2 KiB")
		}
	}
	for _, mutation := range mutations {
		if len(mutation.Key) == 0 || len(mutation.Key) > blueprints.EnvironmentBlueprintKeyMaxBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction key exceeds 2 KiB")
		}
		if mutation.Type == etcdstore.MutationPut {
			valueBytes += len(mutation.Value)
		}
	}
	conservative := valueBytes + operations*blueprints.EnvironmentBlueprintKeyMaxBytes + operations*64 + 128
	if conservative > maximumBytes {
		return errs.New(errs.KindInternal, "Blueprint transaction conservative byte budget exceeded")
	}
	if sizer, ok := store.(blueprintTransactionSizer); ok {
		actual, err := sizer.transactionSize(conditions, mutations)
		if err != nil {
			return err
		}
		if actual > maximumBytes {
			return errs.New(errs.KindInternal, "Blueprint transaction encoded byte budget exceeded")
		}
	}
	return nil
}
