package etcd

import (
	"context"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func attachBlueprintRootCondition(ctx context.Context, store hierarchyStore,
	scope AttachCreateScope,
) (etcdstore.Condition, error) {
	key := blueprints.EnvironmentBlueprintRootKey(
		scope.Environment.Record.ID,
		scope.ComposeProjection.Record.RevisionID,
	)
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{key}, Revision: scope.ComposeProjection.ReadRevision,
	})
	if err != nil {
		return etcdstore.Condition{}, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil || read.Values[0].Key != key {
		return etcdstore.Condition{}, errs.New(errs.KindStateConflict, "Attach pinned Blueprint is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	seal, err := blueprints.DecodeEnvironmentBlueprintSeal(read.Values[0].Value)
	if err != nil {
		return etcdstore.Condition{}, err
	}
	if seal.EnvironmentID != scope.Environment.Record.ID ||
		seal.RevisionID != scope.ComposeProjection.Record.RevisionID {
		return etcdstore.Condition{}, errs.New(errs.KindInternal, "Attach pinned Blueprint identity differs")
	}
	return etcdstore.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func attachDesiredHeadConditions(
	consumerEnvironmentID string,
	consumerRevision int64,
	backingService etcdstore.Versioned[servicerecord.ServiceRecord],
	services []etcdstore.Versioned[servicerecord.ServiceRecord],
) ([]etcdstore.Condition, error) {
	candidates := []etcdstore.Condition{{
		Key: blueprints.EnvironmentBlueprintHeadKey(consumerEnvironmentID), ModRevision: consumerRevision,
	}, servicerecord.ServiceDesiredCondition(backingService)}
	for _, service := range services {
		candidates = append(candidates, servicerecord.ServiceDesiredCondition(service))
	}

	conditions := make([]etcdstore.Condition, 0, len(candidates))
	for _, candidate := range candidates {
		duplicate := false
		for _, condition := range conditions {
			if condition.Key != candidate.Key {
				continue
			}
			if condition.ModRevision != candidate.ModRevision {
				return nil, errs.New(
					errs.KindStateConflict,
					"attach Service desired heads disagree on the Environment revision",
				)
			}
			duplicate = true
			break
		}
		if !duplicate {
			conditions = append(conditions, candidate)
		}
	}
	return conditions, nil
}

func validateAttachRuntimeEpoch(
	mutationContext *environmentfence.MutationContext,
	environmentID string,
	input attachrender.AttachTaskRenderInput,
) error {
	epoch, ok := mutationContext.RevisionForKey(hierarchyrecord.EnvironmentMutationEpochKey(environmentID))
	if !ok || epoch != input.EnvironmentEpochRevision {
		return errs.New(errs.KindStateConflict, "Attach captured runtime changed before publication")
	}
	return nil
}
