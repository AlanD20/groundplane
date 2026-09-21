package etcd

import (
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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
	mutationContext *ordinaryEnvironmentMutationContext,
	environmentID string,
	input AttachTaskRenderInput,
) error {
	epoch, ok := mutationContext.revisionForKey(hierarchyrecord.EnvironmentMutationEpochKey(environmentID))
	if !ok || epoch != input.EnvironmentEpochRevision {
		return errs.New(errs.KindStateConflict, "Attach captured runtime changed before publication")
	}
	return nil
}
