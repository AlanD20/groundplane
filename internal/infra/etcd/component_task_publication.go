package etcd

import (
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func composeEnvironmentBlueprintComponentPublication(
	existing []etcdstore.Condition,
	base idempotencyPlanClassifier,
	publication componentplanning.Publication,
) ([]etcdstore.Condition, idempotencyPlanClassifier, error) {
	conditions := append([]etcdstore.Condition(nil), existing...)
	indices := make(map[string]int, len(existing)+len(publication.Conditions()))
	for index, condition := range conditions {
		indices[condition.Key] = index
	}
	publicationIndices := make([]int, len(publication.Conditions()))
	for index, condition := range publication.Conditions() {
		position, found := indices[condition.Key]
		if found {
			if conditions[position] != condition {
				return nil, nil, errs.New(errs.KindStateConflict, "Blueprint Component source revisions disagree")
			}
		} else {
			position = len(conditions)
			indices[condition.Key] = position
			conditions = append(conditions, condition)
		}
		publicationIndices[index] = position
	}
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Blueprint Component compare evidence is incomplete")
		}
		if err := base(revision, values[:len(existing)]); err != nil {
			return err
		}
		// Both owners classify the same authoritative read at a shared key;
		// deduplication must not shift the Component's positional evidence.
		componentValues := make([]*etcdstore.KeyValue, len(publicationIndices))
		for index, position := range publicationIndices {
			componentValues[index] = values[position]
		}
		return publication.Classify(componentValues)
	}
	return conditions, classify, nil
}
