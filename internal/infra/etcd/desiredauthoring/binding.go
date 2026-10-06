package desiredauthoring

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Classifier func(int64, []*keyvalue.KeyValue) error

func Bind(publication Publication,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
	previous Classifier,
) ([]keyvalue.Condition, []keyvalue.Mutation, Classifier, error) {
	base := len(conditions)
	appended := make([]keyvalue.Condition, 0, len(publication.Conditions))
	for _, desired := range publication.Conditions {
		found := false
		for _, existing := range conditions {
			if existing.Key == desired.Key && existing != desired {
				return nil, nil, nil, errs.New(errs.KindStateConflict, "Environment desired authority changed during preparation")
			}
			if existing == desired {
				found = true
				break
			}
		}
		if !found {
			conditions = append(conditions, desired)
			appended = append(appended, desired)
		}
	}
	mutations = append(mutations, publication.Mutations...)
	return conditions, mutations, func(revision int64, values []*keyvalue.KeyValue) error {
		if len(values) != base+len(appended) {
			return errs.New(errs.KindInternal, "direct desired compare evidence is incomplete")
		}
		for index, condition := range appended {
			if !keyvalue.ConditionMatchesRead(condition, values[base+index]) {
				return errs.New(errs.KindStateConflict, "Environment desired head changed")
			}
		}
		return previous(revision, values[:base])
	}, nil
}
