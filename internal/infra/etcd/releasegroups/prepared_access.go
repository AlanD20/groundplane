package releasegroups

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func (prepared ReleaseGroupPreparedMutation) EnvironmentID() string { return prepared.environmentID }
func (prepared ReleaseGroupPreparedMutation) GroupID() string       { return prepared.groupID }
func (prepared ReleaseGroupPreparedMutation) GroupRevision() int64  { return prepared.groupRevision }
func (prepared ReleaseGroupPreparedMutation) TaskType() taskjournal.TaskType {
	return prepared.taskType
}
func (prepared ReleaseGroupPreparedMutation) Conditions() []etcdstore.Condition {
	return CloneReleaseGroupConditions(prepared.conditions)
}
func (prepared ReleaseGroupPreparedMutation) Mutations() []etcdstore.Mutation {
	return CloneReleaseGroupMutations(prepared.mutations)
}

func (prepared ReleaseGroupPreparedMutation) ValidateRemovalPrimaryMutations() error {
	for _, mutation := range prepared.mutations {
		if prepared.taskType == taskjournal.TaskRemove && strings.HasPrefix(mutation.Key, ReleaseGroupRecordPrefix) {
			return errs.New(errs.KindInternal, "release group removal preparation contains a primary mutation")
		}
	}
	return nil
}

func (prepared ReleaseGroupBlueprintPreparedMutation) ConditionCount() int {
	return len(prepared.conditions)
}

// AppendTo contributes this fragment to the final Blueprint transaction.
// Mutation values retain the original shared ownership and clearing lifecycle.
func (prepared ReleaseGroupBlueprintPreparedMutation) AppendTo(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ([]etcdstore.Condition, []etcdstore.Mutation) {
	return append(conditions, prepared.conditions...), append(mutations, prepared.mutations...)
}
