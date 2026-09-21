package releasegroups

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleaseGroupPreparedMutation is an opaque, immutable transaction fragment.
// Only the etcd Release Group adapter can prepare a non-zero value.
type ReleaseGroupPreparedMutation struct {
	environmentID string
	groupID       string
	groupRevision int64
	taskType      taskjournal.TaskType
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
}

// ReleaseGroupBlueprintPreparedMutation is an opaque, immutable Release Group
// projection fragment for Environment desired-revision publication.
type ReleaseGroupBlueprintPreparedMutation struct {
	environmentID string
	conditions    []etcdstore.Condition
	mutations     []etcdstore.Mutation
}

func (prepared ReleaseGroupBlueprintPreparedMutation) IsZero() bool {
	return prepared.environmentID == "" && len(prepared.conditions) == 0 && len(prepared.mutations) == 0
}

func newReleaseGroupPreparedMutation(
	environmentID string,
	groupID string,
	groupRevision int64,
	taskType taskjournal.TaskType,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ReleaseGroupPreparedMutation {
	return ReleaseGroupPreparedMutation{
		environmentID: environmentID,
		groupID:       groupID,
		groupRevision: groupRevision,
		taskType:      taskType,
		conditions:    CloneReleaseGroupConditions(conditions),
		mutations:     CloneReleaseGroupMutations(mutations),
	}
}

func newReleaseGroupBlueprintPreparedMutation(
	environmentID string,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) ReleaseGroupBlueprintPreparedMutation {
	return ReleaseGroupBlueprintPreparedMutation{
		environmentID: environmentID,
		conditions:    CloneReleaseGroupConditions(conditions),
		mutations:     CloneReleaseGroupMutations(mutations),
	}
}

func ValidateReleaseGroupPreparedFragment(prepared ReleaseGroupPreparedMutation) error {
	if len(prepared.mutations) == 0 {
		return errs.New(errs.KindInternal, "release group mutation handoff is empty")
	}
	for _, condition := range prepared.conditions {
		if condition.Prefix {
			return errs.New(errs.KindInternal, "release group mutation contains open-ended compare authority")
		}
	}
	for _, mutation := range prepared.mutations {
		if mutation.Prefix {
			return errs.New(errs.KindInternal, "release group mutation contains open-ended write authority")
		}
	}
	return nil
}

func ValidateReleaseGroupBlueprintPreparedMutation(
	prepared ReleaseGroupBlueprintPreparedMutation,
	environmentID string,
) error {
	if prepared.IsZero() {
		return nil
	}
	if prepared.environmentID != environmentID {
		return errs.New(errs.KindInternal, "Blueprint Release Group mutation handoff scope is invalid")
	}
	for _, condition := range prepared.conditions {
		if condition.Prefix {
			return errs.New(errs.KindInternal, "Blueprint Release Group mutation contains open-ended compare authority")
		}
	}
	for _, mutation := range prepared.mutations {
		if mutation.Prefix {
			return errs.New(errs.KindInternal, "Blueprint Release Group mutation contains open-ended write authority")
		}
	}
	return nil
}

func CloneReleaseGroupConditions(conditions []etcdstore.Condition) []etcdstore.Condition {
	return append([]etcdstore.Condition(nil), conditions...)
}

func CloneReleaseGroupMutations(mutations []etcdstore.Mutation) []etcdstore.Mutation {
	cloned := make([]etcdstore.Mutation, len(mutations))
	for index := range mutations {
		cloned[index] = mutations[index]
		cloned[index].Value = append([]byte(nil), mutations[index].Value...)
	}
	return cloned
}
