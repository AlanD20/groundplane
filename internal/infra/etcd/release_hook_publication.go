package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourcequeries "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcequeries"
)

type ReleaseHookExecutionPublication struct {
	Sources          scriptsourcequeries.ScriptExecutionSources
	Execution        scriptexecutions.ScriptExecutionRecord
	SnapshotRevision int64
}

type ReleaseHookWorkPublication struct {
	Task         workexecution.Task
	Item         workexecution.Item
	OperationID  string
	ReadRevision int64
	Hooks        []ReleaseHookExecutionPublication
}

type releaseHookPublicationFragment struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
}

func clearReleaseHookPublicationFragment(fragment releaseHookPublicationFragment) {
	etcdstore.ZeroMutationBytes(fragment.mutations)
}
