package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskMaterializationEnvironment(record TaskRecord) (string, bool, error) {
	environmentID, declared := record.Params[taskjournal.TaskMaterializationEnvironmentParam]
	if !declared {
		return "", false, nil
	}
	resourceRemoval := record.Type == taskjournal.TaskRemove &&
		(recordcodec.ValidateID(ids.KindRoute, record.Target) == nil || recordcodec.ValidateID(ids.KindEnvEntry, record.Target) == nil)
	volumeMutation := record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceVolume &&
		recordcodec.ValidateID(ids.KindVolume, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate || record.Type == taskjournal.TaskRemove)
	routeMutation := record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceRoute &&
		recordcodec.ValidateID(ids.KindRoute, record.Target) == nil &&
		(record.Type == taskjournal.TaskCreate || record.Type == taskjournal.TaskUpdate) &&
		record.Params[taskjournal.TaskRouteEnvironmentParam] == environmentID && record.Owner.EnvironmentID == environmentID
	if record.Executor != taskjournal.TaskExecutorAgent ||
		recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		(record.Target != environmentID && !resourceRemoval && !volumeMutation && !routeMutation) {
		return "", false, errs.New(errs.KindValidationFailed, "task materialization Environment is invalid")
	}
	return environmentID, true, nil
}
