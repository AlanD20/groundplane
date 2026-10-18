package imagedelivery

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func retainScriptExecutionImage(retention *imageRetention, execution scriptexecutions.ScriptExecutionRecord) error {
	if err := scriptexecutions.ValidateScriptExecutionRecord(execution); err != nil {
		return err
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil {
		return errs.Wrap(errs.KindStateConflict, err)
	}
	retention.retain(snapshot.LocalImageId, "Captured Script in Task "+execution.CurrentTaskID)
	return nil
}

func retainScriptWorkImages(retention *imageRetention, value keyvalue.KeyValue) error {
	if strings.Contains(strings.TrimPrefix(value.Key, scriptexecutions.ScriptExecutionKey("")), "/") {
		return nil // Checkpoint children carry no independent runner selection.
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](value.Value, "script-execution")
	if err != nil {
		return err
	}
	defer execution.Clear()
	if value.Key != scriptexecutions.ScriptExecutionKey(execution.ID) {
		return errs.New(errs.KindStateConflict, "Script image capture has inconsistent ownership")
	}
	return retainScriptExecutionImage(retention, execution)
}

func (service *Service) retainGroupPreflightImages(ctx context.Context, retention *imageRetention,
	queue *workqueue.Repository, task workqueue.TaskSnapshot, root keyvalue.Versioned[releasegroups.ExecutionCapture], revision int64,
) error {
	for _, hook := range root.Record.Preflight {
		key := etcd.GroupReleasePreflightCaptureKey(task.Task.ID, hook.WorkItemID)
		read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return errs.New(errs.KindStateConflict, "Group preflight image capture read is incomplete")
		}
		value := read.Values[0]
		if value == nil {
			if !task.Task.PublicationComplete {
				return errs.New(errs.KindStateConflict, "Unpublished Group lost its preflight image capture")
			}
			work, err := queue.ReadWork(ctx, task.Task.ID, hook.WorkItemID, revision)
			if err != nil {
				return err
			}
			if work.Item.Item.Input == nil || *work.Item.Item.Input != hook.Input {
				return errs.New(errs.KindStateConflict, "Group preflight image capture has no promoted Script owner")
			}
			continue // The Script execution scan owns the promoted input.
		}
		capture, err := recordcodec.Decode[etcd.GroupReleasePreflightCapture](value.Value, "group-release-preflight")
		clear(value.Value)
		if err != nil {
			return err
		}
		execution := capture.Execution
		if value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision || execution.CurrentTaskID != task.Task.ID ||
			execution.WorkItemID != hook.WorkItemID || execution.Input != hook.Input || execution.ScriptID != hook.ScriptID ||
			execution.ServiceID != hook.ServiceID || execution.EnvironmentID != task.Task.Scope.EnvironmentID {
			execution.Clear()
			return errs.New(errs.KindStateConflict, "Group preflight image capture differs from its accepted member")
		}
		err = retainScriptExecutionImage(retention, execution)
		execution.Clear()
		if err != nil {
			return err
		}
	}
	return nil
}
