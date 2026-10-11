package etcd

import (
	"context"
	"time"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskClaimQueuePageSize = 64

type taskClaimCandidate struct {
	queued        etcdstore.KeyValue
	taskValue     *etcdstore.KeyValue
	task          TaskRecord
	readRevision  int64
	writerKey     string
	environmentID string
	dnsConditions []etcdstore.Condition
	advanced      bool
}

func (repository *TaskRepository) nextTaskClaimCandidate(
	ctx context.Context,
	executor taskjournal.TaskExecutor,
	claimAt time.Time,
) (taskClaimCandidate, bool, error) {
	prefix := taskjournal.TaskQueueScopePrefix(executor)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: taskClaimQueuePageSize, Revision: revision,
		})
		if err != nil {
			return taskClaimCandidate{}, false, err
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.ReadRevision != revision {
			return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "task queue scan changed MVCC revision")
		}
		for _, queued := range page.Values {
			taskID, err := taskjournal.TaskIDFromQueueKey(executor, queued.Key)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			referencedTaskID, err := idempotencyrecord.DecodeTaskReference(queued.Value)
			if err != nil || referencedTaskID != taskID {
				return taskClaimCandidate{}, false, errs.New(
					errs.KindInternal,
					"task queue record does not match its key",
				)
			}
			taskRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision,
			})
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if len(taskRead.Values) != 1 || taskRead.Values[0] == nil {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task primary is missing")
			}
			taskValue := taskRead.Values[0]
			task, err := DecodeTaskRecord(taskValue.Value)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if task.ID != taskID || task.Executor != executor || task.Status != taskjournal.TaskStatusPending ||
				task.idempotencyMarker == nil && !isMarkerlessHierarchyDeletionAgentChild(task) {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task is not claimable")
			}
			currentChild, err := repository.blueprintChildHeadAtRevision(ctx, task, revision)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if !currentChild {
				continue
			}
			dnsConditions, prerequisitesReady, advanced, err := repository.prepareQueuedPrerequisites(
				ctx,
				etcdstore.Versioned[TaskRecord]{
					Record:       task,
					Revision:     taskValue.ModRevision,
					ReadRevision: revision,
				},
				claimAt,
			)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if advanced {
				return taskClaimCandidate{advanced: true}, true, nil
			}
			if !prerequisitesReady {
				continue
			}
			environmentID, materializes, err := taskEnvironmentWriter(task)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			writerKey := ""
			if materializes {
				writerKey = taskjournal.TaskMaterializationWriterKey(environmentID)
				writerRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
					Keys: []string{writerKey}, Revision: revision,
				})
				if err != nil {
					return taskClaimCandidate{}, false, err
				}
				if len(writerRead.Values) != 1 {
					return taskClaimCandidate{}, false, errs.New(
						errs.KindInternal,
						"materialization writer read is incomplete",
					)
				}
				if writerRead.Values[0] != nil {
					writer, err := decodeTaskMaterializationWriter(writerRead.Values[0].Value)
					if err != nil || writer.EnvironmentID != environmentID || writer.TaskID == task.ID {
						return taskClaimCandidate{}, false, corruptTaskMaterializationWriter()
					}
					continue
				}
			}
			return taskClaimCandidate{
				queued: queued, taskValue: taskValue, task: task, readRevision: revision,
				writerKey: writerKey, environmentID: environmentID,
				dnsConditions: dnsConditions,
			}, true, nil
		}
		if !page.More {
			return taskClaimCandidate{}, false, nil
		}
		if len(page.Values) == 0 {
			return taskClaimCandidate{}, false, errs.New(
				errs.KindInternal,
				"task queue page is empty before completion",
			)
		}
		start = page.Values[len(page.Values)-1].Key
	}
}
