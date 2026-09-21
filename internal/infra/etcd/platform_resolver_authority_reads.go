package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	platformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
	"strings"
)

func (repository *TaskRepository) platformResolverAtRevision(
	ctx context.Context,
	revision int64,
) (etcdstore.Versioned[componentrecord.Record], error) {
	componentIDs := make([]string, 0, 1)
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: platformcomponents.PlatformComponentOwnerPrefix, StartExclusive: start,
			Limit: etcdstore.MaximumPageLimit, Revision: revision,
		})
		if err != nil {
			return etcdstore.Versioned[componentrecord.Record]{}, err
		}
		if page == nil || page.ReadRevision != revision {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
				errs.KindInternal,
				"platform Component scan did not preserve its fixed revision",
			)
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, platformcomponents.PlatformComponentOwnerPrefix) {
				recordquery.ClearRangeKeyValues(page.Values)
				return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
					errs.KindInternal,
					"platform Component scan contains an invalid key",
				)
			}
			componentID := strings.TrimPrefix(value.Key, platformcomponents.PlatformComponentOwnerPrefix)
			if ids.Validate(ids.KindComponent, componentID) != nil || string(value.Value) != componentID {
				recordquery.ClearRangeKeyValues(page.Values)
				return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
					errs.KindInternal,
					"platform Component owner index is corrupt",
				)
			}
			componentIDs = append(componentIDs, componentID)
		}
		if !page.More {
			recordquery.ClearRangeKeyValues(page.Values)
			break
		}
		if len(page.Values) == 0 {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(errs.KindInternal, "platform Component scan did not advance")
		}
		start = page.Values[len(page.Values)-1].Key
		recordquery.ClearRangeKeyValues(page.Values)
	}
	sort.Strings(componentIDs)
	if len(componentIDs) == 0 {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindComponentNotFound,
			"platform dns-resolver Component is missing",
		)
	}
	keys := make([]string, len(componentIDs))
	for index, componentID := range componentIDs {
		keys[index] = componentrecord.RecordKey(componentID)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return etcdstore.Versioned[componentrecord.Record]{}, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != len(keys) {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(errs.KindInternal, "platform Component scan is incomplete")
	}
	candidates := make([]etcdstore.Versioned[componentrecord.Record], 0, len(componentIDs))
	for index, componentValue := range state.Values {
		if componentValue == nil {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
				errs.KindStateConflict,
				"platform dns-resolver Component is missing",
			)
		}
		component, decodeErr := componentrecord.DecodeRecord(componentValue.Value)
		if decodeErr != nil {
			return etcdstore.Versioned[componentrecord.Record]{}, decodeErr
		}
		if component.Desired.ID != componentIDs[index] || component.Desired.Owner != core.ComponentOwnerPlatform ||
			component.Desired.OwnerID != "" {
			return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
				errs.KindInternal,
				"platform Component owner index is corrupt",
			)
		}
		candidates = append(candidates, etcdstore.Versioned[componentrecord.Record]{
			Record: component, Revision: componentValue.ModRevision, ReadRevision: revision,
		})
	}
	if repository.platformResolverSelector != nil {
		selected, selectErr := repository.platformResolverSelector(ctx, candidates)
		if selectErr != nil {
			return etcdstore.Versioned[componentrecord.Record]{}, selectErr
		}
		for _, candidate := range candidates {
			if candidate.Record.Desired.ID == selected.Record.Desired.ID && candidate.Revision == selected.Revision &&
				selected.ReadRevision == revision {
				return selected, nil
			}
		}
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindInternal,
			"platform dns-resolver selector returned an unscanned Component",
		)
	}
	if len(candidates) != 1 {
		return etcdstore.Versioned[componentrecord.Record]{}, errs.New(
			errs.KindStateConflict,
			"multiple platform dns-resolver Components are registered",
		)
	}
	return candidates[0], nil
}

func (repository *TaskRepository) platformResolverActiveAtRevision(
	ctx context.Context,
	componentID string,
	revision int64,
) (*etcdstore.KeyValue, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{platformComponentTaskActiveKey(componentID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, errs.New(errs.KindInternal, "platform resolver active fence read is incomplete")
	}
	active := read.Values[0]
	if active == nil {
		return nil, nil
	}
	if active.Key != platformComponentTaskActiveKey(componentID) ||
		ids.Validate(ids.KindTask, string(active.Value)) != nil {
		return nil, errs.New(errs.KindInternal, "platform resolver active fence is corrupt")
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(string(active.Value))}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if state == nil || state.ReadRevision != revision || len(state.Values) != 1 || state.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "platform resolver active Task is missing")
	}
	activeTask, err := decodeTaskRecord(state.Values[0].Value)
	if err != nil || !isPlatformDNSResolverTaskAttempt(activeTask) || activeTask.ID != string(active.Value) ||
		activeTask.Target != componentID || (activeTask.Status != taskjournal.TaskStatusPending && activeTask.Status != taskjournal.TaskStatusRunning) {
		return nil, errs.New(errs.KindStateConflict, "platform resolver active Task is invalid")
	}
	return active, nil
}

func (repository *TaskRepository) platformResolverTaskInputAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (platformcomponents.PlatformComponentTaskRenderInput, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{platformcomponents.PlatformComponentTaskRenderInputKey(task.PlanID)}, Revision: revision,
	})
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is missing",
		)
	}
	input, err := platformcomponents.DecodePlatformComponentTaskRenderInput(read.Values[0].Value)
	if err != nil {
		return platformcomponents.PlatformComponentTaskRenderInput{}, err
	}
	if input.PlanID != task.PlanID || input.ComponentID != task.Target || task.PlanHash != input.ExecutionPlanSHA256 ||
		!platformResolverTaskInputBelongsToTask(task, input) ||
		task.Params[TaskPlatformComponentDesiredSHA256Param] != input.DesiredSHA256 {
		return platformcomponents.PlatformComponentTaskRenderInput{}, errs.New(
			errs.KindStateConflict,
			"platform resolver render input is not pinned",
		)
	}
	return input, nil
}

func platformResolverTaskInputBelongsToTask(
	task TaskRecord,
	input platformcomponents.PlatformComponentTaskRenderInput,
) bool {
	return input.TaskID == task.ID || task.RetryOf != ""
}
