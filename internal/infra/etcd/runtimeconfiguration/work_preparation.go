package runtimeconfiguration

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

// CaptureWork retains immutable file sources and their acknowledged predecessor.
// The operation owner publishes these references with executable input and pins.
func (repository *Repository) CaptureWork(ctx context.Context, task workexecution.Task, item workexecution.Item,
	revision int64, configurationID string, generation uint64, changes []taskmaterialization.Record,
) (WorkCapture, error) {
	var empty WorkCapture
	if ctx == nil {
		return empty, errs.New(errs.KindValidationFailed, "Work configuration context is missing")
	}

	if repository == nil || repository.store == nil || revision <= 0 ||
		task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		(item.Operation != workexecution.Release && item.Operation != workexecution.Component && item.Operation != workexecution.EntryConfiguration) || item.Placement.Executor != workexecution.Agent ||
		task.Scope.EnvironmentID == "" || task.FinishedAt != nil || task.Stop != nil ||
		item.AttemptNumber != 0 || item.ForwardRevoked || item.Phase == workexecution.Settled ||
		!slices.Contains(item.Claims, workexecution.Claim{ScopeID: task.Scope.EnvironmentID}) ||
		!slices.Contains(item.Claims, workexecution.Claim{HostID: item.Placement.HostID, ScopeID: task.Scope.EnvironmentID}) ||
		ids.Validate(ids.KindConfig, configurationID) != nil || generation == 0 {
		return empty, errs.New(errs.KindValidationFailed, "Work configuration requires unstarted native work")
	}
	environmentID := task.Scope.EnvironmentID
	headKey := HeadKey(environmentID)
	read, err := repository.read(ctx, []string{headKey, environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID)}, revision)
	if err != nil {
		return empty, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return empty, errs.New(errs.KindStateConflict, "Work configuration snapshot read is incomplete")
	}
	defer clearKeyValues(read.Values)
	capture := WorkCapture{
		TaskID: task.ID, WorkItemID: item.ID, Placement: item.Placement,
		ReadRevision: revision, Generation: generation,
	}
	for _, file := range changes {
		capture.Destinations = append(capture.Destinations, file.Destination)
	}
	slices.Sort(capture.Destinations)
	var previous Snapshot
	if value := read.Values[0]; value != nil {
		if value.Key != headKey || value.ModRevision <= 0 || value.ModRevision > revision {
			return empty, errs.New(errs.KindStateConflict, "Work configuration head revision is invalid")
		}
		prior, err := DecodeReference(value.Value)
		if err != nil {
			return empty, err
		}
		if prior.EnvironmentID != environmentID || prior.Generation > generation {
			return empty, errs.New(errs.KindStateConflict, "Work configuration predecessor belongs to newer or different intent")
		}
		previous, err = repository.Load(ctx, prior, revision)
		if err != nil {
			return empty, err
		}
		capture.Prior, capture.PriorRevision, capture.Current = &prior, value.ModRevision, prior
	} else if read.Values[1] != nil {
		return empty, errs.New(errs.KindStateConflict, "applied Environment has no acknowledged configuration")
	}
	if capture.Prior == nil || len(changes) != 0 {
		candidate := Snapshot{
			ID: configurationID, EnvironmentID: environmentID, Generation: generation,
			Files: taskmaterialization.Clone(changes),
		}
		if capture.Prior != nil {
			candidate, err = Merge(previous, changes, configurationID, generation)
			if err != nil {
				return empty, err
			}
		}
		capture.Current, err = repository.Stage(ctx, candidate)
		if err != nil {
			return empty, err
		}
	}
	if err := capture.Matches(task, item); err != nil {
		return empty, err
	}
	return capture, nil
}
