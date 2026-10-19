package etcd

import (
	"cmp"
	"context"
	"slices"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func runtimeConfigurationHeadKey(environmentID string) string {
	return "/v1/runtime/environment-configurations/" + environmentID
}

func requireSettledConfiguration(
	ctx context.Context, store hierarchyStore, environmentID, taskID string, revision int64, kind errs.Kind,
) error {
	page, err := recordquery.ListVisibleIndex(
		ctx, store, "tasks", "environment", environmentID,
		taskjournal.TaskEnvironmentIndexPrefix+environmentID+"/",
		taskjournal.TaskStorageKey, ids.KindTask,
		etcdstore.PageRequest{Limit: 1, Revision: revision},
		DecodeTaskRecord, func(task TaskRecord) string { return task.ID },
		func(task TaskRecord) bool { return task.Owner.EnvironmentID == environmentID },
		func(task TaskRecord) bool {
			return task.ID != taskID && task.Configuration != nil &&
				!taskjournal.IsTerminalTaskStatus(task.Status)
		},
	)
	if err != nil || len(page.Items) == 0 {
		return err
	}
	return errs.Newf(kind,
		"Environment configuration is still being applied by Task %s; wait for it to finish",
		page.Items[0].Record.ID)
}

func (ledger *ReleaseLedger) PrepareTaskConfigurationAtRevision(
	ctx context.Context, task TaskRecord, revision int64,
) (TaskRecord, error) {
	if ledger == nil || ledger.store == nil {
		return TaskRecord{}, errs.New(
			errs.KindInternal,
			"Release configuration repository is not configured",
		)
	}
	// Pending writers count too: a queued Task has already captured the prior
	// file head. Seal the next child only after that writer acknowledges or
	// proves restoration, rather than queuing two incompatible predecessors.
	if err := requireSettledConfiguration(ctx, ledger.store, task.Owner.EnvironmentID, task.ID, revision, errs.KindResourceInUse); err != nil {
		return TaskRecord{}, err
	}
	return prepareRuntimeConfigurationTask(ctx, ledger.store, task, revision)
}

// prepareRuntimeConfigurationTask retains the exact file-source set before
// publication. Staging is not an acknowledgement; the existing publication
// transaction must still compare the captured head and Environment writer.
func prepareRuntimeConfigurationTask(
	ctx context.Context, store hierarchyStore, task TaskRecord, revision int64,
) (TaskRecord, error) {
	if ctx == nil || revision <= 0 || task.RenderGeneration <= 0 ||
		ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) != nil {
		return TaskRecord{}, errs.New(
			errs.KindValidationFailed,
			"runtime configuration Task identity is invalid",
		)
	}
	environmentID := task.Owner.EnvironmentID
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			runtimeConfigurationHeadKey(environmentID),
			projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return TaskRecord{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return TaskRecord{}, errs.New(errs.KindInternal, "runtime configuration read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	sources, err := runtimeconfiguration.New(store)
	if err != nil {
		return TaskRecord{}, err
	}
	var reference runtimeconfiguration.Reference
	var previous runtimeconfiguration.Snapshot
	headRevision := int64(0)
	var prior *runtimeconfiguration.Reference
	if read.Values[0] != nil {
		value := read.Values[0]
		if value.Key != runtimeConfigurationHeadKey(environmentID) || value.ModRevision <= 0 ||
			value.ModRevision > revision {
			return TaskRecord{}, errs.New(
				errs.KindInternal,
				"runtime configuration head identity is invalid",
			)
		}
		reference, err = runtimeconfiguration.DecodeReference(value.Value)
		if err != nil {
			return TaskRecord{}, err
		}
		if reference.EnvironmentID != environmentID ||
			reference.Generation > uint64(task.RenderGeneration) {
			return TaskRecord{}, errs.New(
				errs.KindStateConflict,
				"runtime configuration belongs to another execution",
			)
		}
		previous, err = sources.Load(ctx, reference, revision)
		if err != nil {
			return TaskRecord{}, err
		}
		headRevision = value.ModRevision
		priorReference := reference
		prior = &priorReference
	} else if read.Values[1] != nil {
		// An already-applied Environment without file acknowledgement cannot be
		// migrated by guessing that its runtime paths are absent or current.
		return TaskRecord{}, errs.New(errs.KindStateConflict, "applied Environment has no acknowledged configuration")
	}
	if headRevision == 0 || len(task.Materializations) != 0 {
		configurationID := ids.New(ids.KindConfig)
		if task.Configuration != nil {
			_, present, err := taskConfigurationCondition(task)
			if err != nil {
				return TaskRecord{}, err
			}
			if task.Configuration.PriorRevision != headRevision ||
				(task.Configuration.Prior == nil) != (prior == nil) ||
				prior != nil && *task.Configuration.Prior != *prior {
				return TaskRecord{}, errs.New(errs.KindStateConflict, "prepared configuration predecessor changed")
			}
			if present {
				configurationID = task.Configuration.Current.ID
			}
		}
		var candidate runtimeconfiguration.Snapshot
		if headRevision == 0 {
			candidate = runtimeconfiguration.Snapshot{
				ID: configurationID, EnvironmentID: environmentID,
				Generation: uint64(
					task.RenderGeneration,
				), Files: taskmaterialization.Clone(task.Materializations),
			}
			// Stage's input is destination-sorted, unlike Task step order.
			slices.SortFunc(candidate.Files, func(left, right taskmaterialization.Record) int {
				return cmp.Compare(left.Destination, right.Destination)
			})
		} else {
			candidate, err = runtimeconfiguration.Merge(previous, task.Materializations,
				configurationID, uint64(task.RenderGeneration))
			if err != nil {
				return TaskRecord{}, err
			}
		}
		reference, err = sources.Stage(ctx, candidate)
		if err != nil {
			return TaskRecord{}, err
		}
	}
	if task.Configuration != nil && task.Configuration.Current != (runtimeconfiguration.Reference{}) &&
		reference != task.Configuration.Current {
		return TaskRecord{}, errs.New(errs.KindStateConflict, "prepared configuration source set changed")
	}
	prepared := cloneTaskRecord(task)
	prepared.Configuration = taskconfiguration.CloneTaskConfiguration(task.Configuration)
	if prepared.Configuration == nil {
		prepared.Configuration = &taskconfiguration.TaskConfiguration{}
	}
	prepared.Configuration.Current = reference
	prepared.Configuration.Prior = prior
	prepared.Configuration.PriorRevision = headRevision
	return prepared, nil
}

func taskRuntimeConfiguration(task TaskRecord) (*runtimeconfiguration.Reference, error) {
	if task.Configuration == nil {
		return nil, nil
	}
	if err := taskconfiguration.ValidateTaskSecretPinSet(task.Configuration.SecretPins); err != nil {
		return nil, err
	}
	if err := validateTaskBackingHookInputSet(task); err != nil {
		return nil, err
	}
	reference := task.Configuration.Current
	if reference == (runtimeconfiguration.Reference{}) {
		if task.Configuration.Prior != nil || task.Configuration.PriorRevision != 0 ||
			task.Configuration.BackingHookInputs == nil {
			return nil, errs.New(errs.KindStateConflict, "Task configuration authority is inconsistent")
		}
		return nil, nil
	}
	if runtimeconfiguration.ValidateReference(reference) != nil || task.RenderGeneration <= 0 ||
		reference.EnvironmentID != task.Owner.EnvironmentID ||
		reference.Generation > uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindStateConflict, "Task configuration authority is inconsistent")
	}
	return &reference, nil
}

func taskConfigurationCondition(task TaskRecord) (etcdstore.Condition, bool, error) {
	reference, err := taskRuntimeConfiguration(task)
	if err != nil || reference == nil {
		return etcdstore.Condition{}, false, err
	}
	revision, prior := task.Configuration.PriorRevision, task.Configuration.Prior
	if revision < 0 || (prior == nil) != (revision == 0) {
		return etcdstore.Condition{}, false, errs.New(
			errs.KindStateConflict,
			"Task configuration predecessor is invalid",
		)
	}
	if revision > 0 {
		if runtimeconfiguration.ValidateReference(*prior) != nil || prior.EnvironmentID != reference.EnvironmentID ||
			prior.Generation > reference.Generation {
			return etcdstore.Condition{}, false, errs.New(
				errs.KindStateConflict,
				"Task prior configuration is invalid",
			)
		}
	}
	return etcdstore.Condition{
		Key:         runtimeConfigurationHeadKey(reference.EnvironmentID),
		ModRevision: revision,
	}, true, nil
}

// Claim checks retained members as well as the head before authorizing file I/O.
// Immutable source records are never reconstructed from old Task history.
