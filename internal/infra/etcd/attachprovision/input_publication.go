package attachprovision

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InputPublication is an immutable capture and its Secret-pin activation.
// The caller commits Change with the Task, item and resource ownership, never
// separately. Abandon is only for a publication proven absent or not attempted.
type InputPublication struct {
	Generation blueprintattachinputs.Generation
	Change     workqueue.Change
	pins       *tasksecretpins.Repository
	store      Store
}

func (publication *InputPublication) Abandon(ctx context.Context) error {
	if publication == nil || publication.pins == nil {
		return nil
	}
	return publication.pins.Abandon(ctx, publication.Generation.OperationID)
}

// PrepareInput binds one draft to one native work item. Both direct Attach and
// Apply use this capture; neither creates an execution plan or child Task.
func (repository *InputRepository) PrepareInput(ctx context.Context, task workexecution.Task, workID string,
	generation blueprintattachinputs.Generation, guards []keyvalue.Condition,
) (_ InputPublication, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return InputPublication{}, err
	}
	if repository == nil || repository.store == nil || task.Validate() != nil ||
		task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil ||
		ids.Validate(ids.KindWorkItem, workID) != nil || blueprintattachinputs.ValidateDraft(generation) != nil ||
		generation.ParentTaskID != task.ID || generation.RevisionID != task.ID || generation.EnvironmentID != task.Scope.EnvironmentID ||
		(task.Action != workexecution.ActionApply && task.Action != workexecution.ActionAttach) ||
		(task.Action == workexecution.ActionAttach && (task.TargetID != generation.AttachID || generation.Binding != nil)) ||
		(task.Action == workexecution.ActionApply && task.TargetID != generation.EnvironmentID) {
		return InputPublication{}, errs.New(errs.KindValidationFailed, "Attach input publication requires its unpublished native owner")
	}
	publication := InputPublication{Generation: generation, store: repository.store}
	publication.Change.Conditions = append([]keyvalue.Condition(nil), guards...)
	publication.Change.Conditions = append(publication.Change.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID)},
		keyvalue.Condition{Key: workqueue.ItemKey(workID)},
		keyvalue.Condition{Key: blueprintattachinputs.Key(task.ID, generation.AttachID)},
	)
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(publication.Change.Mutations)
			cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, publication.Abandon(cleanupCtx))
		}
	}()
	var count uint64
	var digest string
	if len(generation.SecretSources) != 0 {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, generation.BackingProjectID)
		if err != nil {
			return InputPublication{}, err
		}
		publication.pins = pins
		// Pin batches fence their unpublished owner, not the entire resource
		// graph. Resource comparisons join final activation; copying an Apply's
		// accumulated comparisons here would exceed the bounded batch limit.
		pinGuards := []tasksecretpins.Condition{
			{Key: workqueue.TaskKey(task.ID)}, {Key: workqueue.ItemKey(workID)},
			{Key: blueprintattachinputs.Key(task.ID, generation.AttachID)},
		}
		prepared, err := pins.PrepareOwned(ctx, generation.OperationID, task.ID, workID, generation.SecretSources, pinGuards)
		if err != nil {
			return InputPublication{}, err
		}
		count, digest = prepared.MembershipCount(), prepared.MembershipSHA256()
		activation, err := tasksecretpins.Activation(prepared)
		if err != nil {
			return InputPublication{}, err
		}
		conditions, mutations, err := tasksecretpins.EtcdFragment(activation)
		activation.Clear()
		if err != nil {
			return InputPublication{}, err
		}
		publication.Change.Conditions = append(publication.Change.Conditions, conditions...)
		publication.Change.Mutations = append(publication.Change.Mutations, mutations...)
	}
	bound, err := blueprintattachinputs.BindSecretPins(generation, workID, count, digest)
	if err != nil {
		return InputPublication{}, err
	}
	publication.Generation = bound
	value, err := blueprintattachinputs.Encode(bound)
	if err != nil {
		return InputPublication{}, err
	}
	publication.Change.Mutations = append(publication.Change.Mutations, keyvalue.Mutation{
		Type: keyvalue.MutationPut, Key: blueprintattachinputs.Key(task.ID, generation.AttachID), Value: value,
	})
	return publication, nil
}
