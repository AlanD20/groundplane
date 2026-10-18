// Package attachprovision owns captured native Attach provisioning authority and
// its atomic resource settlement, independently of diagnostic Task history.
package attachprovision

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
	MeasureTransaction(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionBudget, error)
	Transact(context.Context, []keyvalue.Condition, []keyvalue.Mutation) (keyvalue.TransactionResult, error)
}

type InputRepository struct{ store Store }

func NewInputRepository(store Store) (*InputRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Attach provisioning requires captured input storage")
	}
	return &InputRepository{store: store}, nil
}

type CapturedInput struct {
	Generation keyvalue.Versioned[blueprintattachinputs.Generation]
	Conditions []keyvalue.Condition
}

func (input *CapturedInput) Clear() {
	if input != nil {
		blueprintattachinputs.Clear(&input.Generation.Record)
		*input = CapturedInput{}
	}
}

// LoadCaptured reads the original encrypted generation, never a desired head
// or a newer Secret. The returned comparisons must join any execution grant.
// It is also usable after supersession for inspection of already-owned effects.
func (repository *InputRepository) LoadCaptured(ctx context.Context, work workqueue.View) (CapturedInput, error) {
	if work.Item.Item.Input == nil {
		return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning input has not been prepared")
	}
	return repository.loadCaptured(ctx, work)
}

// LoadWaitingCapture exposes the already-published capture for input
// preparation only. It cannot be used to deliver an unprepared assignment.
func (repository *InputRepository) LoadWaitingCapture(ctx context.Context, work workqueue.View) (CapturedInput, error) {
	task, item := work.Task.Task, work.Item.Item
	if !task.PublicationComplete || task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		item.ForwardRevoked || item.Phase != workexecution.Waiting || item.Wait == nil ||
		item.Wait.Reason != workexecution.WaitInput || item.Input != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning is not waiting for captured input")
	}
	return repository.loadCaptured(ctx, work)
}

func (repository *InputRepository) loadCaptured(ctx context.Context, work workqueue.View) (_ CapturedInput, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return CapturedInput{}, err
	}
	task, item := work.Task.Task, work.Item.Item
	if repository == nil || repository.store == nil || work.ReadRevision <= 0 ||
		work.Task.Revision <= 0 || work.Task.Revision > work.ReadRevision ||
		work.Item.Revision <= 0 || work.Item.Revision > work.ReadRevision ||
		task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.Operation != workexecution.Attach {
		return CapturedInput{}, errs.New(errs.KindValidationFailed, "Attach provisioning requires captured native work")
	}
	generation, found, err := blueprintattachinputs.ReadAtRevision(ctx, repository.store, task.ID, item.TargetID, work.ReadRevision)
	if err != nil {
		return CapturedInput{}, err
	}
	if !found {
		return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning input is unavailable")
	}
	defer func() {
		if resultErr != nil {
			blueprintattachinputs.Clear(&generation.Record)
		}
	}()
	capture := generation.Record
	if !capture.MatchesWork(task, item) {
		return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning input belongs to different work")
	}
	reference, err := blueprintattachinputs.WorkInput(capture)
	if err != nil {
		return CapturedInput{}, err
	}
	if item.Input != nil && reference != *item.Input {
		return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning input changed after capture")
	}
	conditions := []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision},
		{Key: blueprintattachinputs.Key(capture.RevisionID, capture.AttachID), ModRevision: generation.Revision},
	}
	if capture.SecretPins != nil {
		pins, err := tasksecretpins.NewEtcdRepository(repository.store, capture.BackingProjectID)
		if err != nil {
			return CapturedInput{}, err
		}
		root, found, err := pins.LoadActive(ctx, capture.OperationID, work.ReadRevision)
		if err != nil {
			return CapturedInput{}, err
		}
		if !found {
			return CapturedInput{}, errs.New(errs.KindStateConflict, "Attach provisioning no longer retains its captured Secret values")
		}
		condition, err := root.AdmissionCondition(*capture.SecretPins)
		if err != nil {
			return CapturedInput{}, err
		}
		conditions = append(conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	return CapturedInput{Generation: generation, Conditions: conditions}, nil
}
