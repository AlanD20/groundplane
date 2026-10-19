package componentwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Recovery restores the predecessor, whose applied records were never advanced
// by the failed forward attempt. Keep their original execution identity; only
// release candidate reservations after exact restoration has been proved.
func (repository *InputRepository) PrepareRestoredState(ctx context.Context, view workqueue.ResumeView,
	raw []byte, at time.Time,
) (workqueue.Change, workexecution.Completion, error) {
	capture, proof, result, err := repository.PrepareRecoveryProof(ctx, view, raw, at)
	if err != nil {
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	keep := capture.Current
	keep.Desired.Enabled = false
	keep.Runtime = componentrecord.RuntimeRecord{}
	if capture.Predecessor != nil {
		keep = capture.Predecessor.Component
	}
	addresses, err := repository.settleAddresses(ctx, view.Work, capture, keep)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	proof.Conditions = append(proof.Conditions, addresses.Conditions...)
	proof.Mutations = append(proof.Mutations, addresses.Mutations...)
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	conditions, err := sources.WorkConditions(ctx, capture.Configuration, view.Work.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(proof.Mutations)
		return workqueue.Change{}, workexecution.Completion{}, err
	}
	for _, condition := range conditions {
		proof.Conditions = append(
			proof.Conditions,
			keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision},
		)
	}
	proof.Conditions = append(proof.Conditions, keyvalue.Condition{
		Key: componentplanning.RuntimeKey(view.Work.Item.Item.TargetID), ModRevision: capture.RuntimeRevision})
	return proof, result, nil
}
