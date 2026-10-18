package releaseoperation

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// InputPreparation publishes the executable form of an already accepted direct
// Release. It shares Release execution with Apply, not Blueprint authoring or
// desired-state publication. No Script, image or configuration is reselected.
type InputPreparation struct {
	ledger *etcd.ReleaseLedger
	queue  *workqueue.Repository
	work   *blueprintrelease.Service
}

func NewInputPreparation(ledger *etcd.ReleaseLedger, queue *workqueue.Repository, work *blueprintrelease.Service) (*InputPreparation, error) {
	if ledger == nil || queue == nil || work == nil {
		return nil, errs.New(errs.KindInternal, "Direct Release preparation dependencies are not configured")
	}
	return &InputPreparation{ledger: ledger, queue: queue, work: work}, nil
}

func (owner *InputPreparation) Prepare(ctx context.Context, taskID, workID string) error {
	view, err := owner.queue.ReadWork(ctx, taskID, workID, 0)
	if err != nil {
		return err
	}
	if view.Task.Task.Action != workexecution.ActionDeploy && view.Task.Task.Action != workexecution.ActionRollback {
		return errs.New(errs.KindStateConflict, "Direct Release preparation received another Task action")
	}
	request, _, err := owner.ledger.ReadDirectReleasePreparation(ctx, view)
	if err != nil {
		return err
	}
	current, guards, err := owner.ledger.MatchDirectReleaseSource(ctx, view, request)
	if err != nil {
		return err
	}
	if !current {
		change, err := releasework.PrepareUnstartedSupersession(view, guards, time.Now().UTC())
		if err != nil {
			return err
		}
		defer keyvalue.ZeroMutationBytes(change.Mutations)
		result, err := owner.queue.Commit(ctx, change)
		keyvalue.ClearValues(result.FailureReads)
		return err
	}
	rollout, err := owner.ledger.GetServiceRollout(ctx, view)
	if err != nil {
		return err
	}
	defer clear(rollout.Record.CandidateArtifact)
	artifact, err := releaserender.OpenReleaseCandidateArtifact(rollout.Record)
	if err != nil {
		return err
	}
	networks, err := executionplan.CaptureReleaseNetworkPreparation(artifact,
		ids.DeriveAt(ids.KindConfig, view.Item.Item.CreatedAt, view.Item.Item.ID, "dns-readiness"))
	if err != nil {
		return err
	}
	activation, ready, err := owner.ledger.ReadReleaseZoneDNSActivation(ctx, networks.Upstreams)
	if err != nil || !ready {
		return err
	}
	prepared, err := owner.work.PrepareCapturedService(ctx, view, request, rollout)
	if err != nil {
		return err
	}
	defer prepared.Clear()
	publication, err := prepared.Publication.WithZoneDNSActivation(activation)
	if err != nil {
		return errors.Join(err, prepared.Abandon(ctx))
	}
	prepared.Publication = publication
	dependencies := make([]workqueue.ItemSnapshot, len(view.Item.Item.Dependencies))
	for index, id := range view.Item.Item.Dependencies {
		dependency, err := owner.queue.ReadWork(ctx, taskID, id, view.ReadRevision)
		if err != nil {
			return errors.Join(err, prepared.Abandon(ctx))
		}
		dependencies[index] = dependency.Item
	}
	change, err := prepared.Change(dependencies)
	if err != nil {
		return errors.Join(err, prepared.Abandon(ctx))
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	if !time.Now().Before(view.Task.Task.Deadline) || !time.Now().Before(view.Item.Item.Deadline) {
		return errors.Join(errs.New(errs.KindStateConflict, "Direct Release input preparation exceeded its deadline"), prepared.Abandon(ctx))
	}
	result, err := owner.queue.Commit(ctx, change)
	if err != nil {
		// Unknown publication outcomes retain prepared sources. Redelivery reads
		// the same native item; it cannot reselect or run an old forward operation.
		return err
	}
	keyvalue.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return errors.Join(errs.New(errs.KindStateConflict, "Direct Release inputs changed during preparation"), prepared.Abandon(ctx))
	}
	return nil
}
