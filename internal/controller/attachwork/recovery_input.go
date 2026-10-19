package attachwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *Reports) LoadRecoveryAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.AttachProvisionRecoveryAssignment, error) {
	if !view.CurrentExecutor || !view.ActiveAttempt || view.Work.Task.Task.CollectionStartedAt != nil ||
		!view.Work.Task.Task.PublicationComplete || workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "Attach inspection no longer owns active execution")
	}
	input, _, err := owner.inputs.Recovery(ctx, view.Work, view.Attempt.Attempt)
	if err != nil {
		return nil, err
	}
	return attachexecution.EncodeProvisionRecoveryAssignment(input)
}

// Recovery loads inspection authority without opening encrypted credentials.
// The work's original generation remains pinned while effects are unresolved.
func (owner *Inputs) Recovery(
	ctx context.Context,
	view workqueue.View,
	attempt workexecution.Attempt,
) (attachexecution.ProvisionRecoveryInput, []keyvalue.Condition, error) {
	saved, err := owner.repository.LoadRecovery(ctx, view)
	if err != nil {
		return attachexecution.ProvisionRecoveryInput{}, nil, err
	}
	input := attachexecution.ProvisionRecoveryInput{Attempt: attempt, Source: saved.Source}
	if err := input.Validate(); err != nil {
		return attachexecution.ProvisionRecoveryInput{}, nil, err
	}
	source, err := owner.repository.LoadCaptured(ctx, view)
	if err != nil {
		return attachexecution.ProvisionRecoveryInput{}, nil, err
	}
	defer source.Clear()
	capture, authority := source.Generation.Record, saved.Source.Failure.Authority
	expected, err := capture.ProvisionAuthority()
	if err != nil {
		return attachexecution.ProvisionRecoveryInput{}, nil, err
	}
	if capture.SettlesInController() || authority != expected {
		return attachexecution.ProvisionRecoveryInput{}, nil, errs.New(
			errs.KindStateConflict,
			"Attach inspection differs from the original provisioning input",
		)
	}
	if input.RequiresConsumer() {
		input.Consumer, err = capture.Consumer()
		if err != nil {
			return attachexecution.ProvisionRecoveryInput{}, nil, err
		}
	}
	if err := input.Validate(); err != nil {
		return attachexecution.ProvisionRecoveryInput{}, nil, err
	}
	guards := append(
		source.Conditions,
		keyvalue.Condition{Key: attachprovision.RecoveryKey(view.Item.Item.ID), ModRevision: saved.Revision},
	)
	return input, guards, nil
}
