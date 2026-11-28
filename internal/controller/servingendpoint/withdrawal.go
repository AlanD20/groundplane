package servingendpoint

import (
	"context"
	"slices"

	domain "github.com/AlanD20/groundplane/internal/core/servingendpoint"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/servingendpoints"
)

// PreparedMove binds source retirement to the exact applied edge snapshot
// and current evidence of a different, reachable serving Host. Changing the
// snapshot or losing the replacement before submission makes the plan stale.
type PreparedMove struct {
	service    *Service
	cell       storage.Cell
	state      domain.State
	leaving    string
	proof      domain.ServingProof
	conditions []keyvalue.Condition
}

func (service *Service) PrepareMove(
	ctx context.Context, target domain.Target, leavingHost, replacementHost string,
) (PreparedMove, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil {
		return PreparedMove{}, err
	}
	replacement, err := service.captureServing(ctx, target, replacementHost, cell.ReadRevision)
	if err != nil {
		return PreparedMove{}, err
	}
	if len(replacement.Conditions) == 0 {
		return PreparedMove{}, conflict("prepared move requires revision-fenced replacement evidence")
	}
	applied, err := service.confirmApplied(ctx, cell, state)
	if err != nil {
		return PreparedMove{}, err
	}
	move := PreparedMove{service: service, cell: cell, state: state, leaving: leavingHost,
		proof: replacement.Proof, conditions: slices.Concat(replacement.Conditions, applied.Conditions)}
	if _, err := state.BeginDrain(leavingHost, move.withdrawal(), service.now()); err != nil {
		return PreparedMove{}, err
	}
	return move, nil
}

func (move PreparedMove) withdrawal() domain.Withdrawal {
	return domain.Withdrawal{Mode: domain.WithdrawalMove, Replacement: &move.proof, MaxProofAge: move.service.maxAge}
}

func (service *Service) BeginMove(ctx context.Context, move PreparedMove) (domain.State, error) {
	if move.service != service {
		return domain.State{}, conflict("serving endpoint withdrawal requires a prepared move")
	}
	next, err := move.state.BeginDrain(move.leaving, move.withdrawal(), service.now())
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, move.cell, next, move.conditions)
}

// Unpublish stages an explicit withdrawal. The endpoint remains reserved
// until the edge acknowledges removal and completes connection draining.
func (service *Service) Unpublish(ctx context.Context, target domain.Target, hostID string) (domain.State, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil {
		return domain.State{}, err
	}
	next, err := state.BeginDrain(hostID, domain.Withdrawal{Mode: domain.WithdrawalUnpublish}, service.now())
	if err != nil {
		return domain.State{}, err
	}
	evidence, err := service.confirmApplied(ctx, cell, state)
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, cell, next, evidence.Conditions)
}

func (service *Service) confirmApplied(
	ctx context.Context,
	cell storage.Cell,
	state domain.State,
) (AppliedEvidence, error) {
	evidence, err := service.edge.CaptureApplied(ctx, service.edgeRequest(cell, state))
	if err != nil {
		return AppliedEvidence{}, err
	}
	if evidence.Live != state.Applied.Witness(state.Target) || len(evidence.Conditions) == 0 {
		return AppliedEvidence{}, conflict("the edge must confirm the applied revision before withdrawing an endpoint")
	}
	return evidence, nil
}

func (service *Service) FinishDrain(ctx context.Context, target domain.Target, hostID string) (domain.State, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil {
		return domain.State{}, err
	}
	index := slices.IndexFunc(state.Draining, func(drain domain.Drain) bool { return drain.Endpoint.HostID == hostID })
	if index == -1 {
		return domain.State{}, conflict("the selected Host has no pending endpoint drain")
	}
	evidence, err := service.edge.CaptureDrain(ctx, service.edgeRequest(cell, state), state.Draining[index])
	if err != nil {
		return domain.State{}, err
	}
	if evidence.Proof.Endpoint != state.Draining[index].Endpoint || evidence.Proof.ObservedAt.After(service.now()) {
		return domain.State{}, conflict("edge drain receipt differs from the withdrawing endpoint")
	}
	next, err := state.FinishDrain(evidence.Proof)
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, cell, next, evidence.Conditions)
}
