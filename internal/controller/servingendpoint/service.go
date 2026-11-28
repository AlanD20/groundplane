// Package servingendpoint coordinates durable Service endpoint publication
// with authenticated Host readiness and edge application evidence.
package servingendpoint

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/servingendpoint"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	storage "github.com/AlanD20/groundplane/internal/infra/etcd/servingendpoints"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Repository interface {
	ReadAt(context.Context, storage.Target, int64) (storage.Cell, error)
	Replace(context.Context, storage.Cell, json.RawMessage, []keyvalue.Condition) error
}

type ServingRequest struct {
	Target       domain.Target
	HostID       string
	EdgeHostID   string
	ReadRevision int64
}

type ServingEvidence struct {
	Authority  domain.ServingAuthority
	Proof      domain.ServingProof
	Conditions []keyvalue.Condition
}

// ServingSource captures the acknowledged Release and exact executor from
// the requested MVCC snapshot and freshly verifies private edge reachability.
type ServingSource interface {
	CaptureServing(context.Context, ServingRequest) (ServingEvidence, error)
}

type EdgeRequest struct {
	Target       domain.Target
	HostID       string
	ReadRevision int64
	Desired      domain.Revision
	Applied      domain.Revision
}

type AppliedEvidence struct {
	Live       domain.EdgeAck
	Conditions []keyvalue.Condition
}

type RestorationEvidence struct {
	AppliedEvidence
	// Fenced is the exact rejected revision the edge can no longer activate.
	Fenced domain.EdgeAck
}

type DrainEvidence struct {
	Proof      domain.DrainProof
	Conditions []keyvalue.Condition
}

// EdgeSource is implemented by the trusted edge runtime or its authenticated
// receipt reader. Captures must fence the corresponding runtime incarnation
// and the immutable report at the requested MVCC revision.
type EdgeSource interface {
	CaptureApplied(context.Context, EdgeRequest) (AppliedEvidence, error)
	CaptureRestoration(context.Context, EdgeRequest) (RestorationEvidence, error)
	CaptureDrain(context.Context, EdgeRequest, domain.Drain) (DrainEvidence, error)
}

type Service struct {
	repository Repository
	serving    ServingSource
	edge       EdgeSource
	edgeHostID string
	now        func() time.Time
	maxAge     time.Duration
}

func New(repository Repository, serving ServingSource, edge EdgeSource, edgeHostID string,
	now func() time.Time, proofMaxAge time.Duration,
) (*Service, error) {
	if repository == nil || serving == nil || edge == nil || now == nil || proofMaxAge <= 0 ||
		ids.Validate(ids.KindHost, edgeHostID) != nil {
		return nil, errs.New(
			errs.KindValidationFailed,
			"serving endpoints require an edge Host and authenticated runtime authority",
		)
	}
	return &Service{repository: repository, serving: serving, edge: edge, edgeHostID: edgeHostID,
		now: now, maxAge: proofMaxAge}, nil
}

type document struct {
	EdgeHostID string       `json:"edge_host_id"`
	State      domain.State `json:"state"`
}

func (service *Service) Read(ctx context.Context, target domain.Target) (domain.State, error) {
	_, state, err := service.read(ctx, target)
	return state, err
}

func (service *Service) read(ctx context.Context, target domain.Target) (storage.Cell, domain.State, error) {
	cell, err := service.repository.ReadAt(ctx, storage.Target(target), 0)
	if err != nil {
		return cell, domain.State{}, err
	}
	if cell.Revision == 0 {
		state, err := domain.New(target)
		return cell, state, err
	}
	persisted, err := decode(cell.Payload)
	clear(cell.Payload)
	cell.Payload = nil
	if err != nil || persisted.State.Target != target || persisted.State.Validate() != nil {
		return cell, domain.State{}, errs.New(errs.KindInternal, "durable serving endpoint state is invalid")
	}
	if persisted.EdgeHostID != service.edgeHostID {
		return cell, domain.State{}, conflict("serving endpoint state belongs to a different edge Host")
	}
	return cell, persisted.State, nil
}

func (service *Service) write(
	ctx context.Context, cell storage.Cell, state domain.State, conditions []keyvalue.Condition,
) (domain.State, error) {
	if err := state.Validate(); err != nil {
		return domain.State{}, err
	}
	if len(conditions) == 0 {
		return domain.State{}, conflict("serving endpoint transition requires revision-fenced runtime evidence")
	}
	payload, err := json.Marshal(document{EdgeHostID: service.edgeHostID, State: state})
	if err != nil {
		return domain.State{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(payload)
	if err := service.repository.Replace(ctx, cell, payload, conditions); err != nil {
		return domain.State{}, err
	}
	return state, nil
}

func (service *Service) captureServing(ctx context.Context, target domain.Target, hostID string,
	revision int64,
) (ServingEvidence, error) {
	if ids.Validate(ids.KindHost, hostID) != nil {
		return ServingEvidence{}, errs.New(errs.KindValidationFailed, "serving endpoint requires a stable Host ID")
	}
	evidence, err := service.serving.CaptureServing(ctx, ServingRequest{
		Target: target, HostID: hostID, EdgeHostID: service.edgeHostID, ReadRevision: revision,
	})
	if err != nil {
		return ServingEvidence{}, err
	}
	if evidence.Authority.Target != target || evidence.Proof.Target != target ||
		evidence.Authority.Endpoint.HostID != hostID || evidence.Authority.Endpoint != evidence.Proof.Endpoint {
		return ServingEvidence{}, conflict("captured serving proxy differs from the selected Host and Service port")
	}
	return evidence, nil
}

// Publish issues a desired revision from an acknowledged and reachable
// runtime. The existing applied revision stays active until the edge responds.
func (service *Service) Publish(ctx context.Context, target domain.Target, hostID string) (domain.State, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil {
		return domain.State{}, err
	}
	evidence, err := service.captureServing(ctx, target, hostID, cell.ReadRevision)
	if err != nil {
		return domain.State{}, err
	}
	next, err := state.Publish(evidence.Authority, evidence.Proof, service.now(), service.maxAge)
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, cell, next, evidence.Conditions)
}

func (service *Service) edgeRequest(cell storage.Cell, state domain.State) EdgeRequest {
	return EdgeRequest{Target: state.Target, HostID: service.edgeHostID, ReadRevision: cell.ReadRevision,
		Desired: state.Desired, Applied: state.Applied}
}

func (service *Service) Acknowledge(ctx context.Context, target domain.Target) (domain.State, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil || state.Desired.Number == state.Applied.Number {
		return state, err
	}
	evidence, err := service.edge.CaptureApplied(ctx, service.edgeRequest(cell, state))
	if err != nil {
		return domain.State{}, err
	}
	if evidence.Live != state.Desired.Witness(target) {
		return domain.State{}, conflict("the desired serving endpoint revision has not applied")
	}
	next, err := state.Acknowledge(evidence.Live)
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, cell, next, evidence.Conditions)
}

func (service *Service) RestorePrevious(ctx context.Context, target domain.Target) (domain.State, error) {
	cell, state, err := service.read(ctx, target)
	if err != nil || state.Desired.Number == state.Applied.Number {
		return state, err
	}
	evidence, err := service.edge.CaptureRestoration(ctx, service.edgeRequest(cell, state))
	if err != nil {
		return domain.State{}, err
	}
	next, err := state.RestorePrevious(evidence.Fenced, evidence.Live)
	if err != nil {
		return domain.State{}, err
	}
	return service.write(ctx, cell, next, evidence.Conditions)
}

func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
