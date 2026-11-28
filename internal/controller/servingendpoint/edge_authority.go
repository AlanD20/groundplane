package servingendpoint

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/hostplacement"
	domain "github.com/AlanD20/groundplane/internal/core/servingendpoint"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EdgeRuntimeRequest struct {
	EdgeRequest
	Executor          workexecution.Identity
	ProcessGeneration [16]byte
	Drain             *domain.Drain
}

type EdgeRuntimeProof struct {
	Target            domain.Target
	Executor          workexecution.Identity
	ProcessGeneration [16]byte
	Live              domain.EdgeAck
	Fenced            domain.EdgeAck
	Drain             *domain.DrainProof
	Conditions        []keyvalue.Condition
}

// EdgeRuntimeSource reads authenticated runtime evidence about the immutable
// endpoint snapshot. The runtime must distinguish an activated configuration
// from a prepared file and verify the retirement fence itself.
type EdgeRuntimeSource interface {
	CaptureEndpointState(context.Context, EdgeRuntimeRequest) (EdgeRuntimeProof, error)
}

type EdgeAuthority struct {
	placement *hostplacement.Resolver
	runtime   EdgeRuntimeSource
}

func NewEdgeAuthority(placement *hostplacement.Resolver, runtime EdgeRuntimeSource) (*EdgeAuthority, error) {
	if placement == nil || runtime == nil {
		return nil, errs.New(errs.KindValidationFailed, "edge authority requires an authenticated Host runtime")
	}
	return &EdgeAuthority{placement: placement, runtime: runtime}, nil
}

func (authority *EdgeAuthority) capture(
	ctx context.Context, request EdgeRequest, drain *domain.Drain,
) (EdgeRuntimeProof, error) {
	selected, err := authority.placement.Resolve(ctx, request.HostID, request.ReadRevision, workexecution.PublicIngress)
	if err != nil {
		return EdgeRuntimeProof{}, err
	}
	proof, err := authority.runtime.CaptureEndpointState(ctx, EdgeRuntimeRequest{
		EdgeRequest: request, Executor: selected.Identity, ProcessGeneration: selected.ProcessGeneration,
		Drain: drain,
	})
	if err != nil {
		return EdgeRuntimeProof{}, err
	}
	if proof.Target != request.Target || proof.Executor != selected.Identity ||
		proof.ProcessGeneration != selected.ProcessGeneration || len(proof.Conditions) == 0 {
		return EdgeRuntimeProof{}, conflict("edge receipt differs from the current Host runtime and Service target")
	}
	proof.Conditions = slices.Concat(selected.Conditions, proof.Conditions)
	return proof, nil
}

func (authority *EdgeAuthority) CaptureApplied(ctx context.Context, request EdgeRequest) (AppliedEvidence, error) {
	proof, err := authority.capture(ctx, request, nil)
	return AppliedEvidence{Live: proof.Live, Conditions: proof.Conditions}, err
}

func (authority *EdgeAuthority) CaptureRestoration(
	ctx context.Context,
	request EdgeRequest,
) (RestorationEvidence, error) {
	proof, err := authority.capture(ctx, request, nil)
	return RestorationEvidence{AppliedEvidence: AppliedEvidence{Live: proof.Live, Conditions: proof.Conditions},
		Fenced: proof.Fenced}, err
}

func (authority *EdgeAuthority) CaptureDrain(
	ctx context.Context,
	request EdgeRequest,
	drain domain.Drain,
) (DrainEvidence, error) {
	proof, err := authority.capture(ctx, request, &drain)
	if err != nil {
		return DrainEvidence{}, err
	}
	if proof.Drain == nil || proof.Live != request.Applied.Witness(request.Target) {
		return DrainEvidence{}, conflict("edge cannot prove the acknowledged configuration and its retired connections")
	}
	return DrainEvidence{Proof: *proof.Drain, Conditions: proof.Conditions}, nil
}
