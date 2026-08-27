package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CoreDNSComponentExecutor is the adapter used by the existing component
// execution seam. The implementation owns container identity, mounted paths,
// and commands; the Agent supplies only the trusted plan and enforces order.
type CoreDNSComponentExecutor interface {
	ValidateCorefile(context.Context, coredns.TaskPlan) error
	ReloadCorefile(context.Context, coredns.TaskPlan) error
	ObserveCoreDNS(context.Context, coredns.TaskPlan) (bool, error)
}

// CoreDNSDisableExecutor is implemented by runtimes that can remove the
// serving CoreDNS workload. It is deliberately optional because an enabled
// apply must never silently turn into a disable operation.
type CoreDNSDisableExecutor interface {
	DisableCoreDNS(context.Context) (bool, error)
}

// CoreDNSStepRuntime is the worker-facing typed component runtime. It receives
// the already validated assignment and is responsible for materializing the
// bounded Corefile before calling ApplyCoreDNSComponent.
type CoreDNSStepRuntime interface {
	ExecuteCoreDNS(context.Context, Assignment, *agentpb.ExecutionStep) error
}

// ApplyCoreDNSComponent is the typed Agent transport entry point. The union is
// decoded here, before any filesystem or container operation is attempted.
func ApplyCoreDNSComponent(
	ctx context.Context,
	executor CoreDNSComponentExecutor,
	disableExecutor CoreDNSDisableExecutor,
	component *agentpb.ComponentApply,
	corefile []byte,
) (coredns.ObservedState, error) {
	if component == nil {
		return coredns.ObservedState{}, errs.New(errs.KindValidationFailed, "coredns: component apply payload is required")
	}
	payload := component.GetCorednsConfigApply()
	if err := validateCoreDNSPayload(payload, corefile); err != nil {
		return coredns.ObservedState{}, err
	}
	if payload.Mode == agentpb.CoreDNSApplyMode_COREDNS_DISABLE {
		if disableExecutor == nil {
			return coredns.ObservedState{}, errs.New(errs.KindValidationFailed, "coredns: disable executor is required")
		}
		healthy, err := disableExecutor.DisableCoreDNS(ctx)
		if err != nil {
			return coredns.ObservedState{}, errs.Wrap(errs.KindRequestFailed, err)
		}
		return coredns.ObservedState{
			ComponentID: payload.ComponentId, ServiceID: payload.ServiceId, Enabled: false,
			Healthy: healthy, DesiredGeneration: payload.DesiredGeneration,
			RenderGeneration: payload.RenderGeneration, AgentID: payload.AgentId,
			AgentGeneration: payload.AgentGeneration, BaselineGeneration: payload.BaselineGeneration,
			OwnershipGeneration: payload.OwnershipGeneration,
		}, nil
	}
	if executor == nil {
		return coredns.ObservedState{}, errs.New(errs.KindValidationFailed, "coredns: component executor is required")
	}
	plan := coredns.TaskPlan{
		ComponentID: payload.ComponentId, ServiceID: payload.ServiceId,
		Corefile:       append([]byte(nil), corefile...),
		CorefileSHA256: sha256.Sum256(corefile),
		InputSHA256:    bytesToDigest(payload.NormalizedInputSha256),
		Steps: []coredns.TaskStep{
			coredns.TaskStepValidateConfig, coredns.TaskStepRender,
			coredns.TaskStepApply, coredns.TaskStepObserve,
		},
	}
	state, err := ApplyCoreDNS(ctx, executor, plan)
	if err != nil {
		return coredns.ObservedState{}, err
	}
	state.DesiredGeneration = payload.DesiredGeneration
	state.RenderGeneration = payload.RenderGeneration
	state.AgentID = payload.AgentId
	state.AgentGeneration = payload.AgentGeneration
	state.BaselineGeneration = payload.BaselineGeneration
	state.OwnershipGeneration = payload.OwnershipGeneration
	return state, nil
}

// ApplyCoreDNSConfig accepts the concrete generated payload for callers that
// already selected the closed ComponentApply union.
func ApplyCoreDNSConfig(
	ctx context.Context,
	executor CoreDNSComponentExecutor,
	payload *agentpb.CoreDNSConfigApply,
	corefile []byte,
) (coredns.ObservedState, error) {
	return ApplyCoreDNSComponent(ctx, executor, nil, &agentpb.ComponentApply{
		ComponentPayload: &agentpb.ComponentApply_CorednsConfigApply{CorednsConfigApply: payload},
	}, corefile)
}

func validateCoreDNSPayload(payload *agentpb.CoreDNSConfigApply, corefile []byte) error {
	if payload == nil || payload.ComponentId == "" || payload.ServiceId == "" || payload.AgentId == "" ||
		payload.DesiredGeneration == 0 || payload.RenderGeneration == 0 || payload.AgentGeneration == 0 ||
		payload.OwnershipGeneration == 0 || payload.ImageIndexRef == "" || payload.ImageChildDigest == "" ||
		payload.Platform == "" || payload.Mode == agentpb.CoreDNSApplyMode_COREDNS_APPLY_MODE_UNSPECIFIED {
		return errs.New(errs.KindValidationFailed, "coredns: typed apply identity is incomplete")
	}
	if payload.Mode < agentpb.CoreDNSApplyMode_COREDNS_INITIAL_ENABLE || payload.Mode > agentpb.CoreDNSApplyMode_COREDNS_REPAIR {
		return errs.New(errs.KindValidationFailed, "coredns: typed apply mode is unsupported")
	}
	if payload.Mode == agentpb.CoreDNSApplyMode_COREDNS_DISABLE {
		if len(corefile) != 0 || payload.CandidateArtifactId != "" || len(payload.CandidateComposeSha256) != 0 ||
			payload.CorefileLength != 0 || len(payload.CorefileSha256) != 0 || len(payload.NormalizedInputSha256) != 0 {
			return errs.New(errs.KindValidationFailed, "coredns: disabled apply carries rendered state")
		}
		return nil
	}
	if len(corefile) == 0 || len(corefile) > 96*1024 || uint32(len(corefile)) != payload.CorefileLength ||
		len(payload.CorefileSha256) != sha256.Size || !bytes.Equal(sha256Bytes(corefile), payload.CorefileSha256) ||
		len(payload.NormalizedInputSha256) != sha256.Size || payload.CandidateArtifactId == "" || len(payload.CandidateComposeSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "coredns: typed Corefile proof does not match bytes")
	}
	if strings.IndexFunc(payload.Platform, func(r rune) bool { return r < 0x21 || r > 0x7e }) >= 0 ||
		strings.IndexFunc(payload.ImageIndexRef, func(r rune) bool { return r < 0x21 || r > 0x7e }) >= 0 ||
		strings.IndexFunc(payload.ImageChildDigest, func(r rune) bool { return r < 0x21 || r > 0x7e }) >= 0 {
		return errs.New(errs.KindValidationFailed, "coredns: typed image identity is invalid")
	}
	return nil
}

func sha256Bytes(value []byte) []byte {
	digest := sha256.Sum256(value)
	return digest[:]
}

func bytesToDigest(value []byte) [32]byte {
	var result [32]byte
	copy(result[:], value)
	return result
}

// ApplyCoreDNS executes the closed validate-before-reload procedure. A failed
// validation returns before ReloadCorefile, preserving the last-known-good
// serving configuration.
func ApplyCoreDNS(ctx context.Context, executor CoreDNSComponentExecutor, plan coredns.TaskPlan) (coredns.ObservedState, error) {
	if executor == nil {
		return coredns.ObservedState{}, errs.New(errs.KindValidationFailed, "coredns: component executor is required")
	}
	if err := plan.Validate(); err != nil {
		return coredns.ObservedState{}, err
	}
	if err := executor.ValidateCorefile(ctx, plan); err != nil {
		return coredns.ObservedState{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	if err := executor.ReloadCorefile(ctx, plan); err != nil {
		return coredns.ObservedState{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	healthy, err := executor.ObserveCoreDNS(ctx, plan)
	if err != nil {
		return coredns.ObservedState{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	return coredns.Observe(plan, healthy)
}
