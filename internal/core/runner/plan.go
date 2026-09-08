package runner

import (
	"fmt"
	"net/netip"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type OwnerKind string

const (
	OwnerTenant  OwnerKind = "tenant"
	OwnerProject OwnerKind = "project"
)

type Target struct {
	RunnerID         string
	TenantID         string
	OwnerKind        OwnerKind
	OwnerID          string
	GitHubURL        string
	Labels           []string
	ImageRef         string
	RuntimeEpoch     uint64
	AllocationConfig runnerallocation.RunnerAllocationConfig
	Allocation       runnerallocation.RunnerHostAllocationRecord
}

type IsolationPolicy struct {
	RunnerPool         netip.Prefix
	DeniedCIDRs        []netip.Prefix
	ControllerEndpoint netip.AddrPort
}

type Plan = runnerallocation.RuntimePlan
type Identity = runnerallocation.RunnerRuntimeIdentity
type Paths = runnerallocation.RunnerRuntimePaths
type Network = runnerallocation.RunnerRuntimeNetwork
type Egress = runnerallocation.RunnerRuntimeEgress
type Container = runnerallocation.RunnerContainer
type Step = runnerallocation.RunnerRuntimeStep
type StepEvidence = runnerallocation.RunnerRuntimeStepEvidence

const (
	StepEnsureIdentity = runnerallocation.StepEnsureIdentity
	StepEnsureNetwork  = runnerallocation.StepEnsureNetwork
	StepEnsureEgress   = runnerallocation.StepEnsureEgress
	StepStartProxy     = runnerallocation.StepStartProxy
	StepStartDaemon    = runnerallocation.StepStartDaemon
	StepStartRunner    = runnerallocation.StepStartRunner
	StepStopRunner     = runnerallocation.StepStopRunner
	StepStopDaemon     = runnerallocation.StepStopDaemon
	StepStopProxy      = runnerallocation.StepStopProxy
	StepRemoveNetwork  = runnerallocation.StepRemoveNetwork
	StepRemoveEgress   = runnerallocation.StepRemoveEgress
	StepRemoveIdentity = runnerallocation.StepRemoveIdentity
	EffectApplied      = runnerallocation.RuntimeEffectApplied
	EffectAbsent       = runnerallocation.RuntimeEffectAbsent
)

func CreationSteps() []Step { return runnerallocation.CreationRuntimeSteps() }
func RemovalSteps() []Step  { return runnerallocation.RemovalRuntimeSteps() }

func NewPlan(target Target, policy IsolationPolicy) (Plan, error) {
	if err := validateTarget(target); err != nil {
		return Plan{}, err
	}
	if err := validatePolicy(target.Allocation.NetworkCIDR, target.AllocationConfig, policy); err != nil {
		return Plan{}, err
	}
	prefix := netip.MustParsePrefix(target.Allocation.NetworkCIDR)
	gateway := prefix.Addr().Next()
	runnerAddress := gateway.Next()
	identityToken := stableToken(target.RunnerID)
	account := "gp_runner_" + identityToken
	slotRoot := fmt.Sprintf("/var/lib/groundplane/runner-slots/slot-%02d", target.Allocation.Slot)
	home := path.Join(slotRoot, "runner")
	proxySocket := fmt.Sprintf("/run/user/%d/groundplane-docker-proxy/docker.sock", target.Allocation.HostUID)
	labels := append([]string(nil), target.Labels...)
	denied := append([]netip.Prefix(nil), policy.DeniedCIDRs...)
	return Plan{
		RunnerID: target.RunnerID, TenantID: target.TenantID, OwnerKind: string(target.OwnerKind),
		OwnerID: target.OwnerID, RuntimeEpoch: target.RuntimeEpoch,
		AllocationConfig: target.AllocationConfig, Allocation: target.Allocation,
		Identity: Identity{
			User: account, Group: account, UID: target.Allocation.HostUID, GID: target.Allocation.HostUID,
			SubUIDStart: target.Allocation.SubUIDStart, SubUIDCount: target.Allocation.SubUIDCount,
			SubGIDStart: target.Allocation.SubGIDStart, SubGIDCount: target.Allocation.SubGIDCount,
		},
		Paths: Paths{
			SlotRoot: slotRoot, RunnerHome: home, WorkRoot: path.Join(home, "_work"),
			DataRoot:  path.Join(home, ".local/share/docker"),
			RawSocket: fmt.Sprintf("/run/user/%d/docker.sock", target.Allocation.HostUID), ProxySocket: proxySocket,
		},
		Network: Network{
			Name: "gp_runner_" + target.RunnerID, BridgeName: "g" + identityToken,
			RunnerPool: policy.RunnerPool, Subnet: prefix, Gateway: gateway, RunnerAddress: runnerAddress,
		},
		Egress: Egress{
			SourceUID:          target.Allocation.HostUID,
			DeniedCIDRs:        denied,
			ControllerEndpoint: policy.ControllerEndpoint,
		},
		Container: Container{
			Name: "gp_runner_" + target.RunnerID, ImageRef: target.ImageRef,
			User: strconv.FormatUint(
				uint64(target.Allocation.HostUID),
				10,
			) + ":" + strconv.FormatUint(
				uint64(target.Allocation.HostUID),
				10,
			),
			ReadOnlyRootFS: true, CapDrop: []string{"ALL"}, SecurityOptions: []string{"no-new-privileges=true"},
			NetworkName: "gp_runner_" + target.RunnerID, NetworkAddress: runnerAddress,
			DockerSocketSource: proxySocket, DockerSocketTarget: "/var/run/docker.sock",
			GitHubURL: target.GitHubURL, RunnerName: "gp-" + identityToken, Labels: labels,
		},
	}, nil
}

func validateTarget(target Target) error {
	if ids.Validate(ids.KindRunner, target.RunnerID) != nil || ids.Validate(ids.KindTenant, target.TenantID) != nil ||
		target.RuntimeEpoch == 0 || target.AllocationConfig.ValidateAllocation(target.Allocation) != nil {
		return errs.New(errs.KindValidationFailed, "runner runtime target is invalid")
	}
	switch target.OwnerKind {
	case OwnerTenant:
		if ids.Validate(ids.KindTenant, target.OwnerID) != nil || target.OwnerID != target.TenantID {
			return errs.New(errs.KindValidationFailed, "runner tenant ownership is invalid")
		}
	case OwnerProject:
		if ids.Validate(ids.KindProject, target.OwnerID) != nil {
			return errs.New(errs.KindValidationFailed, "runner project ownership is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "runner owner kind is invalid")
	}
	parsed, err := url.Parse(target.GitHubURL)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "runner GitHub URL is invalid")
	}
	segments := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	wantSegments := 1
	if target.OwnerKind == OwnerProject {
		wantSegments = 2
	}
	if parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.RawQuery != "" ||
		parsed.Fragment != "" || len(segments) != wantSegments || slices.Contains(segments, "") {
		return errs.New(errs.KindValidationFailed, "runner GitHub URL is invalid")
	}
	if !validDigestImage(target.ImageRef) || !validLabels(target.Labels) {
		return errs.New(errs.KindValidationFailed, "runner image or labels are invalid")
	}
	return nil
}

func validatePolicy(
	network string,
	allocationConfig runnerallocation.RunnerAllocationConfig,
	policy IsolationPolicy,
) error {
	if !policy.RunnerPool.IsValid() || !policy.RunnerPool.Addr().Is4() ||
		policy.RunnerPool != policy.RunnerPool.Masked() ||
		policy.RunnerPool.Bits() > runnerallocation.RunnerSubnetBits ||
		policy.RunnerPool != allocationConfig.RunnerPool ||
		!policy.ControllerEndpoint.IsValid() ||
		!policy.ControllerEndpoint.Addr().Is4() ||
		policy.ControllerEndpoint.Port() == 0 {
		return errs.New(errs.KindValidationFailed, "runner Controller endpoint is invalid")
	}
	runnerPrefix := netip.MustParsePrefix(network)
	if !policy.RunnerPool.Contains(runnerPrefix.Addr()) || runnerPrefix.Bits() != runnerallocation.RunnerSubnetBits ||
		len(policy.DeniedCIDRs) == 0 {
		return errs.New(errs.KindValidationFailed, "runner subnet is outside the runner pool")
	}
	controllerDenied := false
	systemDenied := false
	for index, prefix := range policy.DeniedCIDRs {
		if !prefix.IsValid() || !prefix.Addr().Is4() || prefix != prefix.Masked() ||
			(index > 0 && policy.DeniedCIDRs[index-1].Addr().Compare(prefix.Addr()) >= 0) {
			return errs.New(errs.KindValidationFailed, "runner denied network inputs are invalid")
		}
		controllerDenied = controllerDenied || prefix.Contains(policy.ControllerEndpoint.Addr())
		systemDenied = systemDenied || prefix == allocationConfig.SystemPool
	}
	if !controllerDenied || !systemDenied {
		return errs.New(errs.KindValidationFailed, "runner deny policy does not cover the Controller exception")
	}
	return nil
}

func stableToken(runnerID string) string {
	return strings.ToLower(strings.TrimPrefix(runnerID, string(ids.KindRunner)+"_"))
}

func validLabels(values []string) bool {
	if len(values) > 16 || !slices.IsSorted(values) {
		return false
	}
	for index, value := range values {
		if len(value) == 0 || len(value) > 64 ||
			(value[0] < 'a' || value[0] > 'z') && (value[0] < '0' || value[0] > '9') ||
			(index > 0 && values[index-1] == value) {
			return false
		}
		for _, character := range value {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') &&
				character != '.' && character != '_' && character != '-' {
				return false
			}
		}
		if value == "self-hosted" || value == "linux" || value == "x64" || value == "arm64" {
			return false
		}
	}
	return true
}

func ValidatePlan(plan Plan) error {
	target := Target{
		RunnerID: plan.RunnerID, TenantID: plan.TenantID, OwnerKind: OwnerKind(plan.OwnerKind), OwnerID: plan.OwnerID,
		GitHubURL: plan.Container.GitHubURL, Labels: plan.Container.Labels, ImageRef: plan.Container.ImageRef,
		RuntimeEpoch: plan.RuntimeEpoch, AllocationConfig: plan.AllocationConfig, Allocation: plan.Allocation,
	}
	expected, err := NewPlan(target, IsolationPolicy{
		RunnerPool: plan.Network.RunnerPool, DeniedCIDRs: plan.Egress.DeniedCIDRs,
		ControllerEndpoint: plan.Egress.ControllerEndpoint,
	})
	if err != nil || expected.Digest() != plan.Digest() {
		return errs.New(errs.KindValidationFailed, "runner runtime plan violates the isolation contract")
	}
	return nil
}

func validDigestImage(value string) bool {
	marker := strings.LastIndex(value, "@sha256:")
	if marker <= 0 || len(value)-marker != len("@sha256:")+64 {
		return false
	}
	for _, character := range value[marker+len("@sha256:"):] {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}
