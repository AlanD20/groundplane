package runner

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
)

// Rationale: the Docker adapter must pass only the exact validated rootless
// socket, unprivileged user, private network, dropped capabilities, and ordered
// cleanup requests to privileged host control.
func TestRuntimeAppliesExactIsolationInputsAndCleanupOrder(t *testing.T) {
	t.Parallel()
	host := &recordingHost{}
	runtime, err := New(host)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	plan := validRuntimePlan(t)
	for _, step := range corerunner.CreationSteps() {
		token := []byte(nil)
		if step == corerunner.StepStartRunner {
			token = []byte("transient")
		}
		evidence, applyErr := applyCapability(context.Background(), runtime, plan, step, token)
		if applyErr != nil || !evidence.Valid(runnerallocation.RuntimeOperationCreate, step) {
			t.Fatalf("Apply(%s) = %#v, %v", step, evidence, applyErr)
		}
	}
	for _, step := range corerunner.RemovalSteps() {
		evidence, applyErr := applyCapability(context.Background(), runtime, plan, step, nil)
		if applyErr != nil || !evidence.Valid(runnerallocation.RuntimeOperationRemove, step) {
			t.Fatalf("Apply(%s) = %#v, %v", step, evidence, applyErr)
		}
	}
	if !slices.Equal(host.steps, append(corerunner.CreationSteps(), corerunner.RemovalSteps()...)) {
		t.Fatalf("steps = %v", host.steps)
	}
	if host.container.DockerSocketSource == "/var/run/docker.sock" || host.container.Privileged ||
		!host.container.ReadOnlyRootFS || !slices.Equal(host.container.CapDrop, []string{"ALL"}) ||
		host.container.User == "0:0" || host.container.NetworkName == "" {
		t.Fatalf("container = %#v", host.container)
	}
	if string(host.token) != "transient" {
		t.Fatalf("registration token = %q", host.token)
	}
}

// Rationale: a caller cannot bypass the isolation constructor by changing the
// socket, user, data root, network mode, privileges, or capabilities in-place.
func TestRuntimeRejectsTamperedIsolationPlanBeforeHostControl(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*corerunner.Plan)
	}{
		{name: "host socket", mutate: func(plan *corerunner.Plan) {
			plan.Container.DockerSocketSource = "/var/run/docker.sock"
		}},
		{name: "root user", mutate: func(plan *corerunner.Plan) { plan.Container.User = "0:0" }},
		{name: "host network", mutate: func(plan *corerunner.Plan) { plan.Container.NetworkName = "" }},
		{name: "privileged", mutate: func(plan *corerunner.Plan) { plan.Container.Privileged = true }},
		{name: "capability", mutate: func(plan *corerunner.Plan) { plan.Container.CapAdd = []string{"NET_ADMIN"} }},
		{name: "data root", mutate: func(plan *corerunner.Plan) { plan.Paths.DataRoot = "/var/lib/docker" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := validRuntimePlan(t)
			test.mutate(&plan)
			host := &recordingHost{}
			runtime, err := New(host)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.EnsureIdentity(context.Background(), plan); err == nil {
				t.Fatal("Apply(tampered plan) error = nil")
			}
			if len(host.steps) != 0 {
				t.Fatalf("host steps = %v", host.steps)
			}
		})
	}
}

type recordingHost struct {
	steps     []corerunner.Step
	container corerunner.Container
	token     []byte
}

func (host *recordingHost) EnsureIdentity(context.Context, corerunner.Plan) error {
	host.steps = append(host.steps, corerunner.StepEnsureIdentity)
	return nil
}
func (host *recordingHost) EnsureNetwork(context.Context, corerunner.Plan) error {
	host.steps = append(host.steps, corerunner.StepEnsureNetwork)
	return nil
}
func (host *recordingHost) EnsureEgress(context.Context, corerunner.Plan) error {
	host.steps = append(host.steps, corerunner.StepEnsureEgress)
	return nil
}
func (host *recordingHost) StartProxy(context.Context, corerunner.Plan) error {
	host.steps = append(host.steps, corerunner.StepStartProxy)
	return nil
}
func (host *recordingHost) StartDaemon(context.Context, corerunner.Plan) error {
	host.steps = append(host.steps, corerunner.StepStartDaemon)
	return nil
}
func (host *recordingHost) StartRunner(
	_ context.Context,
	plan corerunner.Plan,
	token []byte,
) (runnerallocation.RunnerRuntimeEvidence, error) {
	host.steps = append(host.steps, corerunner.StepStartRunner)
	host.container = plan.Container
	host.token = append([]byte(nil), token...)
	return runnerallocation.RunnerRuntimeEvidence{
		ContainerID: repeatValue("a", 64), DaemonNonce: repeatValue("b", 64), SocketDevice: 1, SocketInode: 2,
	}, nil
}
func (host *recordingHost) StopRunner(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepStopRunner)
	return cleanupReceipt(), nil
}
func (host *recordingHost) StopDaemon(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepStopDaemon)
	return cleanupReceipt(), nil
}
func (host *recordingHost) StopProxy(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepStopProxy)
	return cleanupReceipt(), nil
}
func (host *recordingHost) RemoveNetwork(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepRemoveNetwork)
	return cleanupReceipt(), nil
}
func (host *recordingHost) RemoveEgress(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepRemoveEgress)
	return cleanupReceipt(), nil
}
func (host *recordingHost) RemoveIdentity(context.Context, corerunner.Plan) (string, error) {
	host.steps = append(host.steps, corerunner.StepRemoveIdentity)
	return cleanupReceipt(), nil
}
func observed(step corerunner.Step, removal bool) (corerunner.StepEvidence, error) {
	if removal {
		return corerunner.StepEvidence{Step: step, State: corerunner.EffectAbsent, ReceiptSHA256: cleanupReceipt()}, nil
	}
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}, nil
}
func (host *recordingHost) ObserveIdentity(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepEnsureIdentity, false)
}
func (host *recordingHost) ObserveNetwork(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepEnsureNetwork, false)
}
func (host *recordingHost) ObserveEgress(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepEnsureEgress, false)
}
func (host *recordingHost) ObserveProxy(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepStartProxy, false)
}
func (host *recordingHost) ObserveDaemon(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepStartDaemon, false)
}
func (host *recordingHost) ObserveRunner(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	ownership := runnerallocation.RunnerRuntimeEvidence{ContainerID: repeatValue("a", 64), DaemonNonce: repeatValue("b", 64), SocketDevice: 1, SocketInode: 2}
	return corerunner.StepEvidence{Step: corerunner.StepStartRunner, State: corerunner.EffectApplied, Ownership: &ownership}, nil
}
func (host *recordingHost) ObserveRunnerAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepStopRunner, true)
}
func (host *recordingHost) ObserveDaemonAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepStopDaemon, true)
}
func (host *recordingHost) ObserveProxyAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepStopProxy, true)
}
func (host *recordingHost) ObserveNetworkAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepRemoveNetwork, true)
}
func (host *recordingHost) ObserveIdentityAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepRemoveIdentity, true)
}
func (host *recordingHost) ObserveEgressAbsent(context.Context, corerunner.Plan) (corerunner.StepEvidence, error) {
	return observed(corerunner.StepRemoveEgress, true)
}

func applyCapability(
	ctx context.Context,
	runtime *HostControl,
	plan corerunner.Plan,
	step corerunner.Step,
	token []byte,
) (corerunner.StepEvidence, error) {
	switch step {
	case corerunner.StepEnsureIdentity:
		return runtime.EnsureIdentity(ctx, plan)
	case corerunner.StepEnsureNetwork:
		return runtime.EnsureNetwork(ctx, plan)
	case corerunner.StepEnsureEgress:
		return runtime.EnsureEgress(ctx, plan)
	case corerunner.StepStartProxy:
		return runtime.StartProxy(ctx, plan)
	case corerunner.StepStartDaemon:
		return runtime.StartDaemon(ctx, plan)
	case corerunner.StepStartRunner:
		return runtime.StartRunner(ctx, plan, token)
	case corerunner.StepStopRunner:
		return runtime.StopRunner(ctx, plan)
	case corerunner.StepStopDaemon:
		return runtime.StopDaemon(ctx, plan)
	case corerunner.StepStopProxy:
		return runtime.StopProxy(ctx, plan)
	case corerunner.StepRemoveNetwork:
		return runtime.RemoveNetwork(ctx, plan)
	case corerunner.StepRemoveIdentity:
		return runtime.RemoveIdentity(ctx, plan)
	case corerunner.StepRemoveEgress:
		return runtime.RemoveEgress(ctx, plan)
	default:
		return corerunner.StepEvidence{}, nil
	}
}

func validRuntimePlan(t *testing.T) corerunner.Plan {
	t.Helper()
	now := time.Date(2026, time.August, 26, 12, 0, 0, 0, time.UTC)
	runnerID := ids.NewAt(ids.KindRunner, now, 1)
	tenantID := ids.NewAt(ids.KindTenant, now, 2)
	plan, err := corerunner.NewPlan(corerunner.Target{
		RunnerID: runnerID, TenantID: tenantID, OwnerKind: corerunner.OwnerTenant, OwnerID: tenantID,
		GitHubURL: "https://github.com/acme", Labels: []string{"qa-workload"},
		ImageRef: "ghcr.io/aland20/groundplane-runner@sha256:" + repeatValue("a", 64), RuntimeEpoch: 1,
		AllocationConfig: runnerallocation.RunnerAllocationConfig{
			SystemPool: netip.MustParsePrefix("10.128.0.0/9"),
			RunnerPool: netip.MustParsePrefix("10.240.0.0/16"),
			HostPool: runnerallocation.RunnerHostPoolConfig{
				HostUIDStart: 200000, HostUIDEnd: 200007,
				SubUIDStart: 300000, SubUIDEnd: 824287,
				SubGIDStart: 900000, SubGIDEnd: 1424287,
			},
		},
		Allocation: runnerallocation.RunnerHostAllocationRecord{
			Slot: 0, HostUID: 200000, SubUIDStart: 300000, SubUIDCount: 65536,
			SubGIDStart: 900000, SubGIDCount: 65536, NetworkCIDR: "10.240.0.0/29",
		},
	}, corerunner.IsolationPolicy{
		RunnerPool:         netip.MustParsePrefix("10.240.0.0/16"),
		DeniedCIDRs:        []netip.Prefix{netip.MustParsePrefix("10.128.0.0/9")},
		ControllerEndpoint: netip.MustParseAddrPort("10.130.0.2:8443"),
	})
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func isRemovalRuntimeStep(step corerunner.Step) bool {
	for _, candidate := range corerunner.RemovalSteps() {
		if candidate == step {
			return true
		}
	}
	return false
}

func cleanupReceipt() string { return "sha256:" + repeatValue("c", 64) }

func repeatValue(value string, count int) string {
	result := ""
	for len(result) < count {
		result += value
	}
	return result
}
