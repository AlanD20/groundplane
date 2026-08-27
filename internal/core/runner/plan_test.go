package runner

import (
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
)

// Rationale: the runtime plan is the last pure boundary before privileged host
// control and must derive every isolation-sensitive name, path, and socket.
func TestNewPlanBuildsDedicatedRootlessRuntime(t *testing.T) {
	t.Parallel()
	runnerID := ids.NewAt(ids.KindRunner, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 1)
	tenantID := ids.NewAt(ids.KindTenant, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 2)
	projectID := ids.NewAt(ids.KindProject, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 3)
	plan, err := NewPlan(Target{
		RunnerID: runnerID, TenantID: tenantID, OwnerKind: OwnerProject, OwnerID: projectID,
		GitHubURL: "https://github.com/acme/storefront", Labels: []string{"qa-workload", "zeta"},
		ImageRef: "ghcr.io/aland20/groundplane-runner@sha256:" + repeatHex("a"), RuntimeEpoch: 4,
		AllocationConfig: testPlanAllocationConfig(),
		Allocation: runnerallocation.RunnerHostAllocationRecord{
			Slot: 3, HostUID: 200003, SubUIDStart: 496608, SubUIDCount: 65536,
			SubGIDStart: 1096608, SubGIDCount: 65536, NetworkCIDR: "10.240.0.24/29",
		},
	}, IsolationPolicy{
		RunnerPool:         netip.MustParsePrefix("10.240.0.0/16"),
		DeniedCIDRs:        []netip.Prefix{netip.MustParsePrefix("10.128.0.0/9")},
		ControllerEndpoint: netip.MustParseAddrPort("10.130.0.2:8443"),
	})
	if err != nil {
		t.Fatalf("NewPlan() error = %v", err)
	}
	if plan.Paths.SlotRoot != "/var/lib/groundplane/runner-slots/slot-03" ||
		plan.Paths.DataRoot != "/var/lib/groundplane/runner-slots/slot-03/runner/.local/share/docker" ||
		plan.Paths.ProxySocket != "/run/user/200003/groundplane-docker-proxy/docker.sock" {
		t.Fatalf("paths = %#v", plan.Paths)
	}
	if plan.Network.Subnet.String() != "10.240.0.24/29" ||
		plan.Network.Gateway.String() != "10.240.0.25" || plan.Network.RunnerAddress.String() != "10.240.0.26" {
		t.Fatalf("network = %#v", plan.Network)
	}
	if plan.Identity.User != plan.Identity.Group || plan.Identity.UID != 200003 ||
		plan.Identity.SubUIDStart != 496608 || plan.Identity.SubGIDStart != 1096608 {
		t.Fatalf("identity = %#v", plan.Identity)
	}
	container := plan.Container
	if container.User != "200003:200003" || container.Privileged || !container.ReadOnlyRootFS ||
		container.NetworkName != plan.Network.Name || container.NetworkAddress != plan.Network.RunnerAddress ||
		!slices.Equal(container.CapDrop, []string{"ALL"}) || len(container.CapAdd) != 0 ||
		container.DockerSocketSource != plan.Paths.ProxySocket || container.DockerSocketTarget != "/var/run/docker.sock" ||
		container.DockerSocketSource == "/var/run/docker.sock" {
		t.Fatalf("container = %#v", container)
	}
	wantRunnerName := "gp-" + strings.ToLower(strings.TrimPrefix(runnerID, "run_"))
	if container.RunnerName != wantRunnerName || len(strings.TrimPrefix(container.RunnerName, "gp-")) != 26 {
		t.Fatalf("runner name = %q, want %q", container.RunnerName, wantRunnerName)
	}
	if !slices.Equal(plan.Egress.DeniedCIDRs, []netip.Prefix{
		netip.MustParsePrefix("10.128.0.0/9"),
	}) || plan.Egress.SourceUID != 200003 || plan.Egress.ControllerEndpoint.String() != "10.130.0.2:8443" {
		t.Fatalf("egress = %#v", plan.Egress)
	}
}

// Rationale: a mismatched owner or Runner pool would turn a dedicated runtime
// into a cross-owner or cross-pool network capability.
func TestNewPlanRejectsCrossOwnerAndNetworkReuseInputs(t *testing.T) {
	t.Parallel()
	target := validTarget(t)
	target.OwnerKind = OwnerTenant
	target.OwnerID = ids.NewAt(ids.KindTenant, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC), 50)
	if _, err := NewPlan(target, validPolicy()); err == nil {
		t.Fatal("NewPlan(cross-tenant owner) error = nil")
	}
	target = validTarget(t)
	policy := validPolicy()
	policy.RunnerPool = netip.MustParsePrefix("10.241.0.0/16")
	if _, err := NewPlan(target, policy); err == nil {
		t.Fatal("NewPlan(runner subnet in denied pool) error = nil")
	}
}

// Rationale: malformed URL escapes must be rejected at the boundary instead
// of being normalized into a different external registration target.
func TestNewPlanRejectsMalformedGitHubTargetWithoutPanicking(t *testing.T) {
	t.Parallel()
	target := validTarget(t)
	target.GitHubURL = "https://github.com/acme/%zz"
	if _, err := NewPlan(target, validPolicy()); err == nil {
		t.Fatal("NewPlan(malformed GitHub target) error = nil")
	}
}

func validTarget(t *testing.T) Target {
	t.Helper()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 10)
	return Target{
		RunnerID: ids.NewAt(ids.KindRunner, now, 11), TenantID: tenantID,
		OwnerKind: OwnerTenant, OwnerID: tenantID, GitHubURL: "https://github.com/acme",
		ImageRef: "ghcr.io/aland20/groundplane-runner@sha256:" + repeatHex("b"), RuntimeEpoch: 1,
		AllocationConfig: testPlanAllocationConfig(),
		Allocation: runnerallocation.RunnerHostAllocationRecord{
			Slot: 0, HostUID: 200000, SubUIDStart: 300000, SubUIDCount: 65536,
			SubGIDStart: 900000, SubGIDCount: 65536, NetworkCIDR: "10.240.0.0/29",
		},
	}
}

func validPolicy() IsolationPolicy {
	return IsolationPolicy{
		RunnerPool:         netip.MustParsePrefix("10.240.0.0/16"),
		DeniedCIDRs:        []netip.Prefix{netip.MustParsePrefix("10.128.0.0/9")},
		ControllerEndpoint: netip.MustParseAddrPort("10.130.0.2:8443"),
	}
}

func testPlanAllocationConfig() runnerallocation.RunnerAllocationConfig {
	return runnerallocation.RunnerAllocationConfig{
		SystemPool: netip.MustParsePrefix("10.128.0.0/9"),
		RunnerPool: netip.MustParsePrefix("10.240.0.0/16"),
		HostPool: runnerallocation.RunnerHostPoolConfig{
			HostUIDStart: 200000, HostUIDEnd: 200007,
			SubUIDStart: 300000, SubUIDEnd: 824287,
			SubGIDStart: 900000, SubGIDEnd: 1424287,
		},
	}
}

func repeatHex(value string) string {
	result := ""
	for len(result) < 64 {
		result += value
	}
	return result
}
