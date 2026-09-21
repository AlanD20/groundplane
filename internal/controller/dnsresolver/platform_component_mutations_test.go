package dnsresolver

import (
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testplatformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// Rationale: desired lifecycle and config mutations must retain the last
// Controller-owned runtime projection until acknowledgement advances it.
func TestPlatformComponentMutationCandidatesPreserveHealthyRuntime(t *testing.T) {
	records, err := testplatformcomponents.DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	record, err := testcomponents.SetRuntime(records[0], []string{ids.New(ids.KindService)}, "", true)
	if err != nil {
		t.Fatalf("SetComponentRuntime() error = %v", err)
	}
	current, err := testcomponents.ProjectRecord(record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord() error = %v", err)
	}

	disabled, ensureService, disableService, err := platformComponentLifecycleCandidate(current, "disable")
	if err != nil {
		t.Fatalf("platformComponentLifecycleCandidate(disable) error = %v", err)
	}
	if disabled.Enabled || ensureService || !disableService {
		t.Fatalf("disable candidate = %#v, ensure=%t disable=%t", disabled, ensureService, disableService)
	}
	assertPlatformRuntimePreserved(t, current, disabled)
	if _, err := testcomponents.ReplaceDesired(record, disabled); err != nil {
		t.Fatalf("ReplaceComponentDesired(disable) error = %v", err)
	}

	config := *current.Config.CoreDNS
	config.TailnetDelegation = true
	configured := platformComponentConfigCandidate(current, config)
	assertPlatformRuntimePreserved(t, current, configured)
	if _, err := testcomponents.ReplaceDesired(record, configured); err != nil {
		t.Fatalf("ReplaceComponentDesired(config) error = %v", err)
	}
}

func assertPlatformRuntimePreserved(t *testing.T, current core.Component, candidate core.Component) {
	t.Helper()
	if !slices.Equal(candidate.GeneratedServices, current.GeneratedServices) ||
		(candidate.GeneratedServices == nil) != (current.GeneratedServices == nil) ||
		candidate.PinnedIPv4 != current.PinnedIPv4 || candidate.Healthy != current.Healthy {
		t.Fatalf(
			"runtime = services %#v, address %q, healthy %t; want services %#v, address %q, healthy %t",
			candidate.GeneratedServices,
			candidate.PinnedIPv4,
			candidate.Healthy,
			current.GeneratedServices,
			current.PinnedIPv4,
			current.Healthy,
		)
	}
}

// Rationale: the strict typed DNS-resolver config must preserve canonical
// resolver ordering and address/port identity before durable publication.
func TestCoreDNSConfigMutationCanonicalizesResolvers(t *testing.T) {
	upstreamAuto := false
	tailnetDelegation := true
	corefileTemplate := testCorefileTemplate
	upstreamResolvers := []string{"1.1.1.1:853", "8.8.8.8"}
	forwarders := []apiTypes.ComponentDNSForwarder{{
		Domain: "home.arpa", Resolvers: []string{"192.168.1.1:5353", "192.168.1.2"},
	}}
	config, err := coreDNSConfigMutation(apiTypes.ComponentConfigMutationInput{
		CoreDNS: &apiTypes.CoreDNSComponentConfigMutationInput{
			CorefileTemplate: &corefileTemplate,
			UpstreamAuto:     &upstreamAuto, UpstreamResolvers: &upstreamResolvers,
			Forwarders: &forwarders, TailnetDelegation: &tailnetDelegation,
		},
	})
	if err != nil {
		t.Fatalf("coreDNSConfigMutation() error = %v", err)
	}
	if config.UpstreamResolvers[0].Address != "1.1.1.1" || config.UpstreamResolvers[0].Port != 853 ||
		config.UpstreamResolvers[1].Address != "8.8.8.8" || config.Forwarders[0].Resolvers[0].Port != 5353 {
		t.Fatalf("coreDNSConfigMutation() = %+v", config)
	}
}

// Rationale: no flattened or partial compatibility shape may bypass the
// closed nullable Component config union.
func TestCoreDNSConfigMutationRejectsPartialConfig(t *testing.T) {
	upstreamAuto := true
	if _, err := coreDNSConfigMutation(apiTypes.ComponentConfigMutationInput{
		CoreDNS: &apiTypes.CoreDNSComponentConfigMutationInput{UpstreamAuto: &upstreamAuto},
	}); err == nil {
		t.Fatal("coreDNSConfigMutation() accepted a partial config")
	}
}
