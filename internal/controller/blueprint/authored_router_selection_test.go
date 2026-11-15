package blueprint

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

// A request-policy edit must select router work without selecting application
// releases after the router's managed-file baseline has been acknowledged.
func TestAuthoredRouterPolicyDoesNotSelectServiceReleases(t *testing.T) {
	document := func(template string) string {
		return authoredServiceDocument("") + "x-gp-components:\n  http-router:\n" +
			"    implementation: caddy\n    enabled: false\n" +
			"    implementation_config:\n      caddyfile_template: '" + template + "'\n"
	}
	before := planAuthoredServiceFixture(t, document("{gp.routes}"), nil)
	after := planAuthoredServiceFixture(t, document("# changed policy\n        {gp.routes}"), nil)
	selection := selectAuthoredServiceFixture(t, before, after)
	var router blueprintunits.ResourceKey
	var routerUnit blueprintunits.Unit
	for _, unit := range after.units {
		if unit.Target.Kind == ids.KindComponent {
			router = unit.Target
			routerUnit = unit
		}
	}
	if router.ID == "" || !containsAuthoredFixtureTarget(selection.Ready, router) {
		t.Fatalf("changed router was not selected: %#v", selection)
	}
	// Services wait for the router's managed-file baseline, then acknowledge
	// their existing runtime instead of requesting another release.
	selection = selectAuthoredServiceFixture(t, before, after, routerUnit)
	for name, target := range after.services {
		if !containsAuthoredFixtureTarget(selection.Satisfied, target) {
			t.Fatalf("router policy selected Service %s for execution: %#v", name, selection)
		}
	}
}

// Skipping matched Service work must not hide a changed managed environment
// input, or redeploy an unrelated Service that cannot consume that Entry.
func TestAuthoredServiceEntryChangeSelectsOnlyItsConsumer(t *testing.T) {
	document := func(value string) string {
		return authoredServiceEnvelope + "services:\n  api: {image: registry.example/api:v1}\n" +
			"  worker: {image: registry.example/worker:v1}\n" +
			"x-gp-entry:\n  LOG_LEVEL:\n    kind: env\n    source: {literal: " + value + "}\n" +
			"    exposure: [worker]\n    secret: false\n"
	}
	before := planAuthoredServiceFixture(t, document("info"), nil)
	after := planAuthoredServiceFixture(t, document("debug"), nil)
	var entry blueprintunits.Unit
	for _, unit := range after.units {
		if unit.Target.Kind == ids.KindEnvEntry {
			entry = unit
		}
	}
	if entry.Target.ID == "" {
		t.Fatal("Entry was not planned")
	}
	selection := selectAuthoredServiceFixture(t, before, after, entry)
	if !containsAuthoredFixtureTarget(selection.Ready, after.services["worker"]) ||
		!containsAuthoredFixtureTarget(selection.Satisfied, after.services["api"]) {
		t.Fatalf("changed Entry consumer selection: %#v", selection)
	}
}
