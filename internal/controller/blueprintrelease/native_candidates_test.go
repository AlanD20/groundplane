package blueprintrelease

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

// Native Compose state must participate in release selection even where the
// public core.Service projection intentionally is not the lossless renderer.
func TestNativeComposeChangesSelectRunningBlueprintCandidates(t *testing.T) {
	for _, test := range []struct {
		name, fields string
		selected     bool
	}{
		{"unchanged", "", false},
		{"environment", "    environment: {QA_PROOF_REVISION: postgres-attach}\n", true},
		{"healthcheck", "    healthcheck: {test: [CMD, php, -v]}\n", true},
		{"resources", "    mem_limit: 128m\n", true},
		{"mount", "    volumes: [scratch:/proof]\n", true},
		{"entrypoint", "    entrypoint: [/bin/sh, -ec]\n", true},
		{"logging", "    logging: {driver: json-file, options: {max-size: 10m}}\n", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			environmentID := ids.New(ids.KindEnvironment)
			parse := func(fields string) blueprintparser.Result {
				document := "kind: environment\nschema: 1\nmetadata: {tenant: qa, project: proof, environment: production}\nx-gp-network-pool: 10.95.0.0/16\nservices:\n  worker:\n    image: example.invalid/worker:fixture\n    command: [sleep, infinity]\n    deploy: {replicas: 2}\n" + fields + "volumes: {scratch: {}}\n"
				parsed, err := blueprintparser.Parse(
					context.Background(),
					blueprintparser.EnvironmentScope{
						EnvironmentID: environmentID,
						Tenant:        "qa",
						Project:       "proof",
						Environment:   "production",
					},
					core.BlueprintBundle{
						RootPath:       "groundplane.yaml",
						ComposeSources: []string{"groundplane.yaml"},
						Files:          []core.BlueprintFile{{Path: "groundplane.yaml", Content: []byte(document)}},
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				return parsed
			}
			before, after := parse(""), parse(test.fields)
			identities, err := controller.ReconcileOwnedComposeIdentities(
				before.Project,
				controller.ComposeIdentitySnapshot{},
				ids.New,
			)
			if err != nil {
				t.Fatal(err)
			}
			desired, err := controller.ProjectServiceProjection(
				before.Project,
				identities.Current,
				before.ServiceExtensions,
			)
			if err != nil {
				t.Fatal(err)
			}
			record, err := etcd.NewServiceRecord(environmentID, desired[0], "")
			if err != nil {
				t.Fatal(err)
			}
			next, err := controller.ProjectServiceProjection(after.Project, identities.Current, after.ServiceExtensions)
			if err != nil {
				t.Fatal(err)
			}
			normalized, err := controller.MarshalNormalizedEnvironmentProject(before.Project)
			if err != nil {
				t.Fatal(err)
			}
			prior, err := controller.LoadNormalizedEnvironmentProject(
				context.Background(),
				etcd.EnvironmentComposeProjection{EnvironmentID: environmentID, NormalizedCompose: normalized},
			)
			if err != nil {
				t.Fatal(err)
			}
			memberships, err := BuildNormalizedServiceMemberships(prior, after.Project)
			if err != nil {
				t.Fatal(err)
			}
			for _, intent := range []core.ServiceRuntimeIntent{core.ServiceRuntimeIntentRunning, core.ServiceRuntimeIntentStopped, core.ServiceRuntimeIntentAbsent} {
				for _, grouped := range []bool{false, true} {
					current := record
					current.Runtime.RuntimeIntent = intent
					changes, err := PrepareServiceChanges(
						environmentID,
						next,
						[]etcd.Versioned[etcd.ServiceRecord]{{Record: current, Revision: 7, ReadRevision: 9}},
					)
					if err != nil {
						t.Fatal(err)
					}
					groups := map[string]struct{}{}
					if grouped {
						groups[current.Desired.ID] = struct{}{}
					}
					selected, err := selectCandidates(etcd.EnvironmentComposeProjection{}, changes, groups, memberships)
					if err != nil {
						t.Fatal(err)
					}
					want := test.selected && intent == core.ServiceRuntimeIntentRunning && !grouped
					if (len(selected) == 1) != want {
						t.Fatalf(
							"intent=%s grouped=%t: candidates=%d want selected=%t",
							intent,
							grouped,
							len(selected),
							want,
						)
					}
				}
			}
		})
	}
}
