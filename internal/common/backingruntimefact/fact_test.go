package backingruntimefact

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// BACK-01: a supported MySQL workload must settle its provisioning report,
// without admitting mutable images or losing exact native ownership fences.
func TestNativeWorkloadAdmitsPinnedFamiliesAndRejectsChangedAuthority(t *testing.T) {
	const suffix = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, scenario := range []struct {
		name, image, labelValue string
		accepted                bool
	}{
		{"mysql", "mysql:8.4@sha256:" + strings.Repeat("a", 64), "plan_" + suffix, true},
		{"postgres", "postgres:16-alpine@sha256:" + strings.Repeat("a", 64), "plan_" + suffix, true},
		{"mutable", "mysql:8.4", "plan_" + suffix, false},
		{"unsupported-line", "mysql:9@sha256:" + strings.Repeat("a", 64), "plan_" + suffix, false},
		{"foreign-plan", "mysql:8.4@sha256:" + strings.Repeat("a", 64), "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			body := []byte("services:\n  database:\n    image: " + scenario.image + "\n")
			digest := sha256.Sum256(body)
			artifact := &agentpb.ComposeArtifact{
				OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
				OwnerId:   "env_" + suffix, CanonicalYaml: body, YamlSha256: digest[:],
				Services: []*agentpb.ComposeService{{
					ServiceId: "svc_" + suffix, ComposeName: "database", ExpectedReplicas: 1,
					ImageReference: scenario.image,
					ExpectedLabels: []*agentpb.LabelPair{
						{Key: "com.groundplane.environment-id", Value: "env_" + suffix},
						{Key: "com.groundplane.kind", Value: "service"},
						{Key: "com.groundplane.managed", Value: "true"},
						{Key: "com.groundplane.plan-id", Value: scenario.labelValue},
						{Key: "com.groundplane.project-id", Value: "prj_" + suffix},
						{Key: "com.groundplane.render-generation", Value: "1"},
						{Key: "com.groundplane.service-id", Value: "svc_" + suffix},
					},
				}},
			}
			_, err := Workload(artifact, "svc_"+suffix, "plan_"+suffix, 1)
			if (err == nil) != scenario.accepted {
				t.Fatalf("native workload admission = %v, accepted = %t", err, scenario.accepted)
			}
		})
	}
}
