package composehelper

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestStartupDependencyClosure(t *testing.T) {
	for _, test := range []struct {
		name, document string
		dependencies   bool
		want           []string
		invalid        bool
	}{
		{"transitive", "services:\n  app:\n    depends_on:\n      database:\n        condition: service_healthy\n  database:\n    depends_on:\n      cache:\n        condition: service_started\n  cache: {}\n  unrelated: {}\n", true, []string{"app", "database", "cache"}, false},
		{"no-deps", "services:\n  app:\n    depends_on:\n      database:\n        condition: service_started\n  database: {}\n  cache: {}\n  unrelated: {}\n", false, []string{"app"}, false},
		{"short-form-rejected", "services:\n  app:\n    depends_on: [database]\n  database: {}\n  cache: {}\n  unrelated: {}\n", true, nil, true},
		{"missing-mapping", "services:\n  app: {}\n", true, nil, true},
		{"missing-dependency", "services:\n  app:\n    depends_on:\n      missing:\n        condition: service_started\n  database: {}\n  cache: {}\n  unrelated: {}\n", true, nil, true},
		{"malformed", "services: [", true, nil, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			artifact := &agentpb.ComposeArtifact{CanonicalYaml: []byte(test.document)}
			for _, name := range []string{"app", "database", "cache", "unrelated"} {
				artifact.Services = append(
					artifact.Services,
					&agentpb.ComposeService{ComposeName: name, ExpectedReplicas: 1},
				)
			}
			services, err := startupClosure(artifact, []string{"app"}, test.dependencies)
			if test.invalid {
				if err == nil {
					t.Fatal("accepted invalid dependency authority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, service := range services {
				names = append(names, service.ComposeName)
			}
			if !reflect.DeepEqual(names, test.want) {
				t.Fatalf("selected=%v want=%v", names, test.want)
			}
			services[0].ComposeName = "changed"
			if artifact.Services[0].ComposeName != "app" {
				t.Fatal("selector returned mutable artifact authority")
			}
		})
	}
}
