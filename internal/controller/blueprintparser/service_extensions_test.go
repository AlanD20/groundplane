package blueprintparser

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: one strict parser must preserve Controller-only release and
// phase decisions while compiling ordinary startup dependencies to Compose.
func TestParseNormalizesTypedServiceExtensions(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(`services:
  migrate: {image: migrate}
  api:
    image: api
    x-gp-release: {default_strategy: blue-green}
    x-gp-depends_on:
      migrate: {condition: service_completed_successfully}
`),
	})
	result, err := Parse(context.Background(), parserEnvironmentScope, bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	spec := result.ServiceExtensions["api"]
	if spec.Release == nil || spec.Release.DefaultStrategy != core.StrategyBlueGreen ||
		spec.Release.OnFailure != core.OnFailureSwitchBack {
		t.Fatalf("Service release extension = %#v", spec.Release)
	}
	dependency, exists := result.Project.Services["api"].DependsOn["migrate"]
	if !exists || dependency.Condition != "service_completed_successfully" || !dependency.Required {
		t.Fatalf("compiled Compose dependency = %#v", dependency)
	}
}

// Rationale: a deploy-only edge must remain typed desired state and must not
// accidentally block ordinary Compose reconciliation startup.
func TestParseKeepsLifecycleDependencyOutOfComposeStartup(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(`services:
  migrate: {image: migrate}
  api:
    image: api
    x-gp-depends_on:
      migrate: {condition: service_completed_successfully, phases: [deploy]}
`),
	})
	result, err := Parse(context.Background(), parserEnvironmentScope, bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if _, exists := result.Project.Services["api"].DependsOn["migrate"]; exists {
		t.Fatal("deploy-only dependency leaked into Compose startup")
	}
	if got := result.ServiceExtensions["api"].DependsOn["migrate"].Phases; !slices.Equal(
		got,
		[]core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
	) {
		t.Fatalf("dependency phases = %v, want [deploy]", got)
	}
}

// Rationale: unknown generated release fields, duplicate native/managed
// definitions, and phase-local cycles must fail before desired mutation.
func TestParseRejectsInvalidServiceExtensionBoundaries(t *testing.T) {
	tests := map[string]string{
		"generated release state": `services:
  api: {image: api, x-gp-release: {default_strategy: recreate, current: {tag: bad}}}
`,
		"duplicate native dependency": `services:
  worker: {image: worker}
  api:
    image: api
    depends_on: [worker]
    x-gp-depends_on: {worker: {condition: service_started}}
`,
		"deploy cycle": `services:
  api:
    image: api
    x-gp-depends_on: {worker: {condition: service_started, phases: [deploy]}}
  worker:
    image: worker
    x-gp-depends_on: {api: {condition: service_started, phases: [deploy]}}
`,
		"missing target": `services:
  api:
    image: api
    x-gp-depends_on: {worker: {condition: service_started}}
`,
		"invalid condition": `services:
  api:
    image: api
    x-gp-depends_on: {worker: {condition: ready}}
  worker: {image: worker}
`,
		"invalid phase": `services:
  api:
    image: api
    x-gp-depends_on: {worker: {condition: service_started, phases: [restart]}}
  worker: {image: worker}
`,
		"wrong service scope": `services:
  api:
    image: api
    x-gp-backup: {enabled: false}
`,
		"always expands into deploy cycle": `services:
  api:
    image: api
    x-gp-depends_on: {worker: {condition: service_started, phases: [always]}}
  worker:
    image: worker
    x-gp-depends_on: {api: {condition: service_started, phases: [deploy]}}
`,
		"mixed native and managed compose cycle": `services:
  api:
    image: api
    depends_on: [worker]
  worker:
    image: worker
    x-gp-depends_on: {api: {condition: service_started}}
`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			bundle := parserBundle([]string{"root.yaml"}, map[string]string{
				"root.yaml": environmentRoot(body),
			})
			requireValidationError(t, parseError(bundle))
		})
	}
}

// Rationale: every submitted source is an authored boundary. A later override
// cannot erase forbidden or malformed state from an earlier source.
func TestParseValidatesSupersededServiceExtensions(t *testing.T) {
	tests := map[string]string{
		"generated release state": environmentRoot(`services:
  api:
    image: api:old
    x-gp-release:
      default_strategy: recreate
      current: {tag: forged}
`),
		"wrong-scope extension": environmentRoot(`services:
  api:
    image: api:old
    x-gp-backup: {enabled: false}
`),
		"invalid dependency variant": environmentRoot(`services:
  worker: {image: worker}
  api:
    image: api:old
    x-gp-depends_on: {worker: {condition: ready}}
`),
	}
	for name, root := range tests {
		t.Run(name, func(t *testing.T) {
			bundle := parserBundle([]string{"root.yaml", "override.yaml"}, map[string]string{
				"root.yaml": root,
				"override.yaml": `services:
  api: !override
    image: api:new
    x-gp-release: {default_strategy: recreate}
`,
			})
			requireValidationError(t, parseError(bundle))
		})
	}
}

// Rationale: always remains one typed authored decision while cycle analysis
// expands it across each lifecycle phase.
func TestParseRetainsTypedAlwaysDependencyPhase(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml"}, map[string]string{
		"root.yaml": environmentRoot(`services:
  worker: {image: worker}
  api:
    image: api
    x-gp-depends_on:
      worker: {condition: service_started, phases: [always]}
`),
	})
	result, err := Parse(context.Background(), parserEnvironmentScope, bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	dependency := result.ServiceExtensions["api"].DependsOn["worker"]
	if dependency.Condition != core.ServiceDependencyStarted || !slices.Equal(
		dependency.Phases,
		[]core.ServiceDependencyPhase{core.ServiceDependencyPhaseAlways},
	) {
		t.Fatalf("always dependency = %#v", dependency)
	}
}

func TestParseMergesStructurallyValidPartialServiceExtensions(t *testing.T) {
	bundle := parserBundle([]string{"root.yaml", "override.yaml"}, map[string]string{
		"root.yaml": environmentRoot(`services:
  migrate: {image: migrate}
  api:
    image: api:old
    x-gp-release: {default_strategy: recreate}
    x-gp-depends_on:
      migrate: {condition: service_completed_successfully}
`),
		"override.yaml": `services:
  api:
    image: api:new
    x-gp-release: {on_failure: leave_active}
    x-gp-depends_on:
      migrate: {phases: [deploy]}
`,
	})
	result, err := Parse(context.Background(), parserEnvironmentScope, bundle)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	spec := result.ServiceExtensions["api"]
	dependency := spec.DependsOn["migrate"]
	if spec.Release == nil || spec.Release.DefaultStrategy != core.StrategyRecreate ||
		spec.Release.OnFailure != core.OnFailureLeaveActive ||
		dependency.Condition != core.ServiceDependencyCompletedSuccessfully ||
		!slices.Equal(dependency.Phases, []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy}) {
		t.Fatalf("merged Service extensions = %#v", spec)
	}
}

func TestParseRejectsSourceLocalServiceExtensionDefects(t *testing.T) {
	tests := map[string]struct {
		root string
		want string
	}{
		"explicit null phases": {
			root: environmentRoot(`services:
  worker: {image: worker}
  api:
    image: api
    x-gp-depends_on: {worker: {condition: service_started, phases: null}}
`),
			want: `source "root.yaml": Service dependency "worker" phases must not be null`,
		},
		"environment adapter": {
			root: environmentRoot(`services:
  api: {image: api, x-gp-adapter: {key: manual}}
`),
			want: `source "root.yaml": x-gp-adapter is allowed only on the sole Service of a backing Blueprint`,
		},
		"superseded generated release field": {
			root: environmentRoot(`services:
  api:
    image: api
    x-gp-release: {default_strategy: recreate, current: {tag: forged}}
`),
			want: `source "root.yaml": Service x-gp-release field "current"`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			bundle := parserBundle([]string{"root.yaml", "override.yaml"}, map[string]string{
				"root.yaml": test.root,
				"override.yaml": `services:
  api: !override {image: api:new, x-gp-release: {default_strategy: recreate}}
`,
			})
			_, err := Parse(context.Background(), parserEnvironmentScope, bundle)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Parse() error = %v, want reason containing %q", err, test.want)
			}
		})
	}
}
