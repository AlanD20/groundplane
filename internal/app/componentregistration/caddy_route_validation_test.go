package componentregistration

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// A Route omitted from the authored set must also be removed from a custom
// template. Validate this before Apply can publish resource definitions.
func TestBlueprintCaddyTemplateRejectsRemovedRouteBeforeExecution(t *testing.T) {
	catalog, err := EnvironmentCatalog()
	if err != nil {
		t.Fatal(err)
	}
	root := core.RouteSpec{
		Hostname:   "api.example.test",
		Path:       "/v1",
		Target:     "api",
		TargetPort: 8080,
		Exposure:   "public",
	}
	wildcard := root
	wildcard.Path = "/v1/*"
	template := "{gp.routes}\n# {gp.route:api.example.test:/v1:upstream}\n"
	for _, test := range []struct {
		name     string
		routes   []core.RouteSpec
		template string
		invalid  bool
	}{
		{"declared", []core.RouteSpec{root, wildcard}, template, false},
		{"dangling_reference", []core.RouteSpec{wildcard}, template, true},
		{"removed_together", []core.RouteSpec{wildcard}, "{gp.routes}\n", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed := blueprintparser.Result{Extensions: blueprintparser.Extensions{
				Routes: test.routes,
				Components: map[string]core.ComponentSpec{"http-router": {
					Implementation: core.ComponentKindIngressCaddy, Enabled: true,
					ImplementationConfig: core.ComponentImplementationConfig{CaddyfileTemplate: test.template},
				}},
			}}
			err := taskplanning.ValidateBlueprintRouterConfiguration(parsed, catalog)
			if !test.invalid {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !errors.Is(err, errs.New(errs.KindRouterConfigurationInvalid, "")) {
				t.Fatalf("dangling template was not rejected as permanent configuration: %v", err)
			}
			var domain *errs.Error
			if !errors.As(err, &domain) || !strings.Contains(domain.ToProblem().Detail, "template") ||
				!strings.Contains(domain.Error(), "undeclared Route") || domain.ToProblem().Status != 422 {
				t.Fatalf("missing actionable public guidance or private cause: %v", err)
			}
		})
	}
}
