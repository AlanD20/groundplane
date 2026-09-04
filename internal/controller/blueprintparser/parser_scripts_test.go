package blueprintparser

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestParseScriptsReturnsTypedClosedExtension(t *testing.T) {
	result, err := Parse(
		context.Background(), parserEnvironmentScope,
		parserBundle(
			[]string{"compose.yaml"},
			map[string]string{"compose.yaml": environmentRoot(`x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: pre-deploy
    script: |
      php artisan migrate --force
services:
  api:
    image: example/api:1
    deploy:
      replicas: 1
`)}),
	)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	script, exists := result.Extensions.Scripts["migration-hook"]
	if !exists || script.Slug != "migrate" || script.Service != "api" ||
		script.When != core.ScriptPreDeploy || script.Script != "php artisan migrate --force\n" {
		t.Fatalf("parsed Scripts = %#v", result.Extensions.Scripts)
	}
}

func TestParseScriptsRejectsClosedContractViolations(t *testing.T) {
	cases := map[string]string{
		"unknown field": `x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: manual
    script: true
    timeout: 30
services:
  api:
    image: example/api:1
`,
		"unknown service": `x-gp-scripts:
  migration-hook:
    slug: migrate
    service: missing
    when: manual
    script: true
services:
  api:
    image: example/api:1
`,
		"legacy task extension": `x-gp-task:
  migration-hook:
    script: true
services:
  api:
    image: example/api:1
`,
		"unknown namespace extension": `x-gp-unknown:
  value: true
services:
  api:
    image: example/api:1
`,
		"blank body": `x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: manual
    script: "   "
services:
  api:
    image: example/api:1
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if err := parseError(parserBundle(
				[]string{"compose.yaml"},
				map[string]string{"compose.yaml": environmentRoot(strings.TrimSpace(body) + "\n")},
			)); err == nil {
				t.Fatalf("Parse() accepted invalid Script contract")
			}
		})
	}
}
