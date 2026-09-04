package blueprintparser

import (
	"context"
	"testing"
)

// Rationale: a Script targets one logical Service release, so an authored
// replica count must not multiply or disqualify the Script target.
func TestParseScriptsAcceptsReplicatedLogicalService(t *testing.T) {
	result, err := Parse(
		context.Background(), parserEnvironmentScope,
		parserBundle(
			[]string{"compose.yaml"},
			map[string]string{"compose.yaml": environmentRoot(`x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: post-deploy
    script: php artisan migrate --force
services:
  api:
    image: example/api:1
    deploy:
      replicas: 3
`)},
		),
	)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	service := result.Project.Services["api"]
	if len(result.Extensions.Scripts) != 1 || service.GetScale() != 3 {
		t.Fatalf("parsed replicated Script target = %#v/%d", result.Extensions.Scripts, service.GetScale())
	}
}

// Rationale: lifting the stale singleton restriction must not make a stopped
// or otherwise non-positive desired workload eligible for Script execution.
func TestParseScriptsRejectsNonPositiveReplicaTargets(t *testing.T) {
	for name, replicas := range map[string]string{"zero": "0", "negative": "-1"} {
		t.Run(name, func(t *testing.T) {
			err := parseError(parserBundle(
				[]string{"compose.yaml"},
				map[string]string{"compose.yaml": environmentRoot(`x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: manual
    script: true
services:
  api:
    image: example/api:1
    deploy:
      replicas: ` + replicas + "\n")},
			))
			if err == nil {
				t.Fatal("Parse() accepted a non-positive Script target replica count")
			}
		})
	}
}
