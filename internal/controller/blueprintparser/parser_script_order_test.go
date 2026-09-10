package blueprintparser

import (
	"context"
	"fmt"
	"testing"
)

// Rationale: setup and migration order is an authored decision independent of
// a Script's renamable label, including the zero default and maximum value.
func TestParseScriptsAcceptsExplicitHookOrder(t *testing.T) {
	for _, order := range []int{0, 10, 65535} {
		t.Run(fmt.Sprint(order), func(t *testing.T) {
			body := fmt.Sprintf(`x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: pre-deploy
    order: %d
    script: |
      php artisan migrate --force
services:
  api:
    image: example/api:1
`, order)
			result, err := Parse(context.Background(), parserEnvironmentScope, parserBundle(
				[]string{"compose.yaml"}, map[string]string{"compose.yaml": environmentRoot(body)},
			))
			if err != nil {
				t.Fatalf("Parse(order=%d): %v", order, err)
			}
			if got := result.Extensions.Scripts["migration-hook"].Order; got != uint16(order) {
				t.Fatalf("parsed order = %d, want %d", got, order)
			}
		})
	}
}

// Rationale: non-integer or out-of-range YAML must not silently become zero or
// a truncated order; the desired grammar distinguishes omission from bad input.
func TestParseScriptsRejectsInvalidHookOrder(t *testing.T) {
	for _, value := range []string{"-1", "65536", "1.5", "1.0", "null", "\"10\"", "true", "[]", "{}"} {
		t.Run(value, func(t *testing.T) {
			body := fmt.Sprintf(`x-gp-scripts:
  migration-hook:
    slug: migrate
    service: api
    when: pre-deploy
    order: %s
    script: echo setup
services:
  api:
    image: example/api:1
`, value)
			_, err := Parse(context.Background(), parserEnvironmentScope, parserBundle(
				[]string{"compose.yaml"}, map[string]string{"compose.yaml": environmentRoot(body)},
			))
			if err == nil {
				t.Fatalf("Parse accepted non-integer or out-of-range order %s", value)
			}
		})
	}
}
