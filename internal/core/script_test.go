package core

import "testing"

func TestScriptValidateAcceptsEveryLockedHook(t *testing.T) {
	// Rationale: every product-locked hook must remain accepted without silently widening the enum.
	for hook := range scriptHooks {
		script := Script{
			ID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV", Slug: "migrate", ServiceName: "api",
			Body: "php artisan migrate --force", When: hook,
		}
		if err := script.Validate(); err != nil {
			t.Fatalf("Validate() hook %q: %v", hook, err)
		}
	}
}

func TestScriptValidateRejectsUnknownHook(t *testing.T) {
	// Rationale: accepting an unknown hook would create desired state that no deploy lifecycle can execute.
	script := Script{
		ID: "scr_01ARZ3NDEKTSV4RRFFQ69G5FAV", Slug: "migrate", ServiceName: "api",
		Body: "php artisan migrate --force", When: ScriptHook("scheduled"),
	}
	if err := script.Validate(); err == nil {
		t.Fatal("Validate() accepted an unknown hook")
	}
}
