package valkey9

import (
	"bytes"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
)

// Rationale: clients need the actual named identity as well as its password.
func TestNamedAuthenticationExposesRole(t *testing.T) {
	facts, err := adapters.BuildFacts(&adapter{}, adapters.FactParams{
		Host: "valkey", Port: "6379", Role: "api_5d3f9a", Password: []byte("URL_safe-1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapters.ClearFacts(facts)
	for _, fact := range facts {
		if fact.Key == "valkey9_ROLE" && string(fact.Value) == "api_5d3f9a" {
			return
		}
	}
	t.Fatal("named authentication does not expose its username")
}

// Rationale: Valkey ACL credentials must be sent as mutable stdin to the
// compiled binary, never exposed in Docker exec arguments.
func TestProvisionStepsKeepPasswordOutOfArguments(t *testing.T) {
	steps := (&adapter{}).ProvisionSteps(adapters.ProvisionParams{
		Role: "api_5d3f9a", Password: []byte("URL_safe-1"),
	})
	if len(steps) != 2 || steps[0].Program != "valkey-cli" ||
		!slices.Equal(
			steps[0].Args,
			[]string{
				"--user",
				"groundplane",
				"-e",
				"-x",
				"ACL",
				"SETUSER",
				"api_5d3f9a",
				"on",
				"~*",
				"&*",
				"+@all",
				"-@admin",
			},
		) ||
		!bytes.Contains(steps[0].Stdin, []byte(">URL_safe-1")) {
		t.Fatalf("ProvisionSteps() = %#v", steps)
	}
}

// Rationale: auth choice determines both connection syntax and secret metadata.
func TestAuthenticationFactModes(t *testing.T) {
	for _, tc := range []struct {
		mode                core.BackingAuthentication
		role, password, url string
		secret              bool
		count               int
	}{
		{core.BackingAuthenticationUsernamePassword, "api_5d3f9a", "safe-1", "redis://api_5d3f9a:safe-1@valkey:6379", true, 5},
		{core.BackingAuthenticationPassword, "default", "safe-1", "redis://:safe-1@valkey:6379", true, 4},
		{core.BackingAuthenticationNone, "", "", "redis://valkey:6379", false, 3},
	} {
		t.Run(string(tc.mode), func(t *testing.T) {
			facts, err := adapters.BuildFacts(
				&adapter{},
				adapters.FactParams{
					Authentication: tc.mode,
					Host:           "valkey",
					Port:           "6379",
					Role:           tc.role,
					Password:       []byte(tc.password),
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			defer adapters.ClearFacts(facts)
			if len(facts) != tc.count || string(facts[0].Value) != tc.url || facts[0].Secret != tc.secret {
				t.Fatal("wrong mode fact shape")
			}
		})
	}
}

// Rationale: default-user attachments must never revoke another owner's access.
func TestPasswordDetachRevokesOnlyOwnerPassword(t *testing.T) {
	steps := (&adapter{}).DetachSteps(
		adapters.ProvisionParams{
			Authentication: core.BackingAuthenticationPassword,
			Role:           "default",
			Password:       []byte("owner-1"),
		},
	)
	defer adapters.ClearSteps(steps)
	if len(steps) != 2 || string(steps[0].Stdin) != "<owner-1" ||
		!slices.Equal(steps[0].Args, []string{"--user", "groundplane", "-e", "-x", "ACL", "SETUSER", "default"}) ||
		!slices.Equal(steps[1].Args, []string{"--user", "groundplane", "-e", "ACL", "SAVE"}) {
		t.Fatal("detach must remove only owner password then persist")
	}
}

// Rationale: joining an unauthenticated backing must not change instance policy.
func TestNoAuthenticationNeverCompilesCredentialMutation(t *testing.T) {
	params := adapters.ProvisionParams{Authentication: core.BackingAuthenticationNone}
	if len((&adapter{}).ProvisionSteps(params)) != 0 || len((&adapter{}).DetachSteps(params)) != 0 {
		t.Fatal("no-auth attachment mutated instance authentication")
	}
	_, err := adapters.BuildFacts(
		&adapter{},
		adapters.FactParams{
			Authentication: core.BackingAuthenticationNone,
			Host:           "valkey",
			Port:           "6379",
			Password:       []byte("unexpected"),
		},
	)
	if err == nil {
		t.Fatal("no-auth accepted a credential")
	}
}
