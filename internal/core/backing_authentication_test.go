package core

import "testing"

// Rationale: every selectable mode is an explicit decision, while adapters
// without that capability cannot acquire an authentication policy accidentally.
func TestResolveBackingAuthentication(t *testing.T) {
	for _, tc := range []struct {
		supported       bool
		input, expected BackingAuthentication
		invalid         bool
	}{
		{true, "", "", true},
		{true, " ", "", true},
		{true, BackingAuthenticationUsernamePassword, BackingAuthenticationUsernamePassword, false},
		{true, BackingAuthenticationPassword, BackingAuthenticationPassword, false},
		{true, BackingAuthenticationNone, BackingAuthenticationNone, false},
		{true, "unknown", "", true},
		{false, "", "", false},
		{false, BackingAuthenticationNone, "", true},
		{false, BackingAuthenticationPassword, "", true},
	} {
		actual, err := ResolveBackingAuthentication(tc.supported, tc.input)
		if (err != nil) != tc.invalid || actual != tc.expected {
			t.Fatalf("mode %q supported=%v: %q, %v", tc.input, tc.supported, actual, err)
		}
	}
}
