package imageref

import (
	"strings"
	"testing"
)

func TestIsDigestPinnedAcceptsOnlyExactSHA256OCIIdentity(t *testing.T) {
	// Rationale: the Controller and Docker adapter must agree on one immutable
	// image identity and never silently accept a mutable tag or malformed digest.
	t.Parallel()

	sha256Digest := strings.Repeat("a", 64)
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "canonical", value: "ghcr.io/aland20/groundplane-agent@sha256:" + sha256Digest, want: true},
		{name: "empty", value: ""},
		{name: "tag only", value: "ghcr.io/aland20/groundplane-agent:latest"},
		{name: "tag and digest", value: "ghcr.io/aland20/groundplane-agent:v1@sha256:" + sha256Digest},
		{name: "short digest", value: "ghcr.io/aland20/groundplane-agent@sha256:" + strings.Repeat("a", 63)},
		{name: "non hex digest", value: "ghcr.io/aland20/groundplane-agent@sha256:" + strings.Repeat("z", 64)},
		{name: "wrong algorithm", value: "ghcr.io/aland20/groundplane-agent@sha512:" + strings.Repeat("a", 128)},
		{name: "leading whitespace", value: " ghcr.io/aland20/groundplane-agent@sha256:" + sha256Digest},
		{name: "digest without name", value: "sha256:" + sha256Digest},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := IsDigestPinned(test.value); got != test.want {
				t.Errorf("IsDigestPinned(%q) = %v, want %v", test.value, got, test.want)
			}
		})
	}
}
