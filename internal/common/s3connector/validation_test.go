package s3connector

import (
	"strings"
	"testing"
)

// Rationale: CRUD and execution must accept one byte-exact authority language;
// otherwise a durable Connector can become unusable only when a Backup runs.
func TestC15AuthorityValidationIsClosedAtTransportBounds(t *testing.T) {
	validEndpoint := "https://" + strings.Repeat("a", EndpointMaxBytes-len("https://"))
	for name, valid := range map[string]bool{
		"endpoint maximum":       ValidEndpoint(validEndpoint),
		"endpoint over maximum":  ValidEndpoint(validEndpoint + "a"),
		"endpoint path":          ValidEndpoint("https://objects.example.test/private"),
		"bucket canonical":       ValidBucket("groundplane-backups"),
		"bucket numeric address": ValidBucket("999.999.999.999"),
		"bucket reserved prefix": ValidBucket("xn--groundplane"),
		"prefix maximum":         ValidPrefix(strings.Repeat("a", PrefixMaxBytes-1) + "/"),
		"prefix over maximum":    ValidPrefix(strings.Repeat("a", PrefixMaxBytes) + "/"),
		"region maximum":         ValidRegion(strings.Repeat("r", RegionMaxBytes)),
		"region over maximum":    ValidRegion(strings.Repeat("r", RegionMaxBytes+1)),
		"region whitespace":      ValidRegion("eu west 1"),
	} {
		want := strings.Contains(name, "maximum") && !strings.Contains(name, "over") ||
			name == "bucket canonical"
		if valid != want {
			t.Errorf("%s validity = %t, want %t", name, valid, want)
		}
	}
}
