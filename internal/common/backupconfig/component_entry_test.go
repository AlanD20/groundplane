package backupconfig

import "testing"

// A configuration archive must retain the Component-only grant and reject any
// attempt to combine it with application Service IDs or public/plain values.
func TestTunnelEntryArchiveRetainsRestrictedScope(t *testing.T) {
	entry := literalEnvironmentEntry([]byte("token"))
	entry.Secret = true
	entry.Encryption = "none"
	entry.Exposure = Exposure{Kind: ExposureEdgeTunnel}
	payload, _, err := buildManifestPayload(t.Context(), []Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := parseManifest(t.Context(), payload)
	if err != nil || len(decoded) != 1 || decoded[0].Exposure.Kind != ExposureEdgeTunnel || !decoded[0].Secret ||
		decoded[0].Encryption != "none" {
		t.Fatalf("restricted archive lost scope: %v", err)
	}
	entry.Exposure.ServiceIDs = []string{"svc_00000000000000000000000000"}
	if ValidateEntry(entry) == nil {
		t.Fatal("archive accepted mixed Component/Service exposure")
	}
	entry.Exposure.ServiceIDs = nil
	entry.Secret = false
	if ValidateEntry(entry) == nil {
		t.Fatal("archive accepted unprotected Component token")
	}
}
