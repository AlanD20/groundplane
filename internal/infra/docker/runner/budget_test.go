package runner

import "testing"

// Rationale: systemd reports human-readable time values; misreading them would
// accept an incorrect CPU quota or reject a correctly bounded Runner.
func TestObservedCPUQuotaParsing(t *testing.T) {
	t.Parallel()
	for value, expected := range map[string]int64{"500ms": 500, "2s": 2000, "1s 500ms": 1500} {
		actual, err := parseSystemdQuota(value)
		if err != nil || actual != expected {
			t.Fatalf("quota %q = %d, %v", value, actual, err)
		}
	}
	for _, value := range []string{"infinity", "", "0", "10us"} {
		if _, err := parseSystemdQuota(value); err == nil {
			t.Fatalf("accepted unavailable or unsupported quota %q", value)
		}
	}
}
