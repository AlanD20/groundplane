package scriptpolicy

import "testing"

// Rationale: the authored identity is lossless and canonical, with no inherited
// user fallback or numeric normalization at the runner trust boundary.
func TestNumericUserPreservesExactUIDAndGID(t *testing.T) {
	for _, test := range []struct {
		value    string
		uid, gid uint32
	}{
		{"0:0", 0, 0}, {"1000:1001", 1000, 1001},
		{"4294967295:4294967295", 4294967295, 4294967295},
	} {
		uid, gid, err := NumericUser(test.value)
		if err != nil || uid != test.uid || gid != test.gid {
			t.Fatalf("NumericUser(%q) = %d:%d, %v", test.value, uid, gid, err)
		}
	}
}

// Rationale: target collisions include ancestors and descendants but not a
// similarly named neighboring path; both directions enforce the same policy.
func TestPathsOverlapUsesWholeComponents(t *testing.T) {
	for _, test := range []struct {
		left, right string
		want        bool
	}{
		{"/data", "/data", true}, {"/data", "/data/child", true},
		{"/", "/data", true}, {"/data", "/data-other", false},
		{"/data/one", "/data/two", false},
	} {
		if PathsOverlap(test.left, test.right) != test.want || PathsOverlap(test.right, test.left) != test.want {
			t.Fatalf("overlap(%q, %q) != %v", test.left, test.right, test.want)
		}
	}
}

// Rationale: path bytes cross the desired and machine boundaries unchanged;
// controls and invalid UTF-8 cannot become a valid target through normalization.
func TestMountTargetRejectsInvalidText(t *testing.T) {
	for _, target := range []string{"/data\n", "/data\tchild", "/data\x00", "/data\x7f", "/data\xff"} {
		if err := ValidateMountTarget(target); err == nil {
			t.Fatalf("invalid path %q accepted", target)
		}
	}
}
