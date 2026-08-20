package core

import "testing"

func TestOnFailureWithDefault(t *testing.T) {
	// Rationale: every omitted service or group policy must resolve to the
	// single accepted switch_back default before execution.
	tests := []struct {
		name string
		in   OnFailure
		want OnFailure
	}{
		{name: "omitted", want: OnFailureSwitchBack},
		{name: "switch back", in: OnFailureSwitchBack, want: OnFailureSwitchBack},
		{name: "leave active", in: OnFailureLeaveActive, want: OnFailureLeaveActive},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.WithDefault(); got != tt.want {
				t.Fatalf("WithDefault() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReleaseGroupValidateOnFailure(t *testing.T) {
	// Rationale: persistence and execution may trust a validated group to carry
	// only the two accepted policies or the well-defined omitted value.
	for _, policy := range []OnFailure{"", OnFailureSwitchBack, OnFailureLeaveActive} {
		group := ReleaseGroup{ID: "rg_x", Name: "realtime", Services: []string{"api", "worker"}, OnFailure: policy}
		if err := group.Validate(); err != nil {
			t.Errorf("Validate() with on_failure=%q: unexpected error: %v", policy, err)
		}
	}

	group := ReleaseGroup{ID: "rg_x", Name: "realtime", Services: []string{"api", "worker"}, OnFailure: "continue"}
	if err := group.Validate(); err == nil {
		t.Fatal("Validate() with unknown on_failure: expected error")
	}
}
