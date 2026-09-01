package release

import "testing"

func TestFailureHookTargetReleaseID(t *testing.T) {
	tests := []struct {
		name   string
		intent Intent
		want   string
	}{
		{
			name:   "leave active selects candidate",
			intent: Intent{ID: "candidate", PriorServingReleaseID: "prior", OnFailure: OnFailureLeaveActive},
			want:   "candidate",
		},
		{
			name:   "switch back selects predecessor",
			intent: Intent{ID: "candidate", PriorServingReleaseID: "prior", OnFailure: OnFailureSwitchBack},
			want:   "prior",
		},
		{
			name:   "initial switch back retains candidate authority",
			intent: Intent{ID: "candidate", OnFailure: OnFailureSwitchBack},
			want:   "candidate",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := FailureHookTargetReleaseID(test.intent); got != test.want {
				t.Fatalf("FailureHookTargetReleaseID() = %q, want %q", got, test.want)
			}
		})
	}
}
