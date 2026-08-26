package core

import (
	"fmt"
	"testing"
)

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
		group := ReleaseGroup{
			ID:        "rg_x",
			Name:      "realtime",
			Services:  []string{"api", "worker"},
			OnFailure: policy,
		}
		if err := group.Validate(); err != nil {
			t.Errorf("Validate() with on_failure=%q: unexpected error: %v", policy, err)
		}
	}

	group := ReleaseGroup{
		ID:        "rg_x",
		Name:      "realtime",
		Services:  []string{"api", "worker"},
		OnFailure: "continue",
	}
	if err := group.Validate(); err == nil {
		t.Fatal("Validate() with unknown on_failure: expected error")
	}
}

func TestReleaseGroupSpecValidate(t *testing.T) {
	// Rationale: the authored map key is the sole group name, while every
	// coordinated release has a unique non-blank membership, an exact optional
	// order permutation, and a closed failure policy.
	tests := []struct {
		name      string
		groupName string
		spec      ReleaseGroupSpec
		wantError bool
	}{
		{
			name:      "valid omitted order and policy",
			groupName: "Realtime group",
			spec:      ReleaseGroupSpec{Services: []string{"api", "worker"}},
		},
		{
			name:      "valid explicit permutation and policy",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services:  []string{"api", "worker"},
				Order:     []string{"worker", "api"},
				OnFailure: OnFailureLeaveActive,
			},
		},
		{
			name:      "explicit empty order",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services:     []string{"api", "worker"},
				Order:        []string{},
				orderPresent: true,
			},
			wantError: true,
		},
		{
			name:      "empty map key",
			groupName: "",
			spec:      ReleaseGroupSpec{Services: []string{"api", "worker"}},
			wantError: true,
		},
		{
			name:      "whitespace map key",
			groupName: " \t ",
			spec:      ReleaseGroupSpec{Services: []string{"api", "worker"}},
			wantError: true,
		},
		{
			name:      "invalid UTF-8 map key",
			groupName: string([]byte{0xff}),
			spec:      ReleaseGroupSpec{Services: []string{"api", "worker"}},
			wantError: true,
		},
		{
			name:      "control character map key",
			groupName: "real\ntime",
			spec:      ReleaseGroupSpec{Services: []string{"api", "worker"}},
			wantError: true,
		},
		{
			name:      "single member",
			groupName: "realtime",
			spec:      ReleaseGroupSpec{Services: []string{"api"}},
			wantError: true,
		},
		{
			name:      "more than thirty two members",
			groupName: "realtime",
			spec:      ReleaseGroupSpec{Services: makeReleaseGroupMembers(33)},
			wantError: true,
		},
		{
			name:      "blank member",
			groupName: "realtime",
			spec:      ReleaseGroupSpec{Services: []string{"api", " "}},
			wantError: true,
		},
		{
			name:      "duplicate member",
			groupName: "realtime",
			spec:      ReleaseGroupSpec{Services: []string{"api", "api"}},
			wantError: true,
		},
		{
			name:      "blank order entry",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services: []string{"api", "worker"},
				Order:    []string{"api", " "},
			},
			wantError: true,
		},
		{
			name:      "duplicate order entry",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services: []string{"api", "worker"},
				Order:    []string{"api", "api"},
			},
			wantError: true,
		},
		{
			name:      "non-member order entry",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services: []string{"api", "worker"},
				Order:    []string{"api", "scheduler"},
			},
			wantError: true,
		},
		{
			name:      "omitted order member",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services: []string{"api", "worker"},
				Order:    []string{"api"},
			},
			wantError: true,
		},
		{
			name:      "unknown policy",
			groupName: "realtime",
			spec: ReleaseGroupSpec{
				Services:  []string{"api", "worker"},
				OnFailure: "continue",
			},
			wantError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.spec.Validate(test.groupName)
			if test.wantError && err == nil {
				t.Fatal("Validate() error = nil, want validation failure")
			}
			if !test.wantError && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func makeReleaseGroupMembers(count int) []string {
	members := make([]string, count)
	for index := range members {
		members[index] = fmt.Sprintf("service-%02d", index)
	}
	return members
}
