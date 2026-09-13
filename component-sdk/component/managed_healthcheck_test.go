package component

import "testing"

// QA: CMP-04; pure managed-healthcheck validation and plan-digest proof only, not container readiness.
// Rationale: readiness argv and timing bounds are sealed planner authority;
// cloned plans must not alias them or omit any health behavior from replay identity.
func TestManagedHealthcheckCloneDigestAndBounds(t *testing.T) {
	plan := EnvironmentPlan{
		Services: []ManagedService{
			{
				ID: "service",
				Healthcheck: &ManagedHealthcheck{
					Command:            []string{"ready", "--local"},
					IntervalSeconds:    5,
					TimeoutSeconds:     3,
					StartPeriodSeconds: 10,
					Retries:            3,
				},
			},
		},
	}
	if err := plan.Services[0].Healthcheck.Validate(); err != nil {
		t.Fatal(err)
	}
	baseline := DigestEnvironmentPlan(plan)
	mutations := map[string]func(*ManagedHealthcheck){
		"command":  func(h *ManagedHealthcheck) { h.Command[0] = "different" },
		"interval": func(h *ManagedHealthcheck) { h.IntervalSeconds++ },
		"timeout":  func(h *ManagedHealthcheck) { h.TimeoutSeconds++ },
		"start":    func(h *ManagedHealthcheck) { h.StartPeriodSeconds++ },
		"retries":  func(h *ManagedHealthcheck) { h.Retries++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			cloned := CloneEnvironmentPlan(plan)
			mutate(cloned.Services[0].Healthcheck)
			if DigestEnvironmentPlan(cloned) == baseline {
				t.Fatal("health behavior absent from digest")
			}
			if DigestEnvironmentPlan(plan) != baseline {
				t.Fatal("clone aliases health authority")
			}
		})
	}
	absent := CloneEnvironmentPlan(plan)
	absent.Services[0].Healthcheck = nil
	if DigestEnvironmentPlan(absent) == baseline {
		t.Fatal("health presence absent from digest")
	}
	invalid := map[string]func(*ManagedHealthcheck){
		"no command":     func(h *ManagedHealthcheck) { h.Command = nil },
		"empty argument": func(h *ManagedHealthcheck) { h.Command[0] = "" },
		"nul":            func(h *ManagedHealthcheck) { h.Command[0] = "bad\x00arg" },
		"newline":        func(h *ManagedHealthcheck) { h.Command[0] = "bad\narg" },
		"many arguments": func(h *ManagedHealthcheck) { h.Command = make([]string, 33) },
		"zero interval":  func(h *ManagedHealthcheck) { h.IntervalSeconds = 0 },
		"large interval": func(h *ManagedHealthcheck) { h.IntervalSeconds = 3601 },
		"zero timeout":   func(h *ManagedHealthcheck) { h.TimeoutSeconds = 0 },
		"large timeout":  func(h *ManagedHealthcheck) { h.TimeoutSeconds = 301 },
		"large start":    func(h *ManagedHealthcheck) { h.StartPeriodSeconds = 3601 },
		"zero retries":   func(h *ManagedHealthcheck) { h.Retries = 0 },
		"large retries":  func(h *ManagedHealthcheck) { h.Retries = 101 },
	}
	for name, mutate := range invalid {
		t.Run(name, func(t *testing.T) {
			cloned := CloneEnvironmentPlan(plan)
			mutate(cloned.Services[0].Healthcheck)
			if cloned.Services[0].Healthcheck.Validate() == nil {
				t.Fatal("invalid healthcheck accepted")
			}
		})
	}
}
