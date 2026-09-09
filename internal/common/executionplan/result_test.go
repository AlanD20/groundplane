package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestUsesEnvironmentDirectoryResultRequiresExclusiveDirectoryPlan(t *testing.T) {
	directory := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
		EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{},
	}}
	compose := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ComposeRemove{
		ComposeRemove: &agentpb.ComposeRemove{},
	}}
	volumePath := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
		ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{},
	}}
	volumeDocker := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ManagedVolumeRemove{
		ManagedVolumeRemove: &agentpb.ManagedVolumeRemove{},
	}}
	apply := &agentpb.ExecutionStep{Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{}}}
	for name, test := range map[string]struct {
		plan *agentpb.ExecutionPlan
		want bool
	}{
		"directory only":      {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{directory}}, want: true},
		"composite":           {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{compose, directory}}},
		"compose only":        {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{compose}}},
		"empty":               {plan: &agentpb.ExecutionPlan{}},
		"nil":                 {},
		"volume removal":      {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{volumeDocker, volumePath}}, want: true},
		"volume detach":       {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{apply, volumeDocker, volumePath}}, want: true},
		"unrelated composite": {plan: &agentpb.ExecutionPlan{Steps: []*agentpb.ExecutionStep{compose, volumePath}}},
	} {
		t.Run(name, func(t *testing.T) {
			if got := UsesEnvironmentDirectoryResult(test.plan); got != test.want {
				t.Fatalf("UsesEnvironmentDirectoryResult() = %t, want %t", got, test.want)
			}
		})
	}
}
