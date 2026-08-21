package agentcontainer

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
)

func TestCreateOptionsInjectsExactManagedAgentImage(t *testing.T) {
	desired := Desired{
		Image:      "ghcr.io/aland20/groundplane-agent@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		AgentID:    "agt_01J00000000000000000000000",
		Generation: "7",
	}
	options := createOptions(desired)
	if options.Config == nil {
		t.Fatal("createOptions() Config = nil")
	}
	want := agentprotocol.AgentImageEnv + "=" + desired.Image
	if len(options.Config.Env) != 1 || options.Config.Env[0] != want {
		t.Fatalf("createOptions() Env = %v, want [%q]", options.Config.Env, want)
	}
}
