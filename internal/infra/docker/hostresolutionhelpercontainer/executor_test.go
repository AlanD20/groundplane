package hostresolutionhelpercontainer

import (
	"slices"
	"testing"
)

func TestCreateOptionsGrantsOnlyResolverWriteCapability(t *testing.T) {
	t.Parallel()

	options := createOptions(
		"registry.example/groundplane-agent@sha256:"+
			"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		OperationApply,
	)
	if options.HostConfig == nil {
		t.Fatal("createOptions() omitted HostConfig")
	}
	if !slices.Equal([]string(options.HostConfig.CapDrop), []string{"ALL"}) {
		t.Fatalf("CapDrop = %v, want [ALL]", options.HostConfig.CapDrop)
	}
	if !slices.Equal([]string(options.HostConfig.CapAdd), []string{"DAC_OVERRIDE"}) {
		t.Fatalf("CapAdd = %v, want [DAC_OVERRIDE]", options.HostConfig.CapAdd)
	}
	if !options.HostConfig.ReadonlyRootfs || options.HostConfig.NetworkMode != "none" ||
		!slices.Equal(options.HostConfig.SecurityOpt, []string{"no-new-privileges"}) {
		t.Fatalf("helper isolation changed: %#v", options.HostConfig)
	}
}
