package runnerproxy

import (
	"bytes"
	"context"
	"net/url"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/moby/moby/api/types/build"
)

func policyContext(t *testing.T) (*BuildPolicy, *BuildContext) {
	t.Helper()
	policy, err := NewBuildPolicy(ids.New(ids.KindRunner), 3)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := PrepareBuildContext(context.Background(), privateSpool(t), bytes.NewReader(
		contextArchive(t, contextFixture{name: "Dockerfile", body: "FROM scratch\n"}),
	))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return policy, prepared
}

// RUN-04: legitimate build options survive while mandatory confinement and
// exact operation ownership are constructed independently of workflow input.
func TestBuildPolicyConstructsOwnedLocalBuild(t *testing.T) {
	policy, prepared := policyContext(t)
	operationID := ids.New(ids.KindOperation)
	query := url.Values{
		"t": {"example:qa"}, "buildargs": {`{"REVISION":"source-commit","EMPTY":""}`},
		"labels": {`{"org.example.source":"source-commit"}`}, "pull": {"1"}, "nocache": {"true"},
	}
	options, err := policy.Prepare(operationID, query.Encode(), prepared)
	if err != nil {
		t.Fatal(err)
	}
	if options.Version != build.BuilderV1 || !options.Remove || !options.ForceRemove ||
		options.NetworkMode != "default" || !options.NoCache || !options.PullParent ||
		options.RemoteContext != "" || options.SessionID != "" || len(options.SecurityOpt) != 0 ||
		len(options.Tags) != 1 || options.Tags[0] != "docker.io/library/example:qa" ||
		options.BuildArgs["REVISION"] == nil || *options.BuildArgs["REVISION"] != "source-commit" ||
		options.BuildArgs["EMPTY"] == nil || *options.BuildArgs["EMPTY"] != "" {
		t.Fatal("owned build did not preserve permitted inputs and fixed policy")
	}
	if options.Labels[runnerOwnerLabel] != policy.runnerID || options.Labels[runnerEpochLabel] != "3" ||
		options.Labels[buildOperationLabel] != operationID || options.Labels[buildInputLabel] != prepared.Digest() ||
		options.Labels["org.example.source"] != "source-commit" {
		t.Fatal("build ownership was not bound to the current operation and context")
	}
}

// RUN-04: alternate builders, remote inputs, host controls and ambiguous JSON
// cannot bypass context validation or overwrite ownership labels.
func TestBuildPolicyRejectsAuthorityOverridesAndUnsupportedExecution(t *testing.T) {
	policy, prepared := policyContext(t)
	cases := []url.Values{
		{"version": {"2"}}, {"remote": {"https://example.com/context.tar"}},
		{"networkmode": {"host"}}, {"securityopt": {"seccomp=unconfined"}},
		{"session": {"buildkit-session"}}, {"outputs": {`[{"type":"local","attrs":{"dest":"/host"}}]`}},
		{"cgroupparent": {"/system.slice"}}, {"extrahosts": {"controller:host-gateway"}},
		{"labels": {`{"groundplane.runner.id":"another-runner"}`}},
		{"labels": {`{"Groundplane.runtime.epoch":"other"}`}},
		{"buildargs": {`{"ARG":null}`}}, {"buildargs": {`{"ARG":"one","ARG":"two"}`}},
		{"dockerfile": {"missing"}}, {"dockerfile": {"../Dockerfile"}},
		{"pull": {"true", "false"}}, {"rm": {"0"}},
		{"t": {"example:qa", "docker.io/library/example:qa"}},
	}
	for index, query := range cases {
		if _, err := policy.Prepare(ids.New(ids.KindOperation), query.Encode(), prepared); err == nil {
			t.Fatalf("unsupported request %d was accepted", index)
		}
	}
}
