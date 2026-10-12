package composehelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// An interrupted scaled restoration can leave a smaller prior routing set.
// Accept only canonical bytes differing from the sealed prior in that subset;
// actual active and restart observation is still required before any deletion.
func proveScaledRestorationBridge(ctx context.Context, taskRunner runner.Runner,
	containerID string, proxy *agentpb.ComposeService, plan rollingrelease.Plan,
) error {
	implementation, err := executionplan.OpenProxyImplementation(proxy.GetProxyImplementation())
	if err != nil {
		return err
	}
	manifest, err := readRestorationRouting(ctx, taskRunner, containerID, implementation)
	if err != nil {
		return err
	}
	observed, err := serviceproxy.Read(implementation, manifest)
	if err != nil || len(observed.Upstreams) == 0 {
		return errs.New(errs.KindStateConflict, "scaled restoration routing bridge is invalid")
	}
	prior, err := executionplan.OpenProxyConfiguration(proxy)
	if err != nil {
		return err
	}
	for _, name := range observed.Upstreams {
		if !slices.ContainsFunc(
			plan.Prior,
			func(replica rollingrelease.Replica) bool { return replica.ContainerName == name },
		) {
			return foreignRestorationRuntime()
		}
	}
	prior.Upstreams = slices.Clone(observed.Upstreams)
	expected, err := serviceproxy.Render(prior)
	if err != nil || !bytes.Equal(expected, manifest) {
		return foreignRestorationRuntime()
	}
	digest := sha256.Sum256(expected)
	confirmed, err := observeRestorationProxy(ctx, taskRunner, containerID, proxy.GetProxyImplementation(), digest[:])
	if err != nil {
		return err
	}
	if !confirmed {
		failure, err := activateCapturedProxy(ctx, taskRunner, containerID, proxy.GetProxyImplementation(), expected)
		if err != nil || failure != nil {
			return errs.New(errs.KindRequestFailed, "scaled restoration routing bridge repair failed")
		}
		confirmed, err = awaitRestorationProxy(ctx, taskRunner, containerID, proxy.GetProxyImplementation(), digest[:])
		if err != nil || !confirmed {
			return errs.New(errs.KindStateConflict, "scaled restoration routing bridge was not confirmed")
		}
	}
	if !confirmed {
		return errs.New(errs.KindStateConflict, "scaled restoration routing bridge was not confirmed")
	}
	return nil
}

func readRestorationRouting(ctx context.Context, taskRunner runner.Runner, containerID string,
	implementation serviceproxy.Implementation,
) ([]byte, error) {
	args := []string{"exec", containerID, "wget", "--quiet", "--output-document=-", "http://127.0.0.1:2019/config/"}
	if implementation == serviceproxy.Envoy {
		args = []string{"exec", containerID, "cat", serviceproxy.EnvoyRoutingPath}
	}
	result, err := taskRunner.Run(ctx, runner.RunCmdOpts{Name: DockerExecutable, Args: args,
		Dir: WorkDirectory, Env: slices.Clone(fixedEnvironment), ReplaceEnv: true,
		CaptureLimitBytes: serviceproxy.MaximumConfigurationBytes})
	if err != nil || result.ExitCode != 0 {
		return nil, errs.New(errs.KindRequestFailed, "restoration routing could not be read")
	}
	return canonicalProxyJSON(result.Stdout)
}

// Activation can be interrupted between restart-file publication and live
// reload. Repair only a recognized captured snapshot, then prove both views;
// arbitrary observed bytes never become execution authority.
func repairRestorationPosition(ctx context.Context, taskRunner runner.Runner,
	containerID string, proxy *agentpb.ComposeService, member *agentpb.CandidateReleaseMember,
) (int, error) {
	implementation, err := executionplan.OpenProxyImplementation(proxy.GetProxyImplementation())
	if err != nil {
		return -1, err
	}
	observed, err := readRestorationRouting(ctx, taskRunner, containerID, implementation)
	if err != nil {
		return -1, err
	}
	for position := len(member.GetRolling().GetChanges()); position >= 0; position-- {
		manifest := proxy.GetProxyConfigJson()
		digest := proxy.GetProxyConfigSha256()
		if position > 0 {
			change := member.GetRolling().GetChanges()[position-1]
			manifest, digest = change.GetProxyConfigJson(), change.GetProxyConfigSha256()
		}
		if !proxyBytesMatch(observed, digest) {
			continue
		}
		failure, err := activateCapturedProxy(ctx, taskRunner, containerID, proxy.GetProxyImplementation(), manifest)
		if err != nil || failure != nil {
			return -1, errs.New(errs.KindRequestFailed, "captured restoration routing repair failed")
		}
		confirmed, err := awaitRestorationProxy(ctx, taskRunner, containerID, proxy.GetProxyImplementation(), digest)
		if err != nil || !confirmed {
			return -1, errs.New(errs.KindStateConflict, "captured restoration routing repair was not confirmed")
		}
		return position, nil
	}
	return -1, nil
}
