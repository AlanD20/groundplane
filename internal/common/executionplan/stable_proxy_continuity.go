package executionplan

import (
	"slices"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Target-only traffic switches retain one proxy process. Bind both artifacts
// to that process and listener set before executing any candidate workload.
// Recreate does not call this owner: it removes the captured process and has
// separate exact-artifact restoration authority.
func validateStableProxyContinuity(candidate, previous *agentpb.ComposeService) error {
	if candidate == nil || previous == nil {
		return errs.New(errs.KindValidationFailed, "traffic switching requires both captured stable proxies")
	}
	if candidate.GetServiceId() != previous.GetServiceId() ||
		candidate.GetComposeName() != previous.GetComposeName() ||
		candidate.GetProxyImplementation() != previous.GetProxyImplementation() ||
		candidate.GetImageReference() != previous.GetImageReference() {
		return errs.New(
			errs.KindValidationFailed,
			"traffic switching cannot replace the captured proxy identity, implementation or image; use an explicit recreate deployment",
		)
	}
	next, err := OpenProxyConfiguration(candidate)
	if err != nil {
		return err
	}
	prior, err := OpenProxyConfiguration(previous)
	if err != nil {
		return err
	}
	if !slices.Equal(next.Ports, prior.Ports) {
		return errs.New(
			errs.KindValidationFailed,
			"traffic switching cannot replace the captured proxy port/protocol pairs; use an explicit recreate deployment",
		)
	}
	return nil
}
