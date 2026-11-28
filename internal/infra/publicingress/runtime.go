package publicingress

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/runner"
)

// NewDockerStore connects a provisioned Host edge to its activation journal.
// After the first Provision, Bootstrap the captured initial configuration.
// Existing installations resume using their previously captured identity.
// The caller supplies a fence for the prior reload attempt and independently
// captured route checks for each receipt. Only an admitted activation should
// call Apply or Recover.
func NewDockerStore(
	taskRunner runner.Runner,
	identity DockerIdentity,
	quiescent func(context.Context, string) error,
	lookup func(context.Context, Receipt) (ProbePlan, error),
) (*Store, error) {
	owner, err := NewDockerOwner(taskRunner, identity, quiescent)
	if err != nil {
		return nil, err
	}
	probe, err := NewRouteProbe(identity, lookup)
	if err != nil {
		return nil, err
	}
	runtime, err := NewCaddy(taskRunner, owner, probe)
	if err != nil {
		return nil, err
	}
	return New(identity.StateDirectory, runtime)
}
