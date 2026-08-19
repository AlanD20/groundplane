// Package docker is the Agent-side driver that applies rendered compose
// projects and materializes files (env files at 0600, resolv.conf). See
// mvp.md, "Everything host-level is actioned by the Agent (locked)".
//
// DOCUMENTED os/exec EXCEPTION (docs/standards.md, section 5):
// this package is one of the three listed exceptions ("the docker
// driver inside internal/infra") allowed to bridge to the docker SDK or
// shell out to the `docker` CLI directly, rather than going through
// internal/common/runner.Runner — container lifecycle here is the
// Applier interface's own concern, not a generic adapter Step.
package docker

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ComposeProject is a rendered docker-compose.yml plus the project name
// the Agent applies it under (e.g. "groundplane-infra", or
// "<tenant>-<project>-<env>" for tenant stacks).
type ComposeProject struct {
	Name    string
	YAML    []byte
	EnvFile string // path to the deterministic per-environment env file, if any
}

// Applier is the narrow interface the Agent's worker pool depends on —
// swapping the docker API for a different backend never ripples past
// this package.
type Applier interface {
	// Up applies the project (compose up -d --remove-orphans), returning
	// observed container state for the reconciliation feed.
	Up(ctx context.Context, p ComposeProject) (ObservedState, error)
	// Down stops and removes the project's containers (never volumes,
	// unless explicitly requested — Destroy is a separate, explicit action).
	Down(ctx context.Context, projectName string, removeVolumes bool) error
	// Observe reports current state for drift detection (reconciliation).
	Observe(ctx context.Context, projectName string) (ObservedState, error)
}

type ObservedState struct {
	Containers []ContainerState
}

type ContainerState struct {
	Name    string
	Image   string
	Healthy bool
	Labels  map[string]string // groundplane.managed=true, .tenant/.project/.environment/.service/.release/.slot
}

// New returns a docker-API-backed Applier.
//
// TODO: wire github.com/docker/docker/client, or shell out to
// `docker compose` — either is fine per architecture.md ("the volatile
// layer... quarantined from the shared core"); only this package cares
// which.
func New(ctx context.Context) (Applier, error) {
	return nil, errs.New(errs.CodeNotImplemented, "docker: not implemented — wire the docker client in internal/infra/docker")
}

// Env-file and resolv.conf materialization live in internal/agent
// (materializer.go), not here — those are Agent-level host
// materialization steps (see mvp.md, "Everything host-level is actioned
// by the Agent"), not docker/compose concerns. This package's job ends
// at applying the compose project itself.
