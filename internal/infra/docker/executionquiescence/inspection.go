// Package executionquiescence observes transient GP executors without changing
// containers, application workloads, or retained recovery evidence.
package executionquiescence

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

const maximumContainers = 4096

type Engine interface {
	ContainerList(context.Context, client.ContainerListOptions) (client.ContainerListResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
}

type Inspector struct{ engine Engine }

func New(engine Engine) (*Inspector, error) {
	if engine == nil {
		return nil, errs.New(errs.KindInternal, "execution quiescence requires a Docker reader")
	}
	return &Inspector{engine: engine}, nil
}

// RequireQuiescent is evidence only while its caller excludes new execution.
// A created helper also blocks: this observer cannot authorize its later start.
// Even a disappeared or replaced helper is uncertainty, rather than permission.
func (inspector *Inspector) RequireQuiescent(ctx context.Context) error {
	if ctx == nil || inspector == nil || inspector.engine == nil {
		return errs.New(errs.KindInternal, "execution quiescence reader is not configured")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	listed, err := inspector.engine.ContainerList(
		ctx,
		client.ContainerListOptions{All: true, Limit: maximumContainers + 1},
	)
	if err != nil {
		return errs.Wrap(errs.KindStorageUnavailable, err)
	}
	if len(listed.Items) > maximumContainers {
		return blocked()
	}
	for _, candidate := range listed.Items {
		helper := transient(candidate.Labels)
		if !helper && candidate.Labels["com.groundplane.managed"] != "true" {
			continue
		}
		if candidate.ID == "" {
			return blocked()
		}
		observed, err := inspector.engine.ContainerInspect(ctx, candidate.ID, client.ContainerInspectOptions{})
		if err != nil {
			return errs.Wrap(errs.KindStorageUnavailable, err)
		}
		value := observed.Container
		if value.ID != candidate.ID || value.Config == nil || value.HostConfig == nil || value.State == nil ||
			value.Config.Labels["com.groundplane.managed"] != candidate.Labels["com.groundplane.managed"] ||
			value.Config.Labels["io.groundplane.role"] != candidate.Labels["io.groundplane.role"] ||
			value.Config.Labels["com.groundplane.kind"] != candidate.Labels["com.groundplane.kind"] {
			return blocked()
		}
		// A joined Go worker does not prove that its Docker exec has ended.
		// Inspect serving containers without stopping them, and reject every
		// retained exec identity, including an uncertain future start.
		if len(value.ExecIDs) != 0 {
			return blocked()
		}
		if !helper {
			continue
		}
		state := value.State
		if state.Running || state.Restarting || state.Paused || state.Pid != 0 ||
			(state.Status != "exited" && state.Status != "dead") ||
			(value.HostConfig.RestartPolicy.Name != "" && value.HostConfig.RestartPolicy.Name != "no") {
			return blocked()
		}
	}
	return ctx.Err()
}

func transient(labels map[string]string) bool {
	// Native Compose executors and materializers carry io.groundplane.role;
	// application and backing containers do not use this executor-only label.
	if labels["io.groundplane.role"] != "" {
		return true
	}
	if labels["com.groundplane.managed"] != "true" {
		return false
	}
	kind := labels["com.groundplane.kind"]
	return strings.Contains(kind, "helper") || strings.Contains(kind, "executor") ||
		strings.HasSuffix(kind, "-check") || kind == "script-runner" || kind == "configuration-materializer"
}

func blocked() error {
	return errs.New(
		errs.KindResourceInUse,
		"GP execution helpers have not been proved stopped; recovery ownership is retained",
	)
}
