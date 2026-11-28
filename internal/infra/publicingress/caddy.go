package publicingress

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	adminListen   = "127.0.0.1:2019"
	adminURL      = "http://" + adminListen + "/config/"
	maxOutput     = maximumConfigBytes + 16384
	dockerTimeout = 20 * time.Second
)

var dockerEnvironment = []string{"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
	"DOCKER_HOST=unix:///var/run/docker.sock"}

// RuntimeOwner verifies the captured container, image, and listener/network
// ownership. It also fences effects from the previous Agent attempt before
// recovery reads the active configuration.
type RuntimeOwner interface {
	Verify(context.Context, string) error
	Quiescent(context.Context, string) error
}

// Probe verifies the enabled listener and readiness for the captured receipt.
// The caller binds that receipt to owned ports and independently expected
// route evidence. Reading Caddy's loopback admin endpoint is not readiness.
type Probe func(context.Context, Receipt) error

type Caddy struct {
	runner runner.Runner
	owner  RuntimeOwner
	probe  Probe
}

func NewCaddy(taskRunner runner.Runner, owner RuntimeOwner, probe Probe) (*Caddy, error) {
	if taskRunner == nil || owner == nil || probe == nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress Caddy runtime dependencies are missing")
	}
	return &Caddy{runner: taskRunner, owner: owner, probe: probe}, nil
}

func (caddy *Caddy) Validate(ctx context.Context, runtimeID string, cfg []byte) error {
	source, err := caddyConfiguration(cfg)
	if err != nil {
		return err
	}
	_, err = caddy.run(ctx, runtimeID, source, "caddy", "validate", "--config", "-")
	return err
}

func (caddy *Caddy) Reload(ctx context.Context, runtimeID string, cfg []byte) error {
	source, err := caddyConfiguration(cfg)
	if err != nil {
		return err
	}
	// The narrow exec contract leaves the old runtime intact. Caddy's /load
	// endpoint validates and replaces configuration transactionally.
	_, err = caddy.run(ctx, runtimeID, source, "caddy", "reload", "--address", adminListen, "--config", "-")
	return err
}

func (caddy *Caddy) Active(ctx context.Context, runtimeID string) ([]byte, error) {
	output, err := caddy.run(ctx, runtimeID, nil, "wget", "--quiet", "--output-document=-", adminURL)
	if err != nil {
		return nil, err
	}
	return caddyConfiguration(output)
}

func (caddy *Caddy) Ready(ctx context.Context, receipt Receipt) error {
	if err := caddy.verify(ctx, receipt.RuntimeID); err != nil {
		return err
	}
	return caddy.probe(ctx, receipt)
}

func (caddy *Caddy) Quiescent(ctx context.Context, runtimeID string) error {
	if err := caddy.verify(ctx, runtimeID); err != nil {
		return err
	}
	return caddy.owner.Quiescent(ctx, runtimeID)
}

func (caddy *Caddy) verify(ctx context.Context, runtimeID string) error {
	if ctx == nil || !validRuntimeID(runtimeID) {
		return errs.New(errs.KindValidationFailed, "public ingress runtime identity or context is invalid")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return caddy.owner.Verify(ctx, runtimeID)
}

func (caddy *Caddy) run(ctx context.Context, runtimeID string, input []byte, command ...string) ([]byte, error) {
	if err := caddy.verify(ctx, runtimeID); err != nil {
		return nil, err
	}
	args := []string{"container", "exec"}
	if len(input) > 0 {
		args = append(args, "--interactive")
	}
	args = append(append(args, runtimeID), command...)
	result, err := caddy.runner.Run(ctx, runner.RunCmdOpts{
		Name: "docker", Args: args, Env: dockerEnvironment,
		ReplaceEnv: true, Stdin: input, Timeout: dockerTimeout, CaptureLimitBytes: maxOutput,
	})
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil || result.ExitCode != 0 || len(result.Stdout)+len(result.Stderr) >= maxOutput {
		return nil, errs.New(errs.KindRequestFailed, "public ingress Caddy command did not complete")
	}
	if err := caddy.verify(ctx, runtimeID); err != nil {
		return nil, err
	}
	return result.Stdout, nil
}

func caddyConfiguration(raw []byte) ([]byte, error) {
	source, err := canonicalObject(raw)
	if err != nil {
		return nil, err
	}
	// This is a committed profile from the closed renderer. Ensure its
	// administration cannot be disabled, moved off loopback, or made remote,
	// and only the verified host-side startup file persists across restarts.
	var document struct {
		Admin json.RawMessage `json:"admin"`
	}
	if err := json.Unmarshal(source, &document); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress Caddy admin configuration is invalid")
	}
	const admin = `{"config":{"persist":false},"listen":"` + adminListen + `"}`
	if string(document.Admin) != admin {
		return nil, errs.New(
			errs.KindValidationFailed,
			"public ingress Caddy admin profile must remain local without autosave",
		)
	}
	return source, nil
}
