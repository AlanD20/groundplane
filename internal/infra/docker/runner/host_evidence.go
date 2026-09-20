package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

func applied(step corerunner.Step) corerunner.StepEvidence {
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectApplied}
}

func absent(plan corerunner.Plan, step corerunner.Step) corerunner.StepEvidence {
	return corerunner.StepEvidence{Step: step, State: corerunner.EffectAbsent, ReceiptSHA256: receipt(plan, step)}
}

func receipt(plan corerunner.Plan, step corerunner.Step) string {
	digest := sha256.Sum256([]byte("groundplane.runner-removal.v1\x00" + plan.Digest() + "\x00" + string(step)))
	return "sha256:" + hex.EncodeToString(digest[:])
}

func runnerToken(plan corerunner.Plan) string {
	return strings.ToLower(strings.TrimPrefix(plan.RunnerID, "run_"))
}

func daemonUnit(plan corerunner.Plan) string {
	return "groundplane-runner-" + runnerToken(plan) + "-daemon.service"
}

func proxyUnit(plan corerunner.Plan) string {
	return "groundplane-runner-" + runnerToken(plan) + "-proxy.service"
}

func nftTable(plan corerunner.Plan) string { return "gpr_" + runnerToken(plan) }

func dockerError(ctx context.Context, operation string, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return errs.Wrap(errs.KindInternal, fmt.Errorf("%s: %w", operation, err))
}

func rootCause(err error) error {
	for errors.Unwrap(err) != nil {
		err = errors.Unwrap(err)
	}
	return err
}
