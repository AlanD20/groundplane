package postgres16execution

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RecoverExecution reads proof for the original Exec. It cannot launch the
// original request and never converts a missing/failed Exec into success.
func (executor *Executor) RecoverExecution(ctx context.Context, expected Container,
	original postgres16protocol.Request, execID string,
) (postgres16protocol.ExecutionEvidence, error) {
	if !validDockerID(execID) {
		return postgres16protocol.ExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"PostgreSQL original Exec identity is invalid",
		)
	}
	digest, err := postgres16protocol.ExecutionRequestSHA256(original)
	if err != nil {
		return postgres16protocol.ExecutionEvidence{}, err
	}
	_, code, err := executor.inspectExit(ctx, expected, execID)
	if err != nil || code != postgres16protocol.ExitSuccess {
		return postgres16protocol.ExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"PostgreSQL original Exec success is unproven",
		)
	}
	result, err := executor.Execute(ctx, expected, postgres16protocol.Request{
		Operation: postgres16protocol.OperationEvidence, Nonce: original.Nonce,
		DeadlineUnixNano: original.DeadlineUnixNano, RequestSHA256: digest,
	}, nil, nil)
	if err != nil {
		return postgres16protocol.ExecutionEvidence{}, err
	}
	evidence, err := postgres16protocol.ParseExecutionEvidence(result.Proof)
	if err != nil || evidence.Nonce != original.Nonce || evidence.RequestSHA256 != digest {
		return postgres16protocol.ExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"PostgreSQL retained execution proof changed",
		)
	}
	if original.Operation == postgres16protocol.OperationRestoreApply &&
		(evidence.State.IOEvidence.Stdin.Bytes != original.SourceSize ||
			evidence.State.IOEvidence.Stdin.SHA256 != original.SourceSHA256) {
		return postgres16protocol.ExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"PostgreSQL restored source proof changed",
		)
	}
	return evidence, nil
}

// RetireExecution removes only the selected reaped helper evidence. Callers
// must first retain its successful result or receive the Controller's durable
// source-cleanup acknowledgement. Already-retired evidence is harmless here;
// this method does not return execution proof or authorize another attempt.
func (executor *Executor) RetireExecution(ctx context.Context, expected Container,
	original postgres16protocol.Request,
) error {
	digest, err := postgres16protocol.ExecutionRequestSHA256(original)
	if err != nil {
		return err
	}
	_, err = executor.Execute(ctx, expected, postgres16protocol.Request{
		Operation: postgres16protocol.OperationRetire, Nonce: original.Nonce,
		DeadlineUnixNano: original.DeadlineUnixNano, RequestSHA256: digest,
	}, nil, nil)
	return err
}
