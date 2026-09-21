package app

import (
	"errors"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func integrationTransactionApplied(result etcd.IdempotencyTransactionResult) bool {
	outcome, _, conflict, err := result.Classify()
	return err == nil && conflict == nil && outcome == etcd.IdempotencyKnownApplied
}

func integrationTransactionConflict(result etcd.IdempotencyTransactionResult) error {
	_, _, conflict, err := result.Classify()
	if err != nil {
		return err
	}
	return conflict
}

func isKind(err error, want errs.Kind) bool {
	return errors.Is(err, errs.New(want, ""))
}
