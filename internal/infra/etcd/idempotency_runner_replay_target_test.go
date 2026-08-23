package etcd

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Runner deletion requires the same stable post-delete replay index
// as every other protected resource deletion.
func TestIdempotencyRunnerReplayTargetUsesStableRunnerIdentity(t *testing.T) {
	t.Parallel()
	runnerID := ids.NewAt(ids.KindRunner, taskJournalTime(), 2900)
	if err := validateIdempotencyReplayTarget(IdempotencyReplayTarget{
		Kind: IdempotencyReplayTargetRunner, ID: runnerID,
	}); err != nil {
		t.Fatalf("validateIdempotencyReplayTarget(runner) error = %v", err)
	}
	if err := validateIdempotencyReplayTarget(IdempotencyReplayTarget{
		Kind: IdempotencyReplayTargetRunner,
		ID:   ids.NewAt(ids.KindConnector, taskJournalTime(), 2901),
	}); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("validateIdempotencyReplayTarget(wrong identity) error = %v", err)
	}
}
