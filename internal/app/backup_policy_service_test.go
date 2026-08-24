package app

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: only a preparation that proves era 1 is absent may trigger
// subordinate age identity generation and supply.
func TestBackupPolicySetGeneratesInitialKeyOnlyWhenPreparedNeedsIt(t *testing.T) {
	tests := []struct {
		name        string
		needsKey    bool
		keyError    error
		wantKeys    int
		wantSupply  int
		wantReplace int
	}{
		{name: "new key", needsKey: true, wantKeys: 1, wantSupply: 1, wantReplace: 1},
		{name: "existing key", wantReplace: 1},
		{
			name: "generation failure", needsKey: true,
			keyError: errs.New(errs.KindInternal, "key generation failed"), wantKeys: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := &backupPolicyTestRepository{needsKey: test.needsKey}
			keys := &backupPolicyTestKeys{err: test.keyError}
			service := backupPolicyTestService(t, repository, keys, &backupPolicyTestIdempotency{})
			_, err := service.SetBackupPolicy(
				context.Background(), testEnvironmentID, backupPolicyTestRequest(), testIdempotencyKey,
			)
			if !errors.Is(err, test.keyError) {
				t.Fatalf("SetBackupPolicy() error = %v, want %v", err, test.keyError)
			}
			if keys.calls != test.wantKeys || repository.supplyCalls != test.wantSupply ||
				repository.replaceCalls != test.wantReplace {
				t.Fatalf(
					"calls = keys %d, supply %d, replace %d",
					keys.calls, repository.supplyCalls, repository.replaceCalls,
				)
			}
		})
	}
}

// Rationale: exact replay must bypass persistence preparation and key
// generation even if current dependencies or key state changed.
func TestBackupPolicySetReplayDoesNotPrepareOrGenerate(t *testing.T) {
	repository := &backupPolicyTestRepository{}
	keys := &backupPolicyTestKeys{}
	replayBody := []byte(`{ "sources" : [] , "enabled" : false }`)
	idempotency := &backupPolicyTestIdempotency{
		existing: true,
		response: etcd.IdempotencyResponse{
			Status: 200, ContentKind: "application/json", Body: replayBody,
		},
	}
	service := backupPolicyTestService(t, repository, keys, idempotency)
	result, err := service.SetBackupPolicy(
		context.Background(), testEnvironmentID, backupPolicyTestRequest(), testIdempotencyKey,
	)
	if err != nil || result.Policy.Sources == nil || repository.prepareCalls != 0 || keys.calls != 0 {
		t.Fatalf(
			"SetBackupPolicy(replay) = %#v, %v; prepare=%d keys=%d",
			result, err, repository.prepareCalls, keys.calls,
		)
	}
	if !bytes.Equal(result.Representation, replayBody) {
		t.Fatalf("replay representation = %q, want %q", result.Representation, replayBody)
	}
}

// Rationale: the transaction-bound public limit must fail before idempotency
// or source-catalog preparation can create durable side effects.
func TestBackupPolicySetRejectsThirteenthSourceBeforePreparation(t *testing.T) {
	repository := &backupPolicyTestRepository{}
	idempotency := &backupPolicyTestIdempotency{}
	service := backupPolicyTestService(t, repository, &backupPolicyTestKeys{}, idempotency)
	input := apiTypes.BackupPolicyReplacementRequest{Sources: make([]apiTypes.BackupSourceInput, 13)}
	if _, err := service.SetBackupPolicy(
		context.Background(),
		testEnvironmentID,
		input,
		testIdempotencyKey,
	); err == nil {
		t.Fatal("SetBackupPolicy() error = nil")
	}
	if repository.prepareCalls != 0 || idempotency.prepareCalls != 0 {
		t.Fatalf("preparation calls = repository %d, idempotency %d", repository.prepareCalls, idempotency.prepareCalls)
	}
}
