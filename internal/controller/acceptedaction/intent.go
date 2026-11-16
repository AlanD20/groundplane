package acceptedaction

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Intent keeps canonical replay evidence opaque while an action owner supplies
// typed operator input. It never exposes the Submitter's storage or cryptography.
type Intent struct {
	locator  idempotency.IdempotencyLocator
	evidence requestidempotency.ProtectedEvidence
	record   idempotency.ProtectedIntentRecord
	replay   *idempotency.IdempotencyResponse
}

// ReplayLocator finds a prior resource action even after that resource is gone.
// The caller still validates its canonical intent through Begin before replay.
func (submitter *Submitter) ReplayLocator(ctx context.Context, target idempotency.IdempotencyReplayTarget,
	method, route, key string,
) (idempotency.IdempotencyLocator, bool, error) {
	if submitter == nil {
		return idempotency.IdempotencyLocator{}, false, errs.New(
			errs.KindInternal,
			"accepted action submission is unavailable",
		)
	}
	return submitter.replay.ResolveReplayLocator(ctx, target, method, route, key)
}

func (submitter *Submitter) Begin(ctx context.Context, input requestidempotency.CanonicalIntentV1,
	locator idempotency.IdempotencyLocator,
) (Intent, error) {
	expectedScopeID := input.Scope.ID
	if input.Scope.Kind == requestidempotency.ScopePlatform {
		expectedScopeID = "-"
	}
	if submitter == nil || input.Method != locator.Method || input.Route != locator.Route ||
		string(input.Scope.Kind) != string(locator.ScopeKind) || expectedScopeID != locator.ScopeID {
		return Intent{}, errs.New(errs.KindInternal, "accepted action replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, input)
	if err != nil {
		return Intent{}, err
	}
	defer digest.Destroy()
	evidence, err := submitter.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return Intent{}, err
	}
	record, err := evidence.DurableRecord()
	if err != nil {
		evidence.Destroy()
		return Intent{}, err
	}
	result := Intent{locator: locator, evidence: evidence, record: record}
	resolution, exists, err := submitter.coordinator.ResolveExisting(ctx, submitter.replay, locator, evidence)
	if err != nil {
		result.Destroy()
		return Intent{}, err
	}
	if exists {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			result.Destroy()
			return Intent{}, errs.New(errs.KindInternal, "accepted action replay is invalid")
		}
		response := resolution.Response
		response.Body = append([]byte(nil), response.Body...)
		result.replay = &response
	}
	return result, nil
}

func (intent *Intent) Destroy() {
	if intent == nil {
		return
	}
	intent.evidence.Destroy()
	clear(intent.record.Ciphertext)
	intent.record.Ciphertext = nil
}

func (intent Intent) Replay() (idempotency.IdempotencyResponse, bool) {
	if intent.replay == nil {
		return idempotency.IdempotencyResponse{}, false
	}
	response := *intent.replay
	response.Body = append([]byte(nil), response.Body...)
	return response, true
}

func (intent Intent) Record() idempotency.ProtectedIntentRecord {
	record := intent.record
	record.Ciphertext = append([]byte(nil), record.Ciphertext...)
	return record
}

func (intent Intent) Submission(task workexecution.Task, kind string) Submission {
	return Submission{Task: task, Kind: kind, Locator: intent.locator, Intent: intent.record, Evidence: intent.evidence}
}
