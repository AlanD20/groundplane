package agent

import (
	"fmt"

	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A refused terminal result remains in the durable outbox. Wake only its caller
// with an error; do not acknowledge the journal or close unrelated exchanges.
func (exchange *nativeExchange) rejectReport(message *agentpb.ControllerMessage) error {
	rejection := message.GetWorkReportRejection()
	if rejection == nil || rejection.Attempt == nil {
		return errs.New(errs.KindStateConflict, "terminal report refusal lacks its attempt")
	}
	exchange.mu.Lock()
	defer exchange.mu.Unlock()
	key := nativeExchangeKey{rejection.Attempt.AttemptId, rejection.Kind}
	waiter := exchange.pending[key]
	if exchange.closed || waiter == nil || waiter.terminalReport == nil ||
		workreceipt.MatchRejection(waiter.terminalReport, message) != nil {
		return errs.New(errs.KindStateConflict, "terminal report refusal differs from the pending report")
	}
	delete(exchange.pending, key)
	if !waiter.abandoned {
		waiter.response <- nativeResponse{err: errs.New(errs.KindRequestFailed,
			fmt.Sprintf("Controller could not settle the retained result (%s): %s",
				rejection.FailureCode, rejection.FailureDetail))}
	}
	return nil
}
