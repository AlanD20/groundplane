package connectorchecks

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/s3connector"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareCleanupRetry transfers only pending cleanup to the native retry Task.
// The original operation, plan, destination, keys, credentials and pins remain
// fixed. The prior record keeps failure evidence, but no ciphertext copy.
func PrepareCleanupRetry(source Record, retryTaskID string) (Record, Record, error) {
	if source.Validate() != nil || source.Phase != CleanupRequired ||
		ids.Validate(ids.KindTask, retryTaskID) != nil || retryTaskID == source.Owner.TaskID {
		return Record{}, Record{}, errs.New(
			errs.KindTaskNotRetryable,
			"only pending Connector cleanup can be retried; start a new Test connection for a new probe",
		)
	}
	retry := Clone(source)
	retry.Owner.TaskID, retry.RetryOf = retryTaskID, source.Owner.TaskID
	retry.Phase, retry.Outcome, retry.Failure, retry.FinishedAt = Cleaning, "", s3connector.CheckFailureNone, nil
	previous := Clone(source)
	previous.Clear()
	previous.Phase, previous.TransferredTo = Transferred, retryTaskID
	if err := retry.Validate(); err != nil {
		retry.Clear()
		return Record{}, Record{}, err
	}
	if err := previous.Validate(); err != nil {
		retry.Clear()
		return Record{}, Record{}, err
	}
	return previous, retry, nil
}
