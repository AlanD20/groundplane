package attachexecution

type FailureReason string

const (
	ExecutionFailed      FailureReason = "execution_failed"
	DeadlineExpired      FailureReason = "deadline_expired"
	ExecutionInterrupted FailureReason = "execution_interrupted"
)

func (reason FailureReason) Valid() bool {
	return reason == ExecutionFailed || reason == DeadlineExpired || reason == ExecutionInterrupted
}
