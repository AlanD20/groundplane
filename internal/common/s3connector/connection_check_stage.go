package s3connector

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ConnectionCheckStageName string

const (
	CheckWriteObject ConnectionCheckStageName = "Write object"
	CheckReadObject  ConnectionCheckStageName = "Read and verify object"
	CheckMultipart   ConnectionCheckStageName = "Create and upload multipart part"
	CheckCleanup     ConnectionCheckStageName = "Delete object and abort multipart; verify cleanup"
)

type ConnectionCheckFailure string

const (
	CheckFailureNone        ConnectionCheckFailure = ""
	CheckFailureTimeout     ConnectionCheckFailure = "timeout"
	CheckFailureCanceled    ConnectionCheckFailure = "canceled"
	CheckFailurePermissions ConnectionCheckFailure = "permissions"
	CheckFailureConnection  ConnectionCheckFailure = "connection"
	CheckFailureOwnership   ConnectionCheckFailure = "ownership"
	CheckFailureJournal     ConnectionCheckFailure = "journal"
	CheckFailureInterrupted ConnectionCheckFailure = "interrupted"
)

type ConnectionCheckStage struct {
	Name      ConnectionCheckStageName `json:"name"`
	Elapsed   time.Duration            `json:"elapsed"`
	Succeeded bool                     `json:"succeeded"`
	Failure   ConnectionCheckFailure   `json:"failure,omitempty"`
}

func (stage ConnectionCheckStage) Valid() bool {
	switch stage.Name {
	case CheckWriteObject, CheckReadObject, CheckMultipart, CheckCleanup:
	default:
		return false
	}
	if stage.Elapsed < 0 || stage.Succeeded != (stage.Failure == CheckFailureNone) {
		return false
	}
	switch stage.Failure {
	case CheckFailureNone, CheckFailureTimeout, CheckFailureCanceled, CheckFailurePermissions,
		CheckFailureConnection, CheckFailureOwnership, CheckFailureJournal, CheckFailureInterrupted:
		return true
	default:
		return false
	}
}

// CheckFailureForError returns only a closed public-safe reason, never provider
// response text, URLs, headers or credential-bearing causes.
func CheckFailureForError(err error) ConnectionCheckFailure {
	if err == nil {
		return CheckFailureNone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return CheckFailureTimeout
	}
	if errors.Is(err, context.Canceled) {
		return CheckFailureCanceled
	}
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindValidationFailed:
		return CheckFailurePermissions
	case errs.KindStorageUnavailable:
		return CheckFailureConnection
	case errs.KindStateConflict:
		return CheckFailureOwnership
	default:
		return CheckFailureJournal
	}
}
