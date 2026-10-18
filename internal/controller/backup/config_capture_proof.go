package backup

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// configCaptureProofTransport makes canonical source replay observational.
// A complete native history must consume no new channel I/O or credit writes.
type configCaptureProofTransport struct{}

func (configCaptureProofTransport) SendFrame(context.Context, *agentpb.BackupConfigTransfer) error {
	return invalidConfigCaptureCompletion()
}

func (configCaptureProofTransport) ReadCredit(context.Context) (*agentpb.BackupConfigCredit, error) {
	return nil, invalidConfigCaptureCompletion()
}

func (configCaptureProofTransport) CommitCredit(context.Context, *agentpb.BackupConfigCredit) error {
	return invalidConfigCaptureCompletion()
}

func invalidConfigCaptureCompletion() error {
	return errs.New(errs.KindStateConflict, "Config capture completion differs from its sealed source or native cursor")
}
