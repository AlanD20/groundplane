package etcd

import (
	"context"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func (ledger *ReleaseLedger) resolveSuccessfulReleaseImage(
	ctx context.Context,
	environmentID, serviceID string,
	intent domain.Intent,
	revision int64,
) (*domain.ResolvedImageEvidence, error) {
	read, err := ledger.store.GetMany(ctx, GetManyRequest{
		Keys:     []string{releaseTerminalKey(intent.ID)},
		Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, corruptReleaseRecord()
	}
	terminal, err := decodeReleaseRecord[domain.TerminalSummary](read.Values[0].Value, "release-terminal-summary")
	if err != nil || terminal.ReleaseID != intent.ID || terminal.FinalServingReleaseID != intent.ID {
		return nil, corruptReleaseRecord()
	}
	if terminal.ResolvedImage == nil {
		return nil, nil
	}
	if domain.ValidateResolvedImageEvidence(*terminal.ResolvedImage, intent) != nil {
		return nil, corruptReleaseRecord()
	}
	resolved := *terminal.ResolvedImage
	return &resolved, nil
}
