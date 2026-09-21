package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func (ledger *ReleaseLedger) verifySuccessfulRelease(
	ctx context.Context,
	environmentID, serviceID string,
	intent domain.Intent,
	revision int64,
) error {
	read, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys:     []string{releases.ReleaseTerminalKey(intent.ID)},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return releases.CorruptReleaseRecord()
	}
	terminal, err := releases.DecodeReleaseRecord[domain.TerminalSummary](read.Values[0].Value, "release-terminal-summary")
	if err != nil || terminal.ReleaseID != intent.ID || terminal.FinalServingReleaseID != intent.ID {
		return releases.CorruptReleaseRecord()
	}
	return nil
}
