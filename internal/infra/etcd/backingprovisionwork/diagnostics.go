package backingprovisionwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// DiagnosticBegin reads the actual acknowledged Begin, not a reconstructed
// timestamp or a different attempt's observation.
func (repository *Repository) DiagnosticBegin(ctx context.Context, view workqueue.ResumeView,
	layout backingprovision.Layout, progress backingprovision.Progress,
) (backingprovision.Progress, error) {
	if progress.Validate(view.Attempt.Attempt, layout) != nil || progress.Sequence == 0 {
		return backingprovision.Progress{}, invalid("backing diagnostic has no recorded execution boundary")
	}
	if progress.InFlight {
		return progress, nil
	}
	key := ReportKey(view.Attempt.Attempt.ID, progress.Sequence-1)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: view.Work.ReadRevision})
	if err != nil {
		return backingprovision.Progress{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 || read.Values[0] == nil {
		return backingprovision.Progress{}, invalid("backing diagnostic start observation is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value.Key != key || value.ModRevision <= 0 || value.ModRevision > read.ReadRevision {
		return backingprovision.Progress{}, invalid("backing diagnostic start revision is invalid")
	}
	return backingprovision.DecodeProgress(view.Attempt.Attempt, layout, value.Value)
}
