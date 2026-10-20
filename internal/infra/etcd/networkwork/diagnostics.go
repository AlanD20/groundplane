package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NetworkDiagnosticStages uses the retained begin report even after a lost
// confirmation response. It never substitutes assignment or receipt time.
func (repository *InputRepository) NetworkDiagnosticStages(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.NetworkWorkInput, progress networkexecution.Progress,
) (workexecution.Stage, workexecution.Stage, error) {
	if input == nil {
		return workexecution.Stage{}, workexecution.Stage{}, conflict("Network diagnostic input is missing")
	}
	begin := progress
	if !progress.InFlight {
		key := ReportKey(view.Attempt.Attempt.ID, 1)
		read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
			Keys: []string{key}, Revision: view.Work.ReadRevision,
		})
		if err != nil {
			return workexecution.Stage{}, workexecution.Stage{}, err
		}
		if read != nil {
			defer keyvalue.ClearValues(read.Values)
		}
		if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 1 ||
			read.Values[0] == nil || read.Values[0].Key != key || read.Values[0].ModRevision <= 0 {
			return workexecution.Stage{}, workexecution.Stage{}, conflict(
				"Network diagnostic begin report is unavailable",
			)
		}
		begin, err = networkexecution.DecodeProgress(
			view.Attempt.Attempt,
			input.Action,
			input.NetworkId,
			read.Values[0].Value,
		)
		if err != nil {
			return workexecution.Stage{}, workexecution.Stage{}, err
		}
	}
	return networkexecution.DiagnosticStages(view.Attempt.Attempt, input, begin, progress)
}
