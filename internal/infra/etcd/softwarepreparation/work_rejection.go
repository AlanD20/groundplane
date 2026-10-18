package softwarepreparation

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	preparation "github.com/AlanD20/groundplane/internal/infra/softwarepreparation"
)

func PrepareUnstarted(snapshot WorkSnapshot, outcome workexecution.Outcome, at time.Time) (workqueue.Change, error) {
	progress := preparation.Progress{Phase: preparation.PhaseFailed, Result: snapshot.Record.Record.Progress.Result,
		ErrorCode: "execution.not_started", ErrorDetail: "Preparation ended before execution: " + string(outcome) + ". No artifact build started."}
	change, _, err := prepareProgress(snapshot, progress, at)
	return change, err
}
