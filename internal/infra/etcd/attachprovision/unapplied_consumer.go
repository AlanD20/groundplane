package attachprovision

import (
	"github.com/AlanD20/groundplane/internal/core"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
)

// Running intent is also the initial state before first Deploy. Only durable
// applied/pending execution evidence requires a host network transition.
func unappliedConsumer(capture blueprintattachinputs.Generation, state, projection *keyvalue.KeyValue) (bool, error) {
	if state == nil {
		return false, checkpointConflict("Attach consumer runtime state is unavailable")
	}
	runtime, err := services.DecodeServiceRuntimeRecord(state.Value)
	if err != nil {
		return false, err
	}
	if runtime.EnvironmentID != capture.EnvironmentID || runtime.ServiceID != capture.ConsumerServiceID {
		return false, recordcodec.CorruptRecord()
	}
	if runtime.Runtime.UnconfirmedLifecycleTaskID != "" ||
		(runtime.Runtime.RuntimeIntent != core.ServiceRuntimeIntentRunning && runtime.Runtime.RuntimeIntent != core.ServiceRuntimeIntentStopped) {
		return false, nil
	}
	if projection == nil {
		return true, nil
	}
	release, err := releases.DecodeReleaseRecord[releasedomain.ServiceProjection](
		projection.Value,
		"service-release-projection",
	)
	if err != nil {
		return false, err
	}
	if release.EnvironmentID != capture.EnvironmentID || release.ServiceID != capture.ConsumerServiceID ||
		release.Revision == 0 {
		return false, recordcodec.CorruptRecord()
	}
	return release.ServingReleaseID == "" && release.CurrentSuccessfulReleaseID == "" &&
		release.ActiveOperationID == "", nil
}
