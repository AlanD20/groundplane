package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// prepareRecoveredBlueprintRuntime joins failed history with the actual
// retained serving state. Restored operations keep their predecessor authority;
// leave_active records the original proved candidate without promoting success.
func (ledger *ReleaseLedger) prepareRecoveredBlueprintRuntime(ctx context.Context, view workqueue.ResumeView,
	input releaseexecution.RecoveryInput, sources ReleaseWorkSources, at time.Time,
) (workqueue.Change, workqueue.Change, error) {
	empty := workqueue.Change{}
	preserved, err := releasework.LoadPreservedServing(ctx, ledger.store, input, view.Work.ReadRevision)
	if err != nil {
		return empty, empty, err
	}
	if input.PreservesServing() != (preserved.Proof != nil) {
		return empty, empty, errs.New(
			errs.KindStateConflict,
			"Release recovery no longer matches its captured serving decision",
		)
	}
	completed := BlueprintReleaseCompletion{PublicationID: sources.Rollout.Record.PublicationID,
		Configuration: sources.Input.Configuration, Predecessor: sources.Predecessors[0]}
	runtime := workqueue.Change{Conditions: preserved.Conditions}
	if input.PreservesServing() {
		completed.Proof = *preserved.Proof
		record, err := serviceruntimerecord.AcknowledgePreservedRelease(
			input,
			preserved,
			completed.Configuration.Current,
			at,
		)
		if err != nil {
			return empty, empty, err
		}
		value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
		if err != nil {
			return empty, empty, err
		}
		runtime.Mutations = append(runtime.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: serviceruntimerecord.Key(record.Runtime.ServiceID), Value: value})
		configuration := completed.Configuration
		if len(configuration.Destinations) != 0 &&
			(configuration.Prior == nil || configuration.Current != *configuration.Prior) {
			value, err := runtimeconfiguration.EncodeReference(configuration.Current)
			if err != nil {
				keyvalue.ZeroMutationBytes(runtime.Mutations)
				return empty, empty, err
			}
			runtime.Mutations = append(runtime.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
				Key: runtimeconfiguration.HeadKey(input.Input.Release.EnvironmentId), Value: value})
		}
	}
	history, err := ledger.prepareBlueprintReleaseHistory(ctx, view, input.Input, completed, &input, at)
	if err != nil {
		keyvalue.ZeroMutationBytes(runtime.Mutations)
		return empty, empty, err
	}
	return runtime, history, nil
}
