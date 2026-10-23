package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type blueprintReleaseConfigurationCapture struct {
	Rollout         workexecution.Input              `json:"rollout"`
	ConfigurationID string                           `json:"configuration_id"`
	Capture         runtimeconfiguration.WorkCapture `json:"capture"`
}

func ReleaseWorkConfigurationCaptureKey(workID string) string {
	return "/v1/execution/release-configuration-captures/" + workID
}

// Runtime planning can precede router execution. Capture the acknowledged file
// baseline at this later preparation boundary, then reuse that exact capture.
// Direct Deploy/Rollback retain theirs in the accepted direct-preparation root.
func (ledger *ReleaseLedger) PrepareBlueprintReleaseWorkConfiguration(ctx context.Context,
	work workqueue.View, rollout releaserender.ServiceRolloutCapture,
	configurationID string, generation uint64, files []taskmaterialization.Record,
) (runtimeconfiguration.WorkCapture, error) {
	var empty runtimeconfiguration.WorkCapture
	if ledger == nil || ledger.store == nil || ctx == nil || work.Task.Task.Action != workexecution.ActionApply ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		ids.Validate(ids.KindConfig, configurationID) != nil ||
		work.Task.Task.Stop != nil || work.Task.Task.FinishedAt != nil ||
		work.Task.Task.CollectionStartedAt != nil || work.Item.Item.Input != nil ||
		work.Item.Item.AttemptNumber != 0 || work.Item.Item.ForwardRevoked ||
		work.Item.Item.Phase != workexecution.Waiting || rollout.MatchesWork(work.Task.Task, work.Item.Item) != nil {
		return empty, errs.New(errs.KindStateConflict, "Blueprint file capture requires its unstarted Release work")
	}
	reference, err := releaserender.ServiceRolloutInput(rollout)
	if err != nil {
		return empty, err
	}
	key := ReleaseWorkConfigurationCaptureKey(work.Item.Item.ID)
	keys := []string{key, workqueue.TaskKey(work.Task.Task.ID), workqueue.ItemKey(work.Item.Item.ID)}
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return empty, err
	}
	if read == nil || len(read.Values) != 3 || read.ReadRevision < work.ReadRevision {
		return empty, errs.New(errs.KindStateConflict, "Blueprint file capture ownership read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[1].Key != keys[1] || read.Values[2].Key != keys[2] ||
		read.Values[1].ModRevision != work.Task.Revision || read.Values[2].ModRevision != work.Item.Revision {
		return empty, errs.New(errs.KindStateConflict, "Blueprint file capture owner changed during preparation")
	}
	decode := func(value *keyvalue.KeyValue) (runtimeconfiguration.WorkCapture, error) {
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			return empty, errs.New(errs.KindStateConflict, "Blueprint file capture is unavailable")
		}
		stored, err := recordcodec.Decode[blueprintReleaseConfigurationCapture](
			value.Value,
			"blueprint-release-configuration",
		)
		if err != nil {
			return empty, err
		}
		if stored.Rollout != reference || stored.ConfigurationID != configurationID ||
			stored.Capture.Generation != generation || stored.Capture.Matches(work.Task.Task, work.Item.Item) != nil {
			return empty, errs.New(errs.KindStateConflict, "Blueprint file capture differs from its staged rollout")
		}
		return stored.Capture, nil
	}
	if read.Values[0] != nil {
		return decode(read.Values[0])
	}
	capture, err := ledger.PrepareReleaseWorkConfiguration(ctx, work, configurationID, generation, files)
	if err != nil {
		return empty, err
	}
	value, err := recordcodec.Encode("blueprint-release-configuration", blueprintReleaseConfigurationCapture{
		Rollout: reference, ConfigurationID: configurationID, Capture: capture,
	})
	if err != nil {
		return empty, err
	}
	defer clear(value)
	if len(value) > keyvalue.MaximumBytes {
		return empty, errs.New(errs.KindReleasePlanTooLarge, "Blueprint file capture exceeds its storage budget")
	}
	result, err := ledger.store.Transact(ctx, []keyvalue.Condition{
		{Key: key}, {Key: keys[1], ModRevision: work.Task.Revision}, {Key: keys[2], ModRevision: work.Item.Revision},
		{Key: runtimeconfiguration.HeadKey(work.Task.Task.Scope.EnvironmentID), ModRevision: capture.PriorRevision},
	}, []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return empty, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	if result.Succeeded {
		return capture, nil
	}
	if len(result.FailureReads) != 4 || result.FailureReads[0] == nil {
		return empty, errs.New(
			errs.KindStateConflict,
			"Blueprint file capture raced its owner or acknowledged baseline",
		)
	}
	return decode(result.FailureReads[0])
}
