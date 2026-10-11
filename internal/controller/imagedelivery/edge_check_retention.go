package imagedelivery

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Checks retain their exact helper image, not every image on the host. A
// transferred historical Task does not independently retain the successor's
// helper; the live cleanup Task owns its own captured image and references.
func (service *Service) retainEdgeCheckImages(
	ctx context.Context,
	retention *imageRetention,
	task etcd.TaskRecord,
	revision int64,
) error {
	keys := []string{edgechecks.Key(task.ID), edgechecks.OperationKey(task.OperationID)}
	read, err := service.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return errs.New(errs.KindInternal, "Tunnel check image authority read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		if taskjournal.IsTerminalTaskStatus(task.Status) && read.Values[1] == nil {
			return nil
		}
		return errs.New(errs.KindStateConflict, "Tunnel check still requires its captured helper image authority")
	}
	record, err := edgechecks.Decode(read.Values[0].Value)
	if err != nil {
		return err
	}
	hash, err := edgechecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != task.ID || record.Owner.PlanID != task.PlanID ||
		record.Owner.OperationID != task.OperationID ||
		record.Owner.EnvironmentID != task.Owner.EnvironmentID ||
		record.Source.Request.RouteID != task.Target ||
		record.RetryOf != task.RetryOf ||
		record.ConfigurationSHA256 != task.Params[taskjournal.TaskEdgeCheckInputParam] ||
		hash != task.PlanHash {
		return errs.New(errs.KindStateConflict, "Tunnel check image authority differs from its Task")
	}
	if record.Phase != edgechecks.Finished && record.Phase != edgechecks.Transferred {
		reason := "Tunnel check helper retained by Task " + task.ID
		retention.retain(record.Source.HelperImageID, reason)
		retention.retain(record.Source.HelperImage, reason)
	}
	return nil
}
