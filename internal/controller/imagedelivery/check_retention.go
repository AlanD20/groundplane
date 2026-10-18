package imagedelivery

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Retain from the operation's durable cleanup authority, not diagnostic Task
// history. The caller's reader fixes both scans to the image-removal snapshot.
func retainCheckImages(retention *imageRetention, read func(string, func(keyvalue.KeyValue) error) error) error {
	if err := read(backingchecks.RecordPrefix, func(value keyvalue.KeyValue) error {
		record, err := backingchecks.Decode(value.Value)
		if err != nil {
			return err
		}
		defer record.Clear()
		if value.Key != backingchecks.Key(record.Owner.TaskID) {
			return errs.New(errs.KindInternal, "Backing check image ownership key is invalid")
		}
		if record.Phase != backingchecks.Finished {
			reason := "Access check helper retained by Task " + record.Owner.TaskID
			retention.retain(record.Source.HelperImageID, reason)
			retention.retain(record.Source.HelperImage, reason)
		}
		return nil
	}); err != nil {
		return err
	}
	return read(edgechecks.RecordPrefix, func(value keyvalue.KeyValue) error {
		record, err := edgechecks.Decode(value.Value)
		if err != nil {
			return err
		}
		if value.Key != edgechecks.Key(record.Owner.TaskID) {
			return errs.New(errs.KindInternal, "Tunnel check image ownership key is invalid")
		}
		if record.Phase != edgechecks.Finished {
			reason := "Tunnel check helper retained by Task " + record.Owner.TaskID
			retention.retain(record.Source.HelperImageID, reason)
			retention.retain(record.Source.HelperImage, reason)
		}
		return nil
	})
}
