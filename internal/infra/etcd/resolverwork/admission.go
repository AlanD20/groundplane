package resolverwork

import (
	"context"

	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/resolverbaseline"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"google.golang.org/protobuf/proto"
)

// ReadAdmission permits only captured intent against the same applied
// predecessor. New desired DNS input revokes future grants, not cleanup.
func (repository *Repository) ReadAdmission(ctx context.Context, view workqueue.View) (StoredInput, workqueue.Change, bool, error) {
	stored, err := repository.Load(ctx, view)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	input, capture := stored.Input, stored.Capture
	keys := []string{componentrecord.RecordKey(input.ComponentID), AppliedKey(input.HostID, input.ComponentID),
		resolverbaseline.StorageKey, hostresolution.StorageKey, IntentKey(input.HostID, input.ComponentID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{Conditions: append([]keyvalue.Condition(nil), stored.Guards...)}
	for index, key := range keys {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])})
	}
	if read.Values[0] == nil || read.Values[2] == nil || read.Values[3] == nil ||
		read.Values[4] == nil || string(read.Values[4].Value) != input.WorkID ||
		keyvalue.RevisionOf(read.Values[1]) != capture.AppliedRevision {
		return stored, change, false, nil
	}
	component, err := componentrecord.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	actual, err := DesiredDigest(component.Desired)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	expected, err := DesiredDigest(capture.Desired)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	baseline, err := resolverbaseline.Decode(read.Values[2].Value)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	projection, err := hostresolution.DecodeHostResolutionProjectionRecord(read.Values[3].Value)
	if err != nil {
		return StoredInput{}, workqueue.Change{}, false, err
	}
	current := actual == expected && baseline.SHA256 == capture.BaselineSHA256 &&
		read.Values[2].ModRevision == capture.BaselineRevision && projection.InputSHA256 == capture.Projection.InputSHA256
	if prior := read.Values[1]; prior != nil {
		applied, err := DecodeApplied(prior.Value)
		if err != nil {
			return StoredInput{}, workqueue.Change{}, false, err
		}
		if applied.HostID != input.HostID || applied.ComponentID != input.ComponentID || applied.Generation >= input.Generation ||
			(applied.Configuration == nil) != (input.Previous == nil) {
			return stored, change, false, nil
		}
		if input.Previous != nil {
			prior := applied.Configuration
			if prior.PlanID != input.Previous.PlanID || prior.Generation != input.Previous.Generation ||
				!proto.Equal(prior.Runtime, input.Previous.Runtime) || !proto.Equal(prior.Apply, input.Previous.Apply) ||
				!proto.Equal(prior.Observe, input.Previous.Observe) {
				return stored, change, false, nil
			}
		}
	} else if input.Previous != nil || input.Generation != 1 {
		return stored, change, false, nil
	}
	change.Conditions = append(change.Conditions, capture.References...)
	return stored, change, current, nil
}
