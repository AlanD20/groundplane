package attachremoval

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	releasedomain "github.com/AlanD20/groundplane/internal/core/release"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Membership is the part of a binding that determines whether a consumer
// still needs a network. Credentials and generated facts are not copied here.
type Membership struct {
	AttachID  string                 `json:"attach_id"`
	ServiceID string                 `json:"service_id"`
	NetworkID string                 `json:"network_id"`
	Operation attachrecord.Operation `json:"operation"`
	Detached  bool                   `json:"detached"`
	Revision  int64                  `json:"revision"`
}

// DetachSnapshot captures one complete consumer binding union and its applied
// runtime at the same MVCC revision as the encrypted cleanup source. The epoch
// protects against additions as well as edits/removals after this snapshot.
type DetachSnapshot struct {
	Source                    Input                            `json:"source"`
	Members                   []Membership                     `json:"members"`
	EpochRevision             int64                            `json:"epoch_revision"`
	ServiceRuntimeRevision    int64                            `json:"service_runtime_revision"`
	RuntimeIntent             string                           `json:"runtime_intent"`
	LifecycleTaskID           string                           `json:"lifecycle_task_id"`
	ReleaseProjection         *releasedomain.ServiceProjection `json:"release_projection"`
	ReleaseProjectionRevision int64                            `json:"release_projection_revision"`
	Runtime                   *serviceruntimerecord.Record     `json:"runtime,omitempty"`
	RuntimeRevision           int64                            `json:"runtime_revision"`
}

type DetachReader interface {
	Reader
	Get(context.Context, string) (*keyvalue.GetResult, error)
	Range(context.Context, keyvalue.RangeRequest) (*keyvalue.RangeResult, error)
}

func ReadDetachSnapshot(
	ctx context.Context,
	store DetachReader,
	revision int64,
	attachID string,
) (_ DetachSnapshot, resultErr error) {
	if ctx == nil || store == nil || revision <= 0 || ids.Validate(ids.KindAttach, attachID) != nil {
		return DetachSnapshot{}, errs.New(errs.KindValidationFailed, "Detach requires a fixed resource snapshot")
	}
	source, err := ReadCaptured(ctx, store, revision, attachID)
	if err != nil {
		return DetachSnapshot{}, err
	}
	result := DetachSnapshot{Source: source}
	defer func() {
		if resultErr != nil {
			result.Clear()
		}
	}()
	if err := RequireTerminal(source); err != nil {
		return DetachSnapshot{}, err
	}
	bindings, err := attachrecord.LoadEnvironmentAttachesAtRevision(ctx, store, source.Attach.EnvironmentID, revision)
	if err != nil {
		return DetachSnapshot{}, err
	}
	for _, binding := range bindings {
		record := binding.Record
		result.Members = append(result.Members, Membership{AttachID: record.ID, ServiceID: record.ServiceID,
			NetworkID: record.BackingNetworkID, Operation: record.Operation, Detached: record.Status == "detached", Revision: binding.Revision})
	}
	keys := []string{hierarchy.EnvironmentMutationEpochKey(source.Attach.EnvironmentID),
		services.ServiceRuntimeKey(source.Attach.ServiceID), serviceruntimerecord.Key(source.Attach.ServiceID),
		releases.ReleaseProjectionKey(source.Attach.ServiceID)}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return DetachSnapshot{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[1] == nil {
		return DetachSnapshot{}, errs.New(errs.KindStateConflict, "Detach consumer state is not fully captured")
	}
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			return DetachSnapshot{}, recordcodec.CorruptRecord()
		}
	}
	state, err := services.DecodeServiceRuntimeRecord(read.Values[1].Value)
	if err != nil || state.EnvironmentID != source.Attach.EnvironmentID || state.ServiceID != source.Attach.ServiceID {
		return DetachSnapshot{}, recordcodec.CorruptRecord()
	}
	result.EpochRevision, result.ServiceRuntimeRevision = read.Values[0].ModRevision, read.Values[1].ModRevision
	result.RuntimeIntent = string(state.Runtime.RuntimeIntent)
	result.LifecycleTaskID = state.Runtime.UnconfirmedLifecycleTaskID
	if value := read.Values[3]; value != nil {
		projection, err := releases.DecodeReleaseRecord[releasedomain.ServiceProjection](
			value.Value,
			"service-release-projection",
		)
		if err != nil {
			return DetachSnapshot{}, err
		}
		result.ReleaseProjection, result.ReleaseProjectionRevision = &projection, value.ModRevision
	}
	if value := read.Values[2]; value != nil {
		runtime, err := releases.DecodeReleaseRecord[serviceruntimerecord.Record](
			value.Value,
			"service-acknowledged-runtime",
		)
		if err != nil {
			return DetachSnapshot{}, err
		}
		result.Runtime, result.RuntimeRevision = &runtime, value.ModRevision
	}
	if err := result.Validate(); err != nil {
		return DetachSnapshot{}, err
	}
	return result, nil
}

func (snapshot DetachSnapshot) Validate() error {
	if Validate(snapshot.Source) != nil || RequireTerminal(snapshot.Source) != nil ||
		snapshot.EpochRevision <= 0 || snapshot.EpochRevision > snapshot.Source.SnapshotRevision ||
		snapshot.ServiceRuntimeRevision <= 0 || snapshot.ServiceRuntimeRevision > snapshot.Source.SnapshotRevision ||
		(snapshot.RuntimeIntent != "running" && snapshot.RuntimeIntent != "stopped" && snapshot.RuntimeIntent != "absent") {
		return recordcodec.CorruptRecord()
	}
	if snapshot.ReleaseProjectionRevision < 0 ||
		snapshot.ReleaseProjectionRevision > snapshot.Source.SnapshotRevision ||
		(snapshot.ReleaseProjection == nil) != (snapshot.ReleaseProjectionRevision == 0) {
		return recordcodec.CorruptRecord()
	}
	if projection := snapshot.ReleaseProjection; projection != nil {
		if projection.EnvironmentID != snapshot.Source.Attach.EnvironmentID ||
			projection.ServiceID != snapshot.Source.Attach.ServiceID || projection.Revision == 0 {
			return recordcodec.CorruptRecord()
		}
	}
	if snapshot.Runtime == nil {
		projection := snapshot.ReleaseProjection
		// Running intent is the initial state too. Positive applied or pending
		// execution evidence, not intent alone, requires a captured host runtime.
		if snapshot.RuntimeRevision != 0 || snapshot.LifecycleTaskID != "" ||
			projection != nil &&
				(projection.ServingReleaseID != "" || projection.CurrentSuccessfulReleaseID != "" || projection.ActiveOperationID != "") {
			return errs.New(
				errs.KindStateConflict,
				"Detach requires the Service's acknowledged runtime while deployment or lifecycle effects exist",
			)
		}
	} else if snapshot.RuntimeRevision <= 0 || snapshot.RuntimeRevision > snapshot.Source.SnapshotRevision ||
		serviceruntimerecord.Validate(*snapshot.Runtime) != nil ||
		snapshot.Runtime.EnvironmentID != snapshot.Source.Attach.EnvironmentID || snapshot.Runtime.Runtime.ServiceID != snapshot.Source.Attach.ServiceID {
		return recordcodec.CorruptRecord()
	}
	previous, found := "", false
	for _, member := range snapshot.Members {
		if ids.Validate(ids.KindAttach, member.AttachID) != nil ||
			ids.Validate(ids.KindService, member.ServiceID) != nil ||
			ids.Validate(ids.KindNetwork, member.NetworkID) != nil ||
			strings.Compare(previous, member.AttachID) >= 0 ||
			member.Revision <= 0 ||
			member.Revision > snapshot.Source.SnapshotRevision ||
			(member.Operation != attachrecord.AttachOperationProvision && member.Operation != attachrecord.AttachOperationDetach) {
			return recordcodec.CorruptRecord()
		}
		previous = member.AttachID
		if member.AttachID == snapshot.Source.Attach.ID {
			attach := snapshot.Source.Attach
			if member.ServiceID != attach.ServiceID || member.NetworkID != attach.BackingNetworkID ||
				member.Operation != attach.Operation || member.Detached != (attach.Status == "detached") || member.Revision != snapshot.Source.AttachRevision {
				return recordcodec.CorruptRecord()
			}
			found = true
		}
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Detach target is missing from its captured Environment membership")
	}
	return nil
}

// KeepsConsumerNetwork considers even an unfinished competing Detach as an
// owner until it actually settles. Planning two removals must not silently
// disconnect an edge still owned by another binding.
func (snapshot DetachSnapshot) KeepsConsumerNetwork() bool {
	return snapshot.KeepsConsumerNetworkAfter(nil)
}

func (snapshot DetachSnapshot) ConsumerConditions() []keyvalue.Condition {
	conditions := snapshot.RuntimeConditions()
	return append(conditions, keyvalue.Condition{
		Key: hierarchy.EnvironmentMutationEpochKey(
			snapshot.Source.Attach.EnvironmentID,
		), ModRevision: snapshot.EpochRevision,
	})
}

// RuntimeConditions also protects a metadata-only Detach at settlement: absence
// captured before first Deploy must not silently become a running consumer.
func (snapshot DetachSnapshot) RuntimeConditions() []keyvalue.Condition {
	target := snapshot.Source.Attach
	return []keyvalue.Condition{
		{Key: services.ServiceRuntimeKey(target.ServiceID), ModRevision: snapshot.ServiceRuntimeRevision},
		{Key: serviceruntimerecord.Key(target.ServiceID), ModRevision: snapshot.RuntimeRevision},
		{Key: releases.ReleaseProjectionKey(target.ServiceID), ModRevision: snapshot.ReleaseProjectionRevision},
	}
}

func (snapshot *DetachSnapshot) Clear() {
	if snapshot == nil {
		return
	}
	Clear(&snapshot.Source)
	if snapshot.Runtime != nil {
		clear(snapshot.Runtime.Runtime.CurrentArtifact)
		clear(snapshot.Runtime.Runtime.RetainedPriorArtifact)
	}
}
