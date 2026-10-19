package componentplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Every affected running Service must have native work captured before a
// router may execute. Publication already carries exact comparisons for these
// records, complete Service membership and their acknowledged predecessors.
func validateProxyRolloutPublication(ctx context.Context, store planningStore,
	preflight *ProxyServicePreflight, sourceMutations []keyvalue.Mutation, readRevision int64,
) error {
	if preflight == nil || readRevision <= 0 {
		return errs.New(
			errs.KindStateConflict,
			"Environment proxy change requires captured Service membership and runtime sources",
		)
	}
	var hooks uint64
	for _, source := range preflight.Services {
		if !source.ReplacementRequired {
			continue
		}
		if source.Rollout == nil || source.Rollout.Validate() != nil || preflight.CandidateInput == nil {
			return errs.New(
				errs.KindStateConflict,
				"Environment proxy change requires a captured rollout for every affected running Service",
			)
		}
		reference := *source.Rollout
		keys := []string{ProxyServiceRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, reference.WorkItemID),
			releaserender.ServiceRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, reference.WorkItemID)}
		read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: readRevision})
		if err != nil {
			return err
		}
		if read == nil || read.ReadRevision != readRevision || len(read.Values) != len(keys) {
			if read != nil {
				keyvalue.ClearValues(read.Values)
			}
			return errs.New(errs.KindInternal, "pending proxy rollout publication read is incomplete")
		}
		count, err := validateProxyRolloutPublicationValues(preflight, source, read.Values)
		keyvalue.ClearValues(read.Values)
		if err != nil {
			return err
		}
		hooks += count
	}
	if hooks == 0 {
		return nil
	}
	if preflight.SourceHold == nil || preflight.SourceHold.Validate() != nil ||
		preflight.SourceHold.MembershipCount < hooks {
		return errs.New(
			errs.KindStateConflict,
			"Environment proxy change requires retained hook inputs for all affected Service rollouts",
		)
	}
	key := scriptsourceevidence.ScriptSourceRootKey(preflight.SourceHold.OperationID)
	for _, mutation := range sourceMutations {
		if mutation.Key == key && mutation.Type == keyvalue.MutationPut {
			root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(mutation.Value)
			if err != nil || mutation.Prefix || root.OperationID != preflight.SourceHold.OperationID ||
				root.MembershipCount != preflight.SourceHold.MembershipCount ||
				root.MembershipSHA256 != preflight.SourceHold.MembershipSHA256 ||
				root.Phase != scriptsourceevidence.ScriptOperationSourceActive {
				return errs.New(
					errs.KindStateConflict,
					"Proxy hook source publication differs from captured membership",
				)
			}
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Environment proxy change has no atomic hook-input hold publication")
}

func validateProxyRolloutPublicationValues(preflight *ProxyServicePreflight, source ProxyServiceSource,
	values []*keyvalue.KeyValue,
) (uint64, error) {
	reference := source.Rollout
	if values[0] == nil || values[1] == nil || values[0].ModRevision != reference.Revision ||
		values[1].ModRevision != reference.NativeRevision ||
		values[0].Key != ProxyServiceRolloutKey(
			preflight.EnvironmentID,
			preflight.ParentTaskID,
			reference.WorkItemID,
		) ||
		values[1].Key != releaserender.ServiceRolloutKey(
			preflight.EnvironmentID,
			preflight.ParentTaskID,
			reference.WorkItemID,
		) {
		return 0, errs.New(errs.KindStateConflict, "pending proxy rollout changed before router publication")
	}
	digest := sha256.Sum256(values[0].Value)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return 0, errs.New(errs.KindStateConflict, "pending proxy rollout source digest changed")
	}
	capture, err := releaserender.DecodeServiceRolloutCapture(values[1].Value)
	if err != nil {
		return 0, err
	}
	render, err := releaserender.OpenServiceRolloutCapture(capture)
	if err != nil {
		return 0, err
	}
	if capture.Action != workexecution.ActionApply || capture.WorkItemID != reference.WorkItemID ||
		capture.Scope.EnvironmentID != preflight.EnvironmentID ||
		capture.TaskID != preflight.ParentTaskID ||
		render.ServiceID != reference.ServiceID {
		return 0, errs.New(errs.KindStateConflict, "pending proxy native rollout belongs to another Service or Apply")
	}
	return uint64(len(render.Hooks)), nil
}
