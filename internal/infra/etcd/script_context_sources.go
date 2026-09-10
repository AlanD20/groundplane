package etcd

import (
	"bytes"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func validateStoredScriptContext(sources ScriptExecutionSources, execution ScriptExecutionRecord) error {
	var snapshot agentpb.ResolvedRunnerSnapshot
	if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil {
		return errs.Wrap(errs.KindValidationFailed, err)
	}
	return validateScriptContextSources(sources, &snapshot)
}

// validateScriptContextSources compares captured machine authority to the actual
// fixed-read Script metadata. Hashes cannot authorize a different desired context.
func validateScriptContextSources(sources ScriptExecutionSources, snapshot *agentpb.ResolvedRunnerSnapshot) error {
	if snapshot == nil {
		return errs.New(errs.KindValidationFailed, "Script runner context snapshot is missing")
	}
	expected, err := scriptContextFromStoredDesired(sources.Script.Record)
	if err != nil {
		return err
	}
	authority := snapshot.ExplicitExecution
	if expected == nil && authority == nil {
		return nil
	}
	if expected == nil || authority == nil || sources.Revision <= 0 || sources.Script.Revision <= 0 ||
		sources.Script.ReadRevision != sources.Revision || authority.ScriptModRevision != uint64(sources.Script.Revision) ||
		!workloadimage.LocalIDValid(snapshot.LocalImageId) ||
		!workloadimage.LocalIDValid(authority.ReleaseLocalImageId) ||
		authority.ReleaseLocalImageId != sources.Release.Intent.CandidateWorkload.LocalImageID ||
		len(authority.ProtoReflect().GetUnknown()) != 0 || !proto.Equal(expected, authority.Context) {
		return errs.New(errs.KindValidationFailed, "captured Script context differs from its fixed source authority")
	}
	digest, err := executionplan.ScriptExecutionContextDigest(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(digest, authority.ContextSha256) {
		return errs.New(errs.KindValidationFailed, "captured Script context digest differs from its fixed source")
	}
	return nil
}

// The storage boundary converts its typed desired record explicitly. Sorting is
// a representation choice for immutable capture; it never mutates authored lists.
func scriptContextFromStoredDesired(record ScriptRecord) (*agentpb.ScriptExplicitExecutionContext, error) {
	execution := record.Desired.Execution
	if execution == nil {
		return nil, nil
	}
	if err := execution.Validate(); err != nil {
		return nil, err
	}
	if execution.Mode == "inherited" {
		return nil, nil
	}
	context := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: execution.Image, User: execution.User,
		EntryIds: append([]string(nil), execution.EntryIDs...),
		Volumes:  make([]*agentpb.ScriptExplicitVolumeGrant, len(execution.Volumes)),
	}
	for index, grant := range execution.Volumes {
		context.Volumes[index] = &agentpb.ScriptExplicitVolumeGrant{
			VolumeId: grant.VolumeID, Target: grant.Target, ReadOnly: grant.ReadOnly,
		}
	}
	sort.Strings(context.EntryIds)
	sort.Slice(context.Volumes, func(left, right int) bool {
		return context.Volumes[left].VolumeId < context.Volumes[right].VolumeId
	})
	return context, nil
}
