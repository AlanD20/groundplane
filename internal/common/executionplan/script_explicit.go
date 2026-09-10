package executionplan

import (
	"bytes"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/scriptpolicy"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ScriptExecutionContextDigest validates and hashes the canonical captured input.
// Source ownership and revisions are independently checked before publication.
func ScriptExecutionContextDigest(context *agentpb.ScriptExplicitExecutionContext) ([]byte, error) {
	if context == nil || !imageref.IsDigestPinned(context.ImageReference) {
		return nil, errs.New(errs.KindValidationFailed, "explicit Script image must be a repository digest")
	}
	if _, _, err := scriptpolicy.NumericUser(context.User); err != nil {
		return nil, err
	}
	if err := rejectUnknown(context.ProtoReflect()); err != nil {
		return nil, err
	}
	if err := validateExplicitScriptGrants(context); err != nil {
		return nil, err
	}
	return scriptMessageDigest(context)
}

func validateExplicitScriptGrants(context *agentpb.ScriptExplicitExecutionContext) error {
	if len(context.Volumes) > scriptpolicy.MaximumVolumes || len(context.EntryIds) > scriptpolicy.MaximumEntries {
		return errs.New(errs.KindValidationFailed, "explicit Script grants exceed the supported bounds")
	}
	previousID := ""
	for index, grant := range context.Volumes {
		if grant == nil || ids.Validate(ids.KindVolume, grant.VolumeId) != nil || grant.VolumeId <= previousID {
			return errs.New(errs.KindValidationFailed, "explicit Script Volume grants are invalid or unsorted")
		}
		if err := scriptpolicy.ValidateMountTarget(grant.Target); err != nil {
			return err
		}
		for _, previous := range context.Volumes[:index] {
			if scriptpolicy.PathsOverlap(grant.Target, previous.Target) {
				return errs.New(errs.KindValidationFailed, "explicit Script Volume targets overlap")
			}
		}
		previousID = grant.VolumeId
	}
	previousID = ""
	for _, entryID := range context.EntryIds {
		if ids.Validate(ids.KindEnvEntry, entryID) != nil || entryID <= previousID {
			return errs.New(errs.KindValidationFailed, "explicit Script Entry grants are invalid or unsorted")
		}
		previousID = entryID
	}
	return nil
}

func validateExplicitScriptAuthority(snapshot *agentpb.ResolvedRunnerSnapshot) error {
	authority := snapshot.ExplicitExecution
	if authority == nil {
		return nil
	}
	if authority.ScriptModRevision == 0 || !workloadimage.LocalIDValid(authority.ReleaseLocalImageId) ||
		len(snapshot.Networks) != 0 || len(snapshot.SecretValues) != 0 {
		return errs.New(errs.KindValidationFailed, "explicit Script source authority or isolation is invalid")
	}
	digest, err := ScriptExecutionContextDigest(authority.Context)
	if err != nil {
		return err
	}
	if !bytes.Equal(digest, authority.ContextSha256) {
		return errs.New(errs.KindValidationFailed, "explicit Script context digest does not match")
	}
	return validateExplicitScriptResources(snapshot, authority.Context)
}

func validateExplicitScriptResources(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	context *agentpb.ScriptExplicitExecutionContext,
) error {
	if len(snapshot.Mounts) != len(context.Volumes) || len(snapshot.EntryBindings) != len(context.EntryIds) {
		return errs.New(errs.KindValidationFailed, "explicit Script resources differ from the captured grants")
	}
	for index, grant := range context.Volumes {
		mount := snapshot.Mounts[index]
		expected := &agentpb.ScriptMount{
			Type: "volume", Source: "gp_vol_" + strings.ToLower(grant.VolumeId), Target: grant.Target,
			ReadOnly: grant.ReadOnly, VolumeNoCopy: true,
		}
		if mount == nil || mount.SourceId != grant.VolumeId || !proto.Equal(mount.RenderedMount, expected) {
			return errs.New(errs.KindValidationFailed, "explicit Script mount differs from its Volume grant")
		}
	}
	fileTargets := make([]string, 0, len(snapshot.EntryBindings))
	for index, binding := range snapshot.EntryBindings {
		if binding == nil || binding.EntryId != context.EntryIds[index] {
			return errs.New(errs.KindValidationFailed, "explicit Script binding differs from its Entry grant")
		}
		if binding.Kind != agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE {
			continue
		}
		if err := scriptpolicy.ValidateMountTarget(binding.FileTarget); err != nil {
			return err
		}
		for _, grant := range context.Volumes {
			if scriptpolicy.PathsOverlap(binding.FileTarget, grant.Target) {
				return errs.New(errs.KindValidationFailed, "explicit Script Entry file overlaps a Volume target")
			}
		}
		for _, previous := range fileTargets {
			if scriptpolicy.PathsOverlap(binding.FileTarget, previous) {
				return errs.New(errs.KindValidationFailed, "explicit Script Entry file targets overlap")
			}
		}
		fileTargets = append(fileTargets, binding.FileTarget)
	}
	return nil
}

func explicitScriptProjectionMatches(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	projection *agentpb.ScriptRunnerProjection,
) bool {
	if snapshot.ExplicitExecution == nil {
		return true
	}
	uid, gid, err := scriptpolicy.NumericUser(snapshot.ExplicitExecution.GetContext().GetUser())
	if err != nil {
		return false
	}
	// Build the complete allowed projection. New ambient fields remain absent
	// automatically, rather than relying on an ever-growing rejection list.
	expected := &agentpb.ScriptRunnerProjection{
		SnapshotId: snapshot.SnapshotId, Name: projection.Name, Image: snapshot.LocalImageId,
		Uid: uid, Gid: gid, WorkingDir: "/", Mounts: snapshot.Mounts, EntryBindings: snapshot.EntryBindings,
		Labels: projection.Labels, Entrypoint: []string{"/bin/sh"}, Command: []string{scriptpolicy.BodyTarget},
		StopGraceSeconds: 10,
	}
	return proto.Equal(expected, projection)
}

func scriptReleaseLocalImageID(snapshot *agentpb.ResolvedRunnerSnapshot) string {
	if snapshot.GetExplicitExecution() != nil {
		return snapshot.ExplicitExecution.ReleaseLocalImageId
	}
	return snapshot.GetLocalImageId()
}
