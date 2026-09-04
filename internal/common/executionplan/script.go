package executionplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"
)

const (
	ScriptExecutionTimeoutSeconds = 900
	MaximumScriptBodyBytes        = 64 * 1024
)

func validateManualScriptPlan(plan *agentpb.ExecutionPlan) error {
	return validateSingleScriptPlan(plan, true)
}

func validateSingleScriptPlan(plan *agentpb.ExecutionPlan, manual bool) error {
	if validateID(ids.KindScript, plan.TargetId) != nil || len(plan.Artifacts) != 0 ||
		len(plan.ScriptRunnerSnapshots) != 1 || len(plan.ScriptRunnerProjections) != 1 ||
		len(plan.ScriptBodyArtifacts) != 1 || len(plan.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "manual Script execution plan shape is invalid")
	}

	snapshot := plan.ScriptRunnerSnapshots[0]
	projection := plan.ScriptRunnerProjections[0]
	body := plan.ScriptBodyArtifacts[0]
	step := plan.Steps[0]
	if err := validateResolvedRunnerSnapshot(snapshot); err != nil {
		return err
	}
	if manual {
		if snapshot.GetProcedureServiceImage() != nil {
			return errs.New(errs.KindValidationFailed, "manual and non-post-deploy Script image authority must be pinned")
		}
		if err := validateManualScriptSourceAuthorities(snapshot); err != nil {
			return err
		}
	}
	if err := validateScriptRunnerProjection(projection); err != nil {
		return err
	}
	if err := validateScriptBodyArtifact(body); err != nil {
		return err
	}
	if step == nil || validateID(ids.KindStep, step.StepId) != nil ||
		step.TimeoutSeconds != ScriptExecutionTimeoutSeconds {
		return errs.New(errs.KindValidationFailed, "Script execution step identity or timeout is invalid")
	}
	run := step.GetRunScript()
	if run == nil || run.ScriptId != plan.TargetId || run.RenderGeneration != plan.RenderGeneration ||
		!validRawULID(run.ScriptExecutionId) || run.ScriptGeneration == 0 ||
		validateID(ids.KindEnvironment, run.EnvironmentId) != nil ||
		validateID(ids.KindService, run.ServiceId) != nil ||
		validateID(ids.KindDeployment, run.ReleaseId) != nil ||
		len(run.ServiceDefinitionSha256) != sha256.Size || len(run.BodySha256) != sha256.Size ||
		!validRawULID(run.RunnerSnapshotId) || len(run.RunnerSnapshotSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "RunScript command is invalid")
	}
	if snapshot.SnapshotId != run.RunnerSnapshotId || snapshot.ScriptExecutionId != run.ScriptExecutionId ||
		snapshot.EnvironmentId != run.EnvironmentId || snapshot.ServiceId != run.ServiceId ||
		snapshot.ReleaseId != run.ReleaseId || snapshot.RenderGeneration != run.RenderGeneration ||
		!bytes.Equal(snapshot.ServiceDefinitionSha256, run.ServiceDefinitionSha256) ||
		projection.SnapshotId != snapshot.SnapshotId || body.ScriptExecutionId != run.ScriptExecutionId ||
		body.ScriptId != run.ScriptId || body.Generation != run.ScriptGeneration ||
		!bytes.Equal(body.Sha256, run.BodySha256) || body.Uid != projection.Uid || body.Gid != projection.Gid {
		return errs.New(errs.KindValidationFailed, "Script execution plan references do not form one closed execution")
	}
	if !scriptSnapshotImageMatchesProjection(snapshot, projection) ||
		!equalScriptNetworks(snapshot.Networks, projection.Networks) ||
		!equalScriptMounts(snapshot.Mounts, projection.Mounts) ||
		!equalScriptEntryBindings(snapshot.EntryBindings, projection.EntryBindings) {
		return errs.New(errs.KindValidationFailed, "Script runner projection differs from its resolved snapshot")
	}
	if projection.Name != "gp-script-"+strings.ToLower(run.ScriptExecutionId) ||
		validateScriptOwnershipLabels(plan, run, snapshot, projection) != nil {
		return errs.New(errs.KindValidationFailed, "Script runner ownership labels are invalid")
	}
	projectionDigest, err := scriptMessageDigest(projection)
	if err != nil {
		return err
	}
	if !bytes.Equal(snapshot.RunnerProjectionSha256, projectionDigest) {
		return errs.New(errs.KindValidationFailed, "Script runner projection digest does not match its snapshot")
	}
	snapshotDigest, err := scriptMessageDigest(snapshot)
	if err != nil {
		return err
	}
	if !bytes.Equal(run.RunnerSnapshotSha256, snapshotDigest) {
		return errs.New(errs.KindValidationFailed, "Script runner snapshot digest does not match RunScript")
	}
	return nil
}

func scriptSnapshotImageMatchesProjection(
	snapshot *agentpb.ResolvedRunnerSnapshot,
	projection *agentpb.ScriptRunnerProjection,
) bool {
	if authority := snapshot.GetProcedureServiceImage(); authority != nil {
		return snapshot.GetImageReference() == "" && len(snapshot.GetImageDigest()) == 0 &&
			projection.GetImage() == authority.GetRequestedReference()
	}
	return snapshot.GetImageReference() == projection.GetImage()
}

func validateScriptOwnershipLabels(
	plan *agentpb.ExecutionPlan,
	run *agentpb.RunScript,
	snapshot *agentpb.ResolvedRunnerSnapshot,
	projection *agentpb.ScriptRunnerProjection,
) error {
	if len(projection.Labels) != 13 {
		return errs.New(errs.KindValidationFailed, "Script runner ownership label count is invalid")
	}
	labels := make(map[string]string, len(projection.Labels))
	for _, label := range projection.Labels {
		labels[label.Key] = label.Value
	}
	expected := map[string]string{
		"com.groundplane.managed":             "true",
		"com.groundplane.kind":                "script-runner",
		"com.groundplane.tenant-id":           snapshot.TenantId,
		"com.groundplane.project-id":          snapshot.ProjectId,
		"com.groundplane.environment-id":      run.EnvironmentId,
		"com.groundplane.service-id":          run.ServiceId,
		"com.groundplane.script-id":           run.ScriptId,
		"com.groundplane.script-generation":   strconv.FormatUint(run.ScriptGeneration, 10),
		"com.groundplane.script-execution-id": run.ScriptExecutionId,
		"com.groundplane.release-id":          run.ReleaseId,
		"com.groundplane.plan-id":             plan.PlanId,
		"com.groundplane.render-generation":   strconv.FormatUint(run.RenderGeneration, 10),
	}
	for key, value := range expected {
		if labels[key] != value {
			return errs.New(errs.KindValidationFailed, "Script runner ownership label value is invalid")
		}
		delete(labels, key)
	}
	operationID := labels["com.groundplane.operation-id"]
	if len(labels) != 1 || ids.Validate(ids.KindOperation, operationID) != nil {
		return errs.New(errs.KindValidationFailed, "Script runner operation label is invalid")
	}
	return nil
}

func validateResolvedRunnerSnapshot(snapshot *agentpb.ResolvedRunnerSnapshot) error {
	if snapshot == nil || !validRawULID(snapshot.SnapshotId) || !validRawULID(snapshot.ScriptExecutionId) ||
		validateID(ids.KindTenant, snapshot.TenantId) != nil || snapshot.TenantModRevision == 0 ||
		validateID(ids.KindProject, snapshot.ProjectId) != nil || snapshot.ProjectModRevision == 0 ||
		validateID(ids.KindEnvironment, snapshot.EnvironmentId) != nil || snapshot.EnvironmentModRevision == 0 ||
		validateID(ids.KindService, snapshot.ServiceId) != nil ||
		len(snapshot.ServiceDefinitionSha256) != sha256.Size ||
		validateID(ids.KindDeployment, snapshot.ReleaseId) != nil || snapshot.ReleaseModRevision == 0 ||
		validateID(ids.KindTask, snapshot.BlueprintBundleGeneration) != nil || snapshot.RenderGeneration == 0 ||
		len(snapshot.RunnerProjectionSha256) != sha256.Size ||
		validateID(ids.KindTask, snapshot.AppliedEnvironmentRevisionId) != nil ||
		snapshot.AppliedEnvironmentRenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "resolved Script runner snapshot identity is invalid")
	}
	if err := validateScriptImageAuthority(snapshot); err != nil {
		return err
	}
	var stagedClaim *agentpb.ScriptStagedSourceAuthority
	if err := validateScriptSourceAuthority(snapshot.ServiceSource, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &stagedClaim); err != nil {
		return err
	}
	if err := validateScriptSourceAuthority(snapshot.NetworkTopologySource, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &stagedClaim); err != nil {
		return err
	}
	if err := validateScriptSourceAuthority(snapshot.AppliedEnvironmentSource, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &stagedClaim); err != nil {
		return err
	}
	if snapshot.GetProcedureServiceImage() == nil {
		separator := strings.LastIndex(snapshot.ImageReference, "@sha256:")
		if separator < 0 {
			return errs.New(errs.KindValidationFailed, "resolved Script runner image is not digest pinned")
		}
		imageDigest, err := hex.DecodeString(snapshot.ImageReference[separator+8:])
		if err != nil || !bytes.Equal(imageDigest, snapshot.ImageDigest) {
			return errs.New(errs.KindValidationFailed, "resolved Script runner image digest does not match")
		}
	}
	if err := validateScriptNetworks(snapshot.Networks, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &stagedClaim); err != nil {
		return err
	}
	if err := validateScriptMounts(snapshot.Mounts, snapshot.EnvironmentId,
		snapshot.AppliedEnvironmentRevisionId, snapshot.AppliedEnvironmentRenderGeneration, &stagedClaim); err != nil {
		return err
	}
	if err := validateScriptEntryBindings(snapshot.EntryBindings); err != nil {
		return err
	}
	return validateScriptSecretValues(snapshot.SecretValues)
}

func validateScriptImageAuthority(snapshot *agentpb.ResolvedRunnerSnapshot) error {
	pinned := snapshot.GetImageReference() != "" || len(snapshot.GetImageDigest()) != 0
	procedure := snapshot.GetProcedureServiceImage()
	if pinned == (procedure != nil) {
		return errs.New(errs.KindValidationFailed, "resolved Script runner image authority is not an exact alternative")
	}
	if pinned {
		if !imageref.IsDigestPinned(snapshot.GetImageReference()) || len(snapshot.GetImageDigest()) != sha256.Size {
			return errs.New(errs.KindValidationFailed, "resolved Script runner pinned image authority is invalid")
		}
		return nil
	}
	if validateID(ids.KindStep, procedure.GetComposeApplyStepId()) != nil ||
		validateID(ids.KindConfig, procedure.GetArtifactId()) != nil ||
		procedure.GetServiceId() != snapshot.GetServiceId() || procedure.GetReleaseId() != snapshot.GetReleaseId() ||
		!validRequestedImageReference(procedure.GetRequestedReference()) {
		return errs.New(errs.KindValidationFailed, "resolved Script runner procedure-service image authority is invalid")
	}
	return nil
}

func validateScriptRunnerProjection(projection *agentpb.ScriptRunnerProjection) error {
	if projection == nil || !validRawULID(projection.SnapshotId) || !validScriptString(projection.Name) ||
		!validRequestedImageReference(projection.Image) || projection.StopGraceSeconds != 10 ||
		len(projection.Entrypoint) != 1 || projection.Entrypoint[0] != "/bin/sh" ||
		len(projection.Command) != 1 || projection.Command[0] != "/groundplane-script-body" {
		return errs.New(errs.KindValidationFailed, "Script runner projection identity or forced process is invalid")
	}
	if err := validateScriptPairs(projection.Environment, false); err != nil {
		return err
	}
	if err := validateScriptPairs(projection.Sysctls, false); err != nil {
		return err
	}
	if err := validateScriptPairs(projection.StorageOpt, false); err != nil {
		return err
	}
	if err := validateScriptPairs(projection.Labels, false); err != nil {
		return err
	}
	for _, values := range [][]string{
		projection.Dns, projection.DnsSearch, projection.DnsOpt, projection.ExtraHosts,
		projection.Tmpfs, projection.CapDrop, projection.SecurityOpt, projection.GroupAdd,
	} {
		if !allValidScriptStrings(values) {
			return errs.New(errs.KindValidationFailed, "Script runner projection contains invalid values")
		}
	}
	if err := validateScriptUlimits(projection.Ulimits); err != nil {
		return err
	}
	if err := validateScriptBlkio(projection.Blkio); err != nil {
		return err
	}
	if err := validateScriptResource(projection.Limits); err != nil {
		return err
	}
	if err := validateScriptResource(projection.Reservations); err != nil {
		return err
	}
	if err := validateScriptMounts(projection.Mounts, "", "", 0, nil); err != nil {
		return err
	}
	if err := validateScriptEntryBindings(projection.EntryBindings); err != nil {
		return err
	}
	return validateScriptNetworks(projection.Networks, "", "", 0, nil)
}

func validateScriptBodyArtifact(body *agentpb.ScriptBodyArtifactMetadata) error {
	if body == nil || !validRawULID(body.ScriptExecutionId) || validateID(ids.KindScript, body.ScriptId) != nil ||
		body.Generation == 0 || body.Size == 0 || body.Size > MaximumScriptBodyBytes || len(body.Sha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Script body artifact metadata is invalid")
	}
	return nil
}

func validateScriptNetworks(
	networks []*agentpb.ScriptRunnerNetwork,
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	stagedClaim **agentpb.ScriptStagedSourceAuthority,
) error {
	previous := ""
	for _, network := range networks {
		if network == nil {
			return errs.New(errs.KindValidationFailed, "Script runner network is invalid or unsorted")
		}
		dockerName, err := networkname.New(network.NetworkId)
		if err != nil ||
			network.NetworkId <= previous || network.RenderedAttachment == nil ||
			network.RenderedAttachment.DockerNetworkName != dockerName ||
			!validScriptString(network.RenderedAttachment.InterfaceName) {
			return errs.New(errs.KindValidationFailed, "Script runner network is invalid or unsorted")
		}
		if err := validateScriptSourceAuthority(network.Source, environmentID, revisionID, renderGeneration, stagedClaim); err != nil {
			return err
		}
		if err := validateScriptPairs(network.RenderedAttachment.DriverOptions, false); err != nil {
			return err
		}
		previous = network.NetworkId
	}
	return nil
}

func validateScriptMounts(
	mounts []*agentpb.ScriptRunnerMount,
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	stagedClaim **agentpb.ScriptStagedSourceAuthority,
) error {
	previous := ""
	for _, mount := range mounts {
		key := ""
		if mount != nil && mount.RenderedMount != nil {
			key = mount.SourceId + "\x00" + mount.RenderedMount.Target
		}
		if mount == nil || validateAnyStableID(mount.SourceId) != nil ||
			key <= previous || mount.RenderedMount == nil ||
			!validScriptMount(mount.RenderedMount) {
			return errs.New(errs.KindValidationFailed, "Script runner mount is invalid or unsorted")
		}
		if err := validateScriptSourceAuthority(mount.Source, environmentID, revisionID, renderGeneration, stagedClaim); err != nil {
			return err
		}
		previous = key
	}
	return nil
}

func validateScriptSourceAuthority(
	authority *agentpb.ScriptSourceAuthority,
	environmentID string,
	revisionID string,
	renderGeneration uint64,
	stagedClaim **agentpb.ScriptStagedSourceAuthority,
) error {
	if authority == nil || (authority.Existing == nil) == (authority.Staged == nil) {
		return errs.New(errs.KindValidationFailed, "Script source authority must contain exactly one evidence kind")
	}
	if authority.Existing != nil {
		if authority.Existing.ModRevision == 0 {
			return errs.New(errs.KindValidationFailed, "existing Script source authority is invalid")
		}
		return nil
	}
	staged := authority.Staged
	if validateID(ids.KindEnvironment, staged.EnvironmentId) != nil ||
		validateID(ids.KindTask, staged.RevisionId) != nil || staged.RenderGeneration == 0 ||
		staged.FixedReadRevision == 0 || len(staged.CanonicalValueSha256) != sha256.Size ||
		(environmentID != "" && staged.EnvironmentId != environmentID) ||
		(revisionID != "" && staged.RevisionId != revisionID) ||
		(renderGeneration != 0 && staged.RenderGeneration != renderGeneration) {
		return errs.New(errs.KindValidationFailed, "staged Script source authority is invalid")
	}
	if stagedClaim != nil {
		if *stagedClaim == nil {
			*stagedClaim = staged
		} else if (*stagedClaim).EnvironmentId != staged.EnvironmentId ||
			(*stagedClaim).RevisionId != staged.RevisionId ||
			(*stagedClaim).RenderGeneration != staged.RenderGeneration ||
			(*stagedClaim).FixedReadRevision != staged.FixedReadRevision {
			return errs.New(errs.KindValidationFailed, "staged Script sources do not share one candidate Environment stage")
		}
	}
	return nil
}

func validateManualScriptSourceAuthorities(snapshot *agentpb.ResolvedRunnerSnapshot) error {
	authorities := []*agentpb.ScriptSourceAuthority{
		snapshot.ServiceSource, snapshot.NetworkTopologySource, snapshot.AppliedEnvironmentSource,
	}
	for _, network := range snapshot.Networks {
		authorities = append(authorities, network.Source)
	}
	for _, mount := range snapshot.Mounts {
		authorities = append(authorities, mount.Source)
	}
	for _, authority := range authorities {
		if authority == nil || authority.Existing == nil || authority.Staged != nil {
			return errs.New(errs.KindValidationFailed, "manual Script source authority must be existing")
		}
	}
	return nil
}

func validScriptMount(mount *agentpb.ScriptMount) bool {
	if mount.Type != "bind" && mount.Type != "volume" && mount.Type != "tmpfs" {
		return false
	}
	return validScriptString(mount.Source) && validAbsoluteScriptPath(mount.Target) &&
		validScriptString(mount.Consistency) && validScriptString(mount.BindSelinux) &&
		validScriptString(mount.BindPropagation) && validScriptString(mount.BindRecursive) &&
		validScriptString(mount.VolumeSubpath) && validScriptString(mount.ConfigUid) &&
		validScriptString(mount.ConfigGid)
}

func validateScriptEntryBindings(values []*agentpb.ScriptRunnerEntryBinding) error {
	previous := ""
	environmentKeys := make(map[string]struct{}, len(values))
	fileTargets := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == nil || validateID(ids.KindEnvEntry, value.EntryId) != nil ||
			validateID(ids.KindConfig, value.ValueGenerationId) != nil || len(value.Sha256) != sha256.Size ||
			value.EntryId <= previous {
			return errs.New(errs.KindValidationFailed, "Script runner Entry binding is invalid or unsorted")
		}
		switch value.Kind {
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_ENV:
			if value.EnvironmentKey == "" || !validScriptString(value.EnvironmentKey) ||
				strings.Contains(value.EnvironmentKey, "=") || value.FileTarget != "" ||
				value.Uid != 0 || value.Gid != 0 || value.Mode != 0 {
				return errs.New(errs.KindValidationFailed, "Script runner Environment Entry binding is invalid")
			}
			if _, duplicate := environmentKeys[value.EnvironmentKey]; duplicate {
				return errs.New(errs.KindValidationFailed, "Script runner Environment Entry key is duplicated")
			}
			environmentKeys[value.EnvironmentKey] = struct{}{}
		case agentpb.ScriptEntryBindingKind_SCRIPT_ENTRY_BINDING_KIND_FILE:
			expectedMode := uint32(0o444)
			if value.Secret {
				expectedMode = 0o600
			}
			if value.EnvironmentKey != "" || !validAbsoluteScriptPath(value.FileTarget) ||
				value.FileTarget == "/groundplane-script-body" || value.Mode != expectedMode {
				return errs.New(errs.KindValidationFailed, "Script runner file Entry binding is invalid")
			}
			if _, duplicate := fileTargets[value.FileTarget]; duplicate {
				return errs.New(errs.KindValidationFailed, "Script runner file Entry target is duplicated")
			}
			fileTargets[value.FileTarget] = struct{}{}
		default:
			return errs.New(errs.KindValidationFailed, "Script runner Entry binding kind is invalid")
		}
		previous = value.EntryId
	}
	return nil
}

func equalScriptEntryBindings(left, right []*agentpb.ScriptRunnerEntryBinding) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func validateScriptSecretValues(values []*agentpb.ScriptRunnerSecretValue) error {
	previous := ""
	for _, value := range values {
		if value == nil || validateAnyStableID(value.OwnerId) != nil ||
			validateAnyStableID(value.ValueGenerationId) != nil || len(value.Digest) != sha256.Size ||
			value.OwnerId <= previous {
			return errs.New(errs.KindValidationFailed, "Script runner secret value is invalid or unsorted")
		}
		previous = value.OwnerId
	}
	return nil
}

func validateScriptPairs(values []*agentpb.ScriptStringPair, allowEmptyValue bool) error {
	previous := ""
	for _, value := range values {
		if value == nil || value.Key == "" || !validScriptString(value.Key) || value.Key <= previous ||
			(!allowEmptyValue && !validScriptString(value.Value)) {
			return errs.New(errs.KindValidationFailed, "Script runner key/value table is invalid or unsorted")
		}
		previous = value.Key
	}
	return nil
}

func validateScriptUlimits(values []*agentpb.ScriptUlimit) error {
	previous := ""
	for _, value := range values {
		if value == nil || !validScriptString(value.Name) || value.Name <= previous ||
			(value.Single == 0 && value.Soft == 0 && value.Hard == 0) {
			return errs.New(errs.KindValidationFailed, "Script runner ulimit table is invalid or unsorted")
		}
		previous = value.Name
	}
	return nil
}

func validateScriptBlkio(value *agentpb.ScriptBlkio) error {
	if value == nil {
		return nil
	}
	if !validWeightDevices(value.WeightDevices) || !validThrottleDevices(value.DeviceReadBps) ||
		!validThrottleDevices(value.DeviceReadIops) || !validThrottleDevices(value.DeviceWriteBps) ||
		!validThrottleDevices(value.DeviceWriteIops) {
		return errs.New(errs.KindValidationFailed, "Script runner blkio policy is invalid or unsorted")
	}
	return nil
}

func validWeightDevices(values []*agentpb.ScriptWeightDevice) bool {
	previous := ""
	for _, value := range values {
		if value == nil || !validAbsoluteScriptPath(value.Path) || value.Weight == 0 || value.Path <= previous {
			return false
		}
		previous = value.Path
	}
	return true
}

func validThrottleDevices(values []*agentpb.ScriptThrottleDevice) bool {
	previous := ""
	for _, value := range values {
		if value == nil || !validAbsoluteScriptPath(value.Path) || value.Rate <= 0 || value.Path <= previous {
			return false
		}
		previous = value.Path
	}
	return true
}

func validateScriptResource(value *agentpb.ScriptResource) error {
	if value == nil {
		return nil
	}
	if !value.Present || value.Cpus < 0 || value.MemoryBytes < 0 || value.Pids < 0 {
		return errs.New(errs.KindValidationFailed, "Script runner deploy resource is invalid")
	}
	return nil
}

func allValidScriptStrings(values []string) bool {
	for _, value := range values {
		if !validScriptString(value) {
			return false
		}
	}
	return true
}

func equalScriptNetworks(left, right []*agentpb.ScriptRunnerNetwork) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func equalScriptMounts(left, right []*agentpb.ScriptRunnerMount) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !proto.Equal(left[index], right[index]) {
			return false
		}
	}
	return true
}

func validAbsoluteScriptPath(value string) bool {
	return validScriptString(value) && path.IsAbs(value) && path.Clean(value) == value
}

func validScriptString(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func validRawULID(value string) bool {
	if len(value) != 26 || value != strings.ToUpper(value) {
		return false
	}
	_, err := ulid.ParseStrict(value)
	return err == nil
}

func scriptMessageDigest(message proto.Message) ([]byte, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return append([]byte(nil), digest[:]...), nil
}
