package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type ManualScriptPlanInput struct {
	TaskID        string
	OperationID   string
	PlanID        string
	StepID        string
	ExecutionID   string
	SnapshotID    string
	Sources       etcd.ScriptExecutionSources
	EntryBindings []*agentpb.ScriptRunnerEntryBinding
}

// BuildManualScriptPlan projects one frozen successful Release into a closed
// one-off runner definition. It does not read current desired state or host
// runtime state and never places Script body bytes in the plan.
func BuildManualScriptPlan(
	ctx context.Context,
	input ManualScriptPlanInput,
) (*agentpb.ExecutionPlan, error) {
	if ctx == nil || ids.Validate(ids.KindTask, input.TaskID) != nil ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(ids.KindPlan, input.PlanID) != nil || ids.Validate(ids.KindStep, input.StepID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "manual Script plan identity is invalid")
	}
	sources := input.Sources
	if sources.RenderInput.Record.Image != sources.Release.Intent.Image ||
		!imageref.IsDigestPinned(sources.Release.Intent.Image) ||
		sources.RenderInput.Record.Projection.RevisionID == "" ||
		sources.RenderInput.Record.Projection.RenderGeneration == 0 {
		return nil, errs.New(errs.KindStateConflict, "successful Release cannot authorize a Script runner")
	}
	project, err := loadNormalizedEnvironmentProject(ctx, sources.RenderInput.Record.Projection)
	if err != nil {
		return nil, err
	}
	service, err := project.GetService(sources.RenderInput.Record.ServiceName)
	if err != nil || service.Name != sources.Service.Record.Desired.Name {
		return nil, errs.New(errs.KindStateConflict, "successful Release service definition is missing")
	}
	appliedProject, err := loadNormalizedEnvironmentProject(ctx, sources.AppliedProjection.Record)
	if err != nil {
		return nil, err
	}
	appliedService, err := appliedProject.GetService(sources.RenderInput.Record.ServiceName)
	if err != nil || appliedService.Name != sources.Service.Record.Desired.Name {
		return nil, errs.New(errs.KindStateConflict, "applied Environment service topology is missing")
	}
	service.Image = sources.Release.Intent.Image
	service.Networks = appliedService.Networks
	if err := validateScriptServiceDisposition(service); err != nil {
		return nil, err
	}
	serviceDefinition, err := json.Marshal(service)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	serviceDigest := sha256.Sum256(serviceDefinition)
	clear(serviceDefinition)

	uid, gid, err := parseScriptNumericUser(service.User)
	if err != nil {
		return nil, err
	}
	networks, err := projectScriptNetworks(service, sources)
	if err != nil {
		return nil, err
	}
	mounts, err := projectScriptMounts(appliedService, sources)
	if err != nil {
		return nil, err
	}
	projection, err := projectScriptRunner(input, service, uid, gid, networks, mounts, input.EntryBindings)
	if err != nil {
		return nil, err
	}
	projectionDigest, err := deterministicScriptProtoDigest(projection)
	if err != nil {
		return nil, err
	}
	imageDigest, err := digestPinnedImageBytes(service.Image)
	if err != nil {
		return nil, err
	}
	snapshot := &agentpb.ResolvedRunnerSnapshot{
		SnapshotId: input.SnapshotID, ScriptExecutionId: input.ExecutionID,
		TenantId: sources.Tenant.Record.ID, TenantModRevision: uint64(sources.Tenant.Revision),
		ProjectId: sources.Project.Record.ID, ProjectModRevision: uint64(sources.Project.Revision),
		EnvironmentId: sources.Environment.Record.ID, EnvironmentModRevision: uint64(sources.Environment.Revision),
		ServiceId: sources.Service.Record.Desired.ID, ServiceModRevision: uint64(sources.Service.Revision),
		ServiceDefinitionSha256: serviceDigest[:],
		ReleaseId:               sources.Release.Intent.ID, ReleaseModRevision: uint64(sources.Release.IntentRevision),
		ImageReference: service.Image, ImageDigest: imageDigest,
		BlueprintBundleGeneration: sources.RenderInput.Record.Projection.RevisionID,
		RenderGeneration:          sources.RenderInput.Record.Projection.RenderGeneration,
		NetworkTopologyRevision:   uint64(sources.AppliedProjection.Revision),
		Networks:                  cloneScriptNetworks(networks), Mounts: cloneScriptMounts(mounts),
		AppliedEnvironmentRevisionId:       sources.AppliedProjection.Record.RevisionID,
		AppliedEnvironmentRenderGeneration: sources.AppliedProjection.Record.RenderGeneration,
		AppliedEnvironmentModRevision:      uint64(sources.AppliedProjection.Revision),
		EntryBindings:                      cloneScriptEntryBindings(input.EntryBindings),
		RunnerProjectionSha256:             projectionDigest,
	}
	snapshotDigest, err := deterministicScriptProtoDigest(snapshot)
	if err != nil {
		return nil, err
	}
	bodyDigest, err := hex.DecodeString(sources.BodyGeneration.Record.BodySHA256)
	if err != nil || len(bodyDigest) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "Script body generation digest is corrupt")
	}
	plan := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: input.PlanID,
		RenderGeneration:        sources.RenderInput.Record.Projection.RenderGeneration,
		Operation:               agentpb.PlanOperation_PLAN_OPERATION_SCRIPT,
		TargetId:                sources.Script.Record.Desired.ID,
		ScriptRunnerSnapshots:   []*agentpb.ResolvedRunnerSnapshot{snapshot},
		ScriptRunnerProjections: []*agentpb.ScriptRunnerProjection{projection},
		ScriptBodyArtifacts: []*agentpb.ScriptBodyArtifactMetadata{{
			ScriptExecutionId: input.ExecutionID, ScriptId: sources.Script.Record.Desired.ID,
			Generation: sources.BodyGeneration.Record.Generation,
			Size:       sources.BodyGeneration.Record.BodySize, Sha256: bodyDigest, Uid: uid, Gid: gid,
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: input.StepID, TimeoutSeconds: executionplan.ScriptExecutionTimeoutSeconds,
			Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
				ScriptExecutionId: input.ExecutionID, ScriptId: sources.Script.Record.Desired.ID,
				ScriptGeneration: sources.BodyGeneration.Record.Generation,
				EnvironmentId:    sources.Environment.Record.ID, ServiceId: sources.Service.Record.Desired.ID,
				ReleaseId:               sources.Release.Intent.ID,
				RenderGeneration:        sources.RenderInput.Record.Projection.RenderGeneration,
				ServiceDefinitionSha256: serviceDigest[:], BodySha256: bodyDigest,
				RunnerSnapshotId: input.SnapshotID, RunnerSnapshotSha256: snapshotDigest,
			}},
		}},
	}
	return executionplan.Seal(plan)
}

func projectScriptRunner(
	input ManualScriptPlanInput,
	service composetypes.ServiceConfig,
	uid uint32,
	gid uint32,
	networks []*agentpb.ScriptRunnerNetwork,
	mounts []*agentpb.ScriptRunnerMount,
	entryBindings []*agentpb.ScriptRunnerEntryBinding,
) (*agentpb.ScriptRunnerProjection, error) {
	environment, err := scriptEnvironmentPairs(service.Environment)
	if err != nil {
		return nil, err
	}
	ulimits, err := scriptUlimits(service.Ulimits)
	if err != nil {
		return nil, err
	}
	blkio, err := scriptBlkio(service.BlkioConfig)
	if err != nil {
		return nil, err
	}
	limits, reservations, err := scriptDeployResources(service.Deploy)
	if err != nil {
		return nil, err
	}
	labels := scriptPairs(map[string]string{
		"com.groundplane.environment-id":      input.Sources.Environment.Record.ID,
		"com.groundplane.kind":                "script-runner",
		"com.groundplane.managed":             "true",
		"com.groundplane.operation-id":        input.OperationID,
		"com.groundplane.plan-id":             input.PlanID,
		"com.groundplane.project-id":          input.Sources.Project.Record.ID,
		"com.groundplane.release-id":          input.Sources.Release.Intent.ID,
		"com.groundplane.render-generation":   strconv.FormatUint(input.Sources.RenderInput.Record.Projection.RenderGeneration, 10),
		"com.groundplane.script-execution-id": input.ExecutionID,
		"com.groundplane.script-generation":   strconv.FormatUint(input.Sources.BodyGeneration.Record.Generation, 10),
		"com.groundplane.script-id":           input.Sources.Script.Record.Desired.ID,
		"com.groundplane.service-id":          input.Sources.Service.Record.Desired.ID,
		"com.groundplane.tenant-id":           input.Sources.Tenant.Record.ID,
	})
	return &agentpb.ScriptRunnerProjection{
		SnapshotId: input.SnapshotID,
		Name:       "gp-script-" + strings.ToLower(input.ExecutionID),
		Image:      service.Image, Platform: service.Platform, Uid: uid, Gid: gid,
		WorkingDir: service.WorkingDir, Environment: environment,
		Dns: append([]string(nil), service.DNS...), DnsSearch: append([]string(nil), service.DNSSearch...),
		DnsOpt: append([]string(nil), service.DNSOpts...), ExtraHosts: service.ExtraHosts.AsList(":"),
		Sysctls: scriptPairs(service.Sysctls), Ulimits: ulimits,
		OomKillDisable: service.OomKillDisable, OomScoreAdj: service.OomScoreAdj,
		PidsLimit: service.PidsLimit, ShmSize: int64(service.ShmSize), Init: service.Init,
		Isolation: service.Isolation, Runtime: service.Runtime, ReadOnly: service.ReadOnly,
		Tmpfs: append([]string(nil), service.Tmpfs...), CapDrop: append([]string(nil), service.CapDrop...),
		SecurityOpt: append([]string(nil), service.SecurityOpt...), GroupAdd: append([]string(nil), service.GroupAdd...),
		Blkio: blkio, CpuCount: service.CPUCount, CpuPercent: service.CPUPercent,
		CpuShares: service.CPUShares, CpuPeriod: service.CPUPeriod, CpuQuota: service.CPUQuota,
		CpuRtPeriod: service.CPURTPeriod, CpuRtRuntime: service.CPURTRuntime,
		Cpus: service.CPUS, Cpuset: service.CPUSet,
		MemLimit: int64(service.MemLimit), MemReservation: int64(service.MemReservation),
		MemSwappiness: int64(service.MemSwappiness), MemswapLimit: int64(service.MemSwapLimit),
		StorageOpt: scriptPairs(service.StorageOpt), Limits: limits, Reservations: reservations,
		Mounts: mounts, Networks: networks, EntryBindings: cloneScriptEntryBindings(entryBindings), Labels: labels,
		Entrypoint: []string{"/bin/sh"}, Command: []string{"/groundplane-script-body"}, StopGraceSeconds: 10,
	}, nil
}

func validateScriptServiceDisposition(service composetypes.ServiceConfig) error {
	allowed := map[string]struct{}{
		"BlkioConfig": {}, "CapDrop": {}, "CPUCount": {}, "CPUPercent": {}, "CPUPeriod": {},
		"CPUQuota": {}, "CPURTPeriod": {}, "CPURTRuntime": {}, "CPUS": {}, "CPUSet": {}, "CPUShares": {},
		"Deploy": {}, "DNS": {}, "DNSOpts": {}, "DNSSearch": {}, "Environment": {}, "ExtraHosts": {},
		"GroupAdd": {}, "Image": {}, "Init": {}, "Isolation": {}, "MemLimit": {}, "MemReservation": {},
		"MemSwapLimit": {}, "MemSwappiness": {}, "Networks": {}, "OomKillDisable": {}, "OomScoreAdj": {},
		"PidsLimit": {}, "Platform": {}, "ReadOnly": {}, "Runtime": {}, "SecurityOpt": {}, "ShmSize": {},
		"StorageOpt": {}, "Sysctls": {}, "Tmpfs": {}, "Ulimits": {}, "User": {}, "Volumes": {}, "WorkingDir": {},
	}
	forced := map[string]struct{}{
		"Name": {}, "Profiles": {}, "Attach": {}, "Command": {}, "ContainerName": {}, "DependsOn": {},
		"Entrypoint": {}, "Expose": {}, "HealthCheck": {}, "Labels": {}, "CustomLabels": {}, "Logging": {},
		"LogDriver": {}, "LogOpt": {}, "Ports": {}, "PullPolicy": {}, "Restart": {}, "Scale": {},
		"StdinOpen": {}, "StopGracePeriod": {}, "StopSignal": {}, "Tty": {},
	}
	value := reflect.ValueOf(service)
	typeOf := value.Type()
	for index := 0; index < value.NumField(); index++ {
		if value.Field(index).IsZero() {
			continue
		}
		name := typeOf.Field(index).Name
		if _, ok := allowed[name]; ok {
			continue
		}
		if _, ok := forced[name]; ok {
			continue
		}
		return errs.Newf(errs.KindValidationFailed, "Script runner does not support Compose service field %s", name)
	}
	if len(service.EnvFiles) != 0 || len(service.Configs) != 0 || len(service.Secrets) != 0 {
		return errs.New(
			errs.KindValidationFailed,
			"Script runner Entry artifacts must be resolved through the private assignment channel",
		)
	}
	return nil
}

func projectScriptNetworks(
	service composetypes.ServiceConfig,
	sources etcd.ScriptExecutionSources,
) ([]*agentpb.ScriptRunnerNetwork, error) {
	identityByName := make(map[string]string, len(sources.AppliedProjection.Record.Networks))
	for _, identity := range sources.AppliedProjection.Record.Networks {
		identityByName[identity.Name] = identity.ID
	}
	revisionByID := make(map[string]int64, len(sources.Networks))
	for _, network := range sources.Networks {
		revisionByID[network.Record.Desired.ID] = network.Revision
	}
	result := make([]*agentpb.ScriptRunnerNetwork, 0, len(service.Networks))
	for name, config := range service.Networks {
		networkID := identityByName[name]
		if networkID == "" || revisionByID[networkID] <= 0 {
			return nil, errs.New(errs.KindStateConflict, "Script runner network is not part of the frozen topology")
		}
		if config == nil {
			config = &composetypes.ServiceNetworkConfig{}
		}
		if len(config.Aliases) != 0 || config.GatewayPriority != 0 || config.Ipv4Address != "" ||
			config.Ipv6Address != "" || len(config.LinkLocalIPs) != 0 || config.MacAddress != "" ||
			len(config.Extensions) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Script runner network contains unsupported endpoint fields")
		}
		result = append(result, &agentpb.ScriptRunnerNetwork{
			NetworkId: networkID, NetworkModRevision: uint64(revisionByID[networkID]),
			RenderedAttachment: &agentpb.ScriptNetworkAttachment{
				DockerNetworkName: "gp_net_" + strings.ToLower(networkID),
				DriverOptions:     scriptPairs(config.DriverOpts), InterfaceName: config.InterfaceName,
				Priority: int64(config.Priority),
			},
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].NetworkId < result[right].NetworkId })
	return result, nil
}

func projectScriptMounts(
	service composetypes.ServiceConfig,
	sources etcd.ScriptExecutionSources,
) ([]*agentpb.ScriptRunnerMount, error) {
	volumeIDByTarget := make(map[string]string)
	for _, mount := range sources.AppliedProjection.Record.VolumeMounts {
		if mount.ServiceID == sources.Service.Record.Desired.ID {
			volumeIDByTarget[mount.Target] = mount.VolumeID
		}
	}
	result := make([]*agentpb.ScriptRunnerMount, 0, len(service.Volumes))
	for _, mount := range service.Volumes {
		volumeID := volumeIDByTarget[mount.Target]
		if volumeID == "" || ids.Validate(ids.KindVolume, volumeID) != nil {
			return nil, errs.New(errs.KindValidationFailed, "Script runner supports only managed Volume mounts")
		}
		projected, err := projectScriptMount(mount, volumeID)
		if err != nil {
			return nil, err
		}
		result = append(result, &agentpb.ScriptRunnerMount{
			SourceId: volumeID, SourceModRevision: uint64(sources.AppliedProjection.Revision), RenderedMount: projected,
		})
	}
	sort.Slice(result, func(left, right int) bool {
		leftKey := result[left].SourceId + "\x00" + result[left].RenderedMount.Target
		rightKey := result[right].SourceId + "\x00" + result[right].RenderedMount.Target
		return leftKey < rightKey
	})
	return result, nil
}

func projectScriptMount(mount composetypes.ServiceVolumeConfig, volumeID string) (*agentpb.ScriptMount, error) {
	if mount.Type != "volume" || mount.Bind != nil || mount.Tmpfs != nil || mount.Image != nil ||
		len(mount.Extensions) != 0 || mount.Volume != nil &&
		(len(mount.Volume.Labels) != 0 || len(mount.Volume.Extensions) != 0) {
		return nil, errs.New(errs.KindValidationFailed, "Script runner Volume mount contains unsupported fields")
	}
	result := &agentpb.ScriptMount{
		Type: "volume", Source: "gp_vol_" + strings.ToLower(volumeID), Target: mount.Target,
		ReadOnly: mount.ReadOnly, Consistency: mount.Consistency,
	}
	if mount.Volume != nil {
		result.VolumeNoCopy = mount.Volume.NoCopy
		result.VolumeSubpath = mount.Volume.Subpath
	}
	return result, nil
}

func scriptEnvironmentPairs(environment composetypes.MappingWithEquals) ([]*agentpb.ScriptStringPair, error) {
	keys := make([]string, 0, len(environment))
	for key, value := range environment {
		if value == nil {
			return nil, errs.New(errs.KindValidationFailed, "Script runner environment contains an unresolved host lookup")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*agentpb.ScriptStringPair, len(keys))
	for index, key := range keys {
		result[index] = &agentpb.ScriptStringPair{Key: key, Value: *environment[key]}
	}
	return result, nil
}

func scriptPairs(values map[string]string) []*agentpb.ScriptStringPair {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*agentpb.ScriptStringPair, len(keys))
	for index, key := range keys {
		result[index] = &agentpb.ScriptStringPair{Key: key, Value: values[key]}
	}
	return result
}

func scriptUlimits(values map[string]*composetypes.UlimitsConfig) ([]*agentpb.ScriptUlimit, error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]*agentpb.ScriptUlimit, len(keys))
	for index, key := range keys {
		value := values[key]
		if value == nil || len(value.Extensions) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Script runner ulimit is invalid")
		}
		result[index] = &agentpb.ScriptUlimit{
			Name: key, Single: int64(value.Single), Soft: int64(value.Soft), Hard: int64(value.Hard),
		}
	}
	return result, nil
}

func scriptBlkio(value *composetypes.BlkioConfig) (*agentpb.ScriptBlkio, error) {
	if value == nil {
		return nil, nil
	}
	if len(value.Extensions) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Script runner blkio extensions are unsupported")
	}
	result := &agentpb.ScriptBlkio{Weight: uint32(value.Weight)}
	for _, device := range value.WeightDevice {
		if len(device.Extensions) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Script runner blkio device extensions are unsupported")
		}
		result.WeightDevices = append(result.WeightDevices, &agentpb.ScriptWeightDevice{Path: device.Path, Weight: uint32(device.Weight)})
	}
	convert := func(values []composetypes.ThrottleDevice) ([]*agentpb.ScriptThrottleDevice, error) {
		converted := make([]*agentpb.ScriptThrottleDevice, len(values))
		for index, device := range values {
			if len(device.Extensions) != 0 {
				return nil, errs.New(errs.KindValidationFailed, "Script runner blkio device extensions are unsupported")
			}
			converted[index] = &agentpb.ScriptThrottleDevice{Path: device.Path, Rate: int64(device.Rate)}
		}
		sort.Slice(converted, func(left, right int) bool { return converted[left].Path < converted[right].Path })
		return converted, nil
	}
	var err error
	sort.Slice(result.WeightDevices, func(left, right int) bool { return result.WeightDevices[left].Path < result.WeightDevices[right].Path })
	if result.DeviceReadBps, err = convert(value.DeviceReadBps); err != nil {
		return nil, err
	}
	if result.DeviceReadIops, err = convert(value.DeviceReadIOps); err != nil {
		return nil, err
	}
	if result.DeviceWriteBps, err = convert(value.DeviceWriteBps); err != nil {
		return nil, err
	}
	if result.DeviceWriteIops, err = convert(value.DeviceWriteIOps); err != nil {
		return nil, err
	}
	return result, nil
}

func scriptDeployResources(
	deploy *composetypes.DeployConfig,
) (*agentpb.ScriptResource, *agentpb.ScriptResource, error) {
	if deploy == nil {
		return nil, nil, nil
	}
	if deploy.Mode != "" && deploy.Mode != "replicated" || len(deploy.Labels) != 0 || deploy.UpdateConfig != nil ||
		deploy.RollbackConfig != nil || deploy.RestartPolicy != nil || !reflect.ValueOf(deploy.Placement).IsZero() ||
		deploy.EndpointMode != "" || len(deploy.Extensions) != 0 || len(deploy.Resources.Extensions) != 0 {
		return nil, nil, errs.New(errs.KindValidationFailed, "Script runner deploy section contains unsupported fields")
	}
	convert := func(resource *composetypes.Resource) (*agentpb.ScriptResource, error) {
		if resource == nil {
			return nil, nil
		}
		if len(resource.Devices) != 0 || len(resource.GenericResources) != 0 || len(resource.Extensions) != 0 {
			return nil, errs.New(errs.KindValidationFailed, "Script runner deploy resource contains unsupported fields")
		}
		return &agentpb.ScriptResource{
			Present: true, Cpus: resource.NanoCPUs.Value(), MemoryBytes: int64(resource.MemoryBytes), Pids: resource.Pids,
		}, nil
	}
	limits, err := convert(deploy.Resources.Limits)
	if err != nil {
		return nil, nil, err
	}
	reservations, err := convert(deploy.Resources.Reservations)
	if err != nil {
		return nil, nil, err
	}
	return limits, reservations, nil
}

func parseScriptNumericUser(value string) (uint32, uint32, error) {
	if value == "" {
		return 0, 0, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner user must be an explicit numeric uid:gid")
	}
	uid, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner uid is invalid")
	}
	gid, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner gid is invalid")
	}
	return uint32(uid), uint32(gid), nil
}

func digestPinnedImageBytes(value string) ([]byte, error) {
	separator := strings.LastIndex(value, "@sha256:")
	if separator < 0 || !imageref.IsDigestPinned(value) {
		return nil, errs.New(errs.KindValidationFailed, "Script runner image is not digest pinned")
	}
	digest, err := hex.DecodeString(value[separator+8:])
	if err != nil || len(digest) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "Script runner image digest is invalid")
	}
	return digest, nil
}

func deterministicScriptProtoDigest(value proto.Message) ([]byte, error) {
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return append([]byte(nil), digest[:]...), nil
}

func cloneScriptNetworks(values []*agentpb.ScriptRunnerNetwork) []*agentpb.ScriptRunnerNetwork {
	result := make([]*agentpb.ScriptRunnerNetwork, len(values))
	for index, value := range values {
		result[index] = proto.Clone(value).(*agentpb.ScriptRunnerNetwork)
	}
	return result
}

func cloneScriptMounts(values []*agentpb.ScriptRunnerMount) []*agentpb.ScriptRunnerMount {
	result := make([]*agentpb.ScriptRunnerMount, len(values))
	for index, value := range values {
		result[index] = proto.Clone(value).(*agentpb.ScriptRunnerMount)
	}
	return result
}

func cloneScriptEntryBindings(values []*agentpb.ScriptRunnerEntryBinding) []*agentpb.ScriptRunnerEntryBinding {
	result := make([]*agentpb.ScriptRunnerEntryBinding, len(values))
	for index, value := range values {
		result[index] = proto.Clone(value).(*agentpb.ScriptRunnerEntryBinding)
	}
	return result
}

func finiteScriptFloat(value float32) bool {
	return !math.IsNaN(float64(value)) && !math.IsInf(float64(value), 0)
}
