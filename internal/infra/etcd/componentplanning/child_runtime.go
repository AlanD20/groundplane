package componentplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const componentRuntimePrefix = "/v1/records/blueprint-component-runtimes/"

// Runtime is the last acknowledged runtime of one Environment Component.
// Artifact is copied from the completed Task's immutable execution authority,
// so later planning never depends on a mutable desired head or retained Task.
type Runtime struct {
	EnvironmentID    string                 `json:"environment_id"`
	Component        componentrecord.Record `json:"component"`
	RevisionID       string                 `json:"revision_id"`
	RenderGeneration uint64                 `json:"render_generation"`
	Artifact         []byte                 `json:"artifact,omitempty"`
	Zones            []zonerecord.Record    `json:"zones,omitempty"`
	TaskID           string                 `json:"task_id"`
	PlanID           string                 `json:"plan_id"`
	PlanHash         string                 `json:"plan_hash"`
}

func RuntimeKey(componentID string) string {
	return componentRuntimePrefix + componentID
}

func ValidateRuntime(record Runtime) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		componentrecord.ValidateRecord(record.Component) != nil ||
		record.Component.Desired.Owner != core.ComponentOwnerEnvironment ||
		record.Component.Desired.OwnerID != record.EnvironmentID ||
		ids.Validate(ids.KindTask, record.RevisionID) != nil ||
		record.RenderGeneration == 0 || ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindPlan, record.PlanID) != nil || !recordcodec.ValidNonZeroSHA256(record.PlanHash) ||
		record.Component.Runtime.Healthy != record.Component.Desired.Enabled {
		return errs.New(errs.KindValidationFailed, "Component runtime identity is invalid")
	}
	previousZoneID := ""
	for _, zone := range record.Zones {
		if zonerecord.ValidateRecord(zone) != nil || zone.EnvironmentID != record.EnvironmentID ||
			zone.Desired.ID <= previousZoneID {
			return errs.New(errs.KindValidationFailed, "Component runtime Zone authority is invalid")
		}
		previousZoneID = zone.Desired.ID
	}
	if !record.Component.Desired.Enabled {
		if len(record.Component.Runtime.GeneratedServices) != 0 ||
			record.Component.Runtime.PinnedIPv4 != "" || len(record.Artifact) != 0 {
			return errs.New(errs.KindValidationFailed, "disabled Component runtime is invalid")
		}
		return nil
	}
	if len(record.Component.Runtime.GeneratedServices) != 1 || len(record.Artifact) == 0 ||
		len(record.Artifact) > projectionrecord.EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindValidationFailed, "enabled Component runtime artifact is invalid")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Artifact, artifact); err != nil {
		return errs.New(errs.KindValidationFailed, "Component runtime artifact is unreadable")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	yamlDigest := sha256.Sum256(artifact.GetCanonicalYaml())
	if err != nil || !bytes.Equal(canonical, record.Artifact) ||
		ids.Validate(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != record.EnvironmentID || len(artifact.GetCanonicalYaml()) == 0 ||
		len(artifact.GetYamlSha256()) != sha256.Size ||
		subtle.ConstantTimeCompare(yamlDigest[:], artifact.GetYamlSha256()) != 1 {
		return errs.New(errs.KindValidationFailed, "Component runtime artifact identity is invalid")
	}
	serviceID := record.Component.Runtime.GeneratedServices[0]
	matched := 0
	for _, service := range artifact.GetServices() {
		if service.GetOwnerComponentId() != record.Component.Desired.ID {
			continue
		}
		if service.GetServiceId() != serviceID {
			return errs.New(errs.KindValidationFailed, "Component runtime artifact exceeds its singleton")
		}
		matched++
	}
	if matched != 1 {
		return errs.New(errs.KindValidationFailed, "Component runtime Service is unavailable")
	}
	return nil
}

func EncodeRuntime(record Runtime) ([]byte, error) {
	if err := ValidateRuntime(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("blueprint-component-runtime", record)
}

func ReadRuntime(
	ctx context.Context,
	store interface {
		GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
	},
	componentID string,
	revision int64,
) (keyvalue.Versioned[Runtime], bool, error) {
	var empty keyvalue.Versioned[Runtime]
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return empty, false, err
	}
	if store == nil || ids.Validate(ids.KindComponent, componentID) != nil || revision < 0 {
		return empty, false, errs.New(errs.KindValidationFailed, "Component runtime lookup is invalid")
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{RuntimeKey(componentID)}, Revision: revision,
	})
	if err != nil {
		return empty, false, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 {
		return empty, false, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return keyvalue.Versioned[Runtime]{ReadRevision: read.ReadRevision}, false, nil
	}
	record, err := recordcodec.Decode[Runtime](read.Values[0].Value, "blueprint-component-runtime")
	if err != nil || ValidateRuntime(record) != nil || record.Component.Desired.ID != componentID {
		return empty, false, recordcodec.CorruptRecord()
	}
	return keyvalue.Versioned[Runtime]{
		Record: record, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}, true, nil
}

func (repository *Planner) GetBlueprintComponentRuntime(
	ctx context.Context,
	componentID string,
) (keyvalue.Versioned[Runtime], bool, error) {
	if repository == nil {
		return keyvalue.Versioned[Runtime]{}, false, errs.New(
			errs.KindValidationFailed,
			"Component runtime reader is unavailable",
		)
	}
	return ReadRuntime(ctx, repository.store, componentID, 0)
}
