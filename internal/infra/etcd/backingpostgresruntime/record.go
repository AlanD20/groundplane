// Package backingpostgresruntime owns the current database authority consumed
// by Backup. Provisioning and patch Deploys publish the same record atomically
// with their verified outcomes; readers never fall back to an older container.
package backingpostgresruntime

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type Record struct {
	EnvironmentID       string                                       `json:"environment_id"`
	ServiceID           string                                       `json:"service_id"`
	Artifact            []byte                                       `json:"artifact"`
	LocalImageID        string                                       `json:"local_image_id"`
	ToolsImage          string                                       `json:"tools_image"`
	CatalogSHA256       string                                       `json:"catalog_sha256"`
	Provisioning        *environmentprojection.BackingRuntimeReceipt `json:"provisioning,omitempty"`
	Deployment          *serviceruntimerecord.Acknowledgement        `json:"deployment,omitempty"`
	DeploymentReleaseID string                                       `json:"deployment_release_id,omitempty"`
}

func Key(environmentID, serviceID string) string {
	return "/v1/runtime/backing-postgres/" + environmentID + "/" + serviceID
}

func FromProvisioning(projection environmentprojection.EnvironmentComposeProjection) (Record, error) {
	if projection.BackingRuntime == nil {
		return Record{}, invalid()
	}
	source := *projection.BackingRuntime
	artifact, workload, err := environmentprojection.SelectBackingRuntime(projection, source.ServiceID)
	if err != nil {
		return Record{}, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		return Record{}, errs.Wrap(errs.KindInternal, err)
	}
	record := Record{EnvironmentID: projection.EnvironmentID, ServiceID: source.ServiceID,
		Artifact: encoded, LocalImageID: source.LocalImageID, ToolsImage: workload.PostgresToolsImage,
		CatalogSHA256: source.ManagedReleaseSHA256, Provisioning: &source}
	return record, Validate(record)
}

func FromDeployment(prior Record, deployed serviceruntimerecord.Record) (Record, error) {
	if Validate(prior) != nil || serviceruntimerecord.Validate(deployed) != nil ||
		deployed.EnvironmentID != prior.EnvironmentID || deployed.Runtime.ServiceID != prior.ServiceID {
		return Record{}, invalid()
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(deployed.Runtime.CurrentArtifact, artifact) != nil {
		return Record{}, invalid()
	}
	workload, err := ValidatePatchArtifact(prior, artifact)
	if err != nil {
		return Record{}, err
	}
	source := deployed.Source
	record := Record{EnvironmentID: prior.EnvironmentID, ServiceID: prior.ServiceID,
		Artifact: append([]byte(nil), deployed.Runtime.CurrentArtifact...), LocalImageID: workload.ImageReference,
		ToolsImage: prior.ToolsImage, CatalogSHA256: prior.CatalogSHA256, Deployment: &source,
		DeploymentReleaseID: deployed.Runtime.ReleaseID}
	return record, Validate(record)
}

// ValidatePatchArtifact protects the data and tools before publication and again
// at acknowledgement. A patch changes the database image, not its storage.
func ValidatePatchArtifact(prior Record, artifact *agentpb.ComposeArtifact) (*agentpb.ComposeService, error) {
	if artifact == nil || artifact.OwnerId != prior.EnvironmentID {
		return nil, invalid()
	}
	var workload *agentpb.ComposeService
	for _, candidate := range artifact.Services {
		if candidate.ServiceId == prior.ServiceID {
			if workload != nil || candidate.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
				return nil, invalid()
			}
			workload = candidate
		}
	}
	if workload == nil || workload.PostgresToolsImage != prior.ToolsImage ||
		!workloadimage.LocalIDValid(workload.ImageReference) {
		return nil, invalid()
	}
	previous, previousWorkload, err := Select(prior)
	if err != nil || !sameVolumes(previous, previousWorkload, artifact, workload) {
		return nil, errs.New(
			errs.KindValidationFailed,
			"PostgreSQL image updates must preserve the data Volume and backup-tool mounts",
		)
	}
	return workload, nil
}

func Select(record Record) (*agentpb.ComposeArtifact, *agentpb.ComposeService, error) {
	if err := Validate(record); err != nil {
		return nil, nil, err
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(record.Artifact, artifact) != nil {
		return nil, nil, invalid()
	}
	for _, service := range artifact.Services {
		if service.ServiceId == record.ServiceID {
			return artifact, service, nil
		}
	}
	return nil, nil, invalid()
}

func Validate(record Record) error {
	if ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, record.ServiceID) != nil ||
		!workloadimage.LocalIDValid(record.LocalImageID) ||
		!recordcodec.ValidSHA256(record.CatalogSHA256) ||
		(record.Provisioning == nil) == (record.Deployment == nil) {
		return invalid()
	}
	if _, err := postgres16protocol.ToolsDirectory(record.ToolsImage); err != nil {
		return err
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(record.Artifact, artifact) != nil || artifact.OwnerId != record.EnvironmentID {
		return invalid()
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.Services {
		if service.ServiceId == record.ServiceID {
			if selected != nil {
				return invalid()
			}
			selected = service
		}
	}
	if selected == nil || selected.PostgresToolsImage != record.ToolsImage || selected.ExpectedReplicas != 1 {
		return invalid()
	}
	if record.Provisioning != nil {
		proof := record.Provisioning
		digest := sha256.Sum256(record.Artifact)
		if proof.ServiceID != record.ServiceID || proof.LocalImageID != record.LocalImageID ||
			proof.ManagedReleaseSHA256 != record.CatalogSHA256 || proof.AcknowledgedAt.IsZero() ||
			proof.AcknowledgedAt.Location() != time.UTC || record.DeploymentReleaseID != "" ||
			ids.Validate(ids.KindTask, proof.TaskID) != nil || ids.Validate(ids.KindAgent, proof.AgentID) != nil ||
			ids.Validate(ids.KindAssignment, proof.AssignmentID) != nil || proof.ExecutionEpoch == 0 ||
			!recordcodec.ValidSHA256(proof.PlanHash) || hex.EncodeToString(digest[:]) != proof.ArtifactSHA256 {
			return invalid()
		}
		if _, err := backingruntimefact.Workload(artifact, record.ServiceID, proof.PlanID, proof.RenderGeneration); err != nil {
			return err
		}
	} else {
		if selected.ImageReference != record.LocalImageID || selected.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			return invalid()
		}
		if err := serviceruntimerecord.Validate(serviceruntimerecord.Record{
			EnvironmentID: record.EnvironmentID,
			Runtime: executionplan.CandidateRuntime{ServiceID: record.ServiceID, ReleaseID: record.DeploymentReleaseID,
				Target: "singleton", CurrentArtifact: record.Artifact}, Source: *record.Deployment,
		}); err != nil {
			return err
		}
	}
	return nil
}

func Encode(record Record) ([]byte, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backing_postgres_runtime", record)
}

func Decode(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "backing_postgres_runtime")
	if err != nil || Validate(record) != nil {
		return Record{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func invalid() error {
	return errs.New(errs.KindStateConflict, "current PostgreSQL runtime authority is invalid")
}

// Image replacement must retain the exact managed data and tooling mounts.
func sameVolumes(prior *agentpb.ComposeArtifact, old *agentpb.ComposeService,
	next *agentpb.ComposeArtifact, current *agentpb.ComposeService,
) bool {
	type workload struct {
		Volumes []dataMount `yaml:"volumes"`
	}
	type document struct {
		Services map[string]workload   `yaml:"services"`
		Volumes  map[string]dataVolume `yaml:"volumes"`
	}
	var a, b document
	if yaml.Unmarshal(prior.CanonicalYaml, &a) != nil || yaml.Unmarshal(next.CanonicalYaml, &b) != nil {
		return false
	}
	x, present := a.Services[old.ComposeName]
	y, found := b.Services[current.ComposeName]
	slices.SortFunc(x.Volumes, func(a, b dataMount) int { return strings.Compare(a.Target, b.Target) })
	slices.SortFunc(y.Volumes, func(a, b dataMount) int { return strings.Compare(a.Target, b.Target) })
	if !present || !found || !slices.Equal(x.Volumes, y.Volumes) {
		return false
	}
	for _, mount := range x.Volumes {
		if mount.Type == "volume" {
			oldVolume, exists := a.Volumes[mount.Source]
			newVolume, found := b.Volumes[mount.Source]
			if !exists || !found || oldVolume.Name != newVolume.Name || oldVolume.Driver != newVolume.Driver ||
				oldVolume.External != newVolume.External || !maps.Equal(oldVolume.DriverOpts, newVolume.DriverOpts) ||
				!maps.Equal(oldVolume.Labels, newVolume.Labels) {
				return false
			}
		}
	}
	return true
}

type dataMount struct {
	Type     string `yaml:"type"`
	Source   string `yaml:"source"`
	Target   string `yaml:"target"`
	ReadOnly bool   `yaml:"read_only"`
	Bind     struct {
		CreateHostPath bool `yaml:"create_host_path"`
	} `yaml:"bind"`
	Volume struct {
		NoCopy  bool   `yaml:"nocopy"`
		Subpath string `yaml:"subpath"`
	} `yaml:"volume"`
}

type dataVolume struct {
	Name       string            `yaml:"name"`
	Driver     string            `yaml:"driver"`
	DriverOpts map[string]string `yaml:"driver_opts"`
	External   bool              `yaml:"external"`
	Labels     map[string]string `yaml:"labels"`
}
