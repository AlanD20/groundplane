package blueprints

import (
	"crypto/sha256"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentDesiredRevisionIdentity struct {
	EnvironmentID string
	RevisionID    string
}

func ValidateEnvironmentDesiredRevisionIdentity(value EnvironmentDesiredRevisionIdentity) error {
	if ids.Validate(ids.KindEnvironment, value.EnvironmentID) != nil ||
		ids.Validate(ids.KindTask, value.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Environment desired revision identity is invalid")
	}
	return nil
}

type EnvironmentDesiredMutationAudit struct {
	Volume        *EnvironmentVolumeMutationAudit
	Service       *EnvironmentServiceMutationAudit
	Entry         *EnvironmentEntryMutationAudit
	Entries       []EnvironmentEntryMutationAudit
	Zone          *EnvironmentZoneMutationAudit
	Route         *EnvironmentRouteMutationAudit
	ConfigRestore *EnvironmentConfigRestoreAudit
	Configuration *EnvironmentConfigurationMutationAudit
}

// Configuration publications change authored metadata, not the rendered host
// generation. The protected request identifies the exact direct action.
type EnvironmentConfigurationMutationAudit struct {
	BaseRevisionID string
}

// EnvironmentConfigRestoreAudit identifies a complete Entry-set replacement.
// Selected bytes remain in their immutable generations, never in this audit.
type EnvironmentConfigRestoreAudit struct {
	BaseRevisionID string
	PointID        string
	GenerationID   string
	SourceSHA256   [sha256.Size]byte
}
type EnvironmentEntryMutationAction uint8

const (
	EnvironmentEntryMutationCreate EnvironmentEntryMutationAction = iota + 1
	EnvironmentEntryMutationEdit
	EnvironmentEntryMutationRemove
)

type EnvironmentEntryMutationAudit struct {
	Action         EnvironmentEntryMutationAction
	BaseRevisionID string
	EntryID        string
	Record         *entryrecord.Record
}
type EnvironmentServiceMutationAction uint8

const (
	EnvironmentServiceMutationCreate EnvironmentServiceMutationAction = iota + 1
	EnvironmentServiceMutationEdit
	EnvironmentServiceMutationRemove
)

type EnvironmentServiceMutationAudit struct {
	Action         EnvironmentServiceMutationAction
	BaseRevisionID string
	ServiceID      string
	Request        *EnvironmentServiceMutationRequest
}
type EnvironmentServiceMutationRequest struct {
	VolumeMounts  *[]core.Mount                      `json:"volume_mounts,omitempty"`
	Command       *[]string                          `json:"command,omitempty"`
	Entrypoint    *[]string                          `json:"entrypoint,omitempty"`
	WorkingDir    *string                            `json:"working_dir,omitempty"`
	User          *string                            `json:"user,omitempty"`
	Aliases       *map[string][]string               `json:"aliases,omitempty"`
	DependsOn     *map[string]core.ServiceDependency `json:"depends_on,omitempty"`
	Logging       *core.ServiceLogging               `json:"logging,omitempty"`
	EnvironmentID string                             `json:"environment_id,omitempty"`
	Name          string                             `json:"name,omitempty"`
	Image         string                             `json:"image"`
	Zones         []string                           `json:"zones,omitempty"`
	Strategy      core.Strategy                      `json:"strategy"`
	OnFailure     core.OnFailure                     `json:"on_failure"`
	Healthcheck   core.Healthcheck                   `json:"healthcheck,omitempty"`
	Resources     core.Resources                     `json:"resources,omitempty"`
	Expose        []string                           `json:"expose,omitempty"`
	Restart       string                             `json:"restart,omitempty"`
	Replicas      int                                `json:"replicas"`
}

type EnvironmentZoneMutationAction uint8

const EnvironmentZoneMutationCreate EnvironmentZoneMutationAction = 1
const EnvironmentZoneMutationRemove EnvironmentZoneMutationAction = 3

type EnvironmentZoneMutationAudit struct {
	Action                 EnvironmentZoneMutationAction
	BaseRevisionID, ZoneID string
	Request                *EnvironmentZoneMutationRequest
}
type EnvironmentZoneMutationRequest struct {
	EnvironmentID string `json:"environment_id"`
	Name          string `json:"name"`
	Subnet        string `json:"subnet"`
	Internal      bool   `json:"internal"`
}
type EnvironmentRouteMutationAction uint8

const EnvironmentRouteMutationCreate EnvironmentRouteMutationAction = 1
const EnvironmentRouteMutationEdit EnvironmentRouteMutationAction = 2
const EnvironmentRouteMutationRemove EnvironmentRouteMutationAction = 3

type EnvironmentRouteMutationAudit struct {
	Action                  EnvironmentRouteMutationAction
	BaseRevisionID, RouteID string
	Request                 *EnvironmentRouteMutationRequest
}
type EnvironmentRouteMutationRequest struct {
	EnvironmentID   string `json:"environment_id,omitempty"`
	Host            string `json:"host,omitempty"`
	Path            string `json:"path,omitempty"`
	Exposure        string `json:"exposure"`
	TargetServiceID string `json:"target_service_id,omitempty"`
	TargetPort      uint16 `json:"target_port,omitempty"`
}

type EnvironmentVolumeMutationAction uint8

const (
	EnvironmentVolumeMutationAdd EnvironmentVolumeMutationAction = iota + 1
	EnvironmentVolumeMutationEdit
	EnvironmentVolumeMutationRemove
)

type EnvironmentVolumeMutationAudit struct {
	Action             EnvironmentVolumeMutationAction
	VolumeID           string
	Slug               string
	Key                string
	KeySupplied        bool
	PreconditionDigest [sha256.Size]byte
}
