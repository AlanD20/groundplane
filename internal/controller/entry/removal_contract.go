package entry

import (
	"context"
	"time"
)

type RemoveRequest struct {
	EntryID        string
	IdempotencyKey string
}

// RemovalOutcome is the capability result. HTTP status, media type, and body
// serialization belong to the human-API adapter rather than the use case.
type RemovalOutcome struct {
	TaskID string
}

// RemovalEnvironmentIdentity freezes every label and path used to reparse the
// immutable Blueprint revision after publication. Renames never rewrite it.
type RemovalEnvironmentIdentity struct {
	TenantID            string
	TenantSlug          string
	ProjectID           string
	ProjectSlug         string
	EnvironmentID       string
	EnvironmentName     string
	AuthorizedVolumeDir string
}

// RemovalDependencyFence is the exact singleton revision inspected before a
// removal is planned. Present=false fences absence of the singleton index.
type RemovalDependencyFence struct {
	Present  bool
	ID       string
	Revision int64
}

// RemovalCandidate is capability-owned concurrency evidence. It contains no
// persistence records, transaction descriptions, tombstones, or HTTP values.
type RemovalCandidate struct {
	EntryID             string
	EntryRevision       int64
	EnvironmentRevision int64
	ProjectRevision     int64
	TenantRevision      int64
	ProjectionRevision  int64
	Identity            RemovalEnvironmentIdentity
	Cloudflare          RemovalDependencyFence
}

type RemovalInspection struct {
	Replay    *RemovalOutcome
	Candidate RemovalCandidate
}

type RemovalTask struct {
	ID          string
	OperationID string
	PlanID      string
	CreatedAt   time.Time
}

type RemovalPublication struct {
	Request   RemoveRequest
	Candidate RemovalCandidate
	Task      RemovalTask
	Plan      RemovalTaskPlan
}

// Repository is the consumer-owned durable side-effect seam. Implementations
// translate persistence DTOs entirely behind these capability-owned values.
type Repository interface {
	InspectRemoval(context.Context, RemoveRequest) (RemovalInspection, error)
	PublishRemoval(context.Context, RemovalPublication) (RemovalOutcome, error)
}

type RemovalExecutor string

const (
	RemovalExecutorAgent      RemovalExecutor = "agent"
	RemovalExecutorController RemovalExecutor = "controller"
)

type RemovalPlanRequest struct {
	TaskID             string
	PlanID             string
	EntryID            string
	EnvironmentID      string
	EntryRevision      int64
	ProjectionRevision int64
	ArtifactID         string
	CreatedAt          time.Time
	Identity           RemovalEnvironmentIdentity
}

type RemovalTaskPlan struct {
	Executor            RemovalExecutor
	PlanHash            string
	RenderGeneration    int32
	EnvironmentID       string
	BlueprintRevisionID string
	ArtifactID          string
	Identity            RemovalEnvironmentIdentity
	Steps               []RemovalStep
	Materializations    []RemovalMaterialization
	TimeoutSeconds      int64
}

type RemovalStep struct {
	ID string
}

type RemovalMaterialization struct {
	StepID            string
	MaterializationID string
	EnvironmentID     string
	Destination       string
	ServiceID         string
	ServiceName       string
	OutputKind        RemovalOutputKind
	UID               uint32
	GID               uint32
	Mode              uint32
	Length            uint64
	SHA256            string
	Source            RemovalSource
}

type RemovalOutputKind string

const (
	RemovalOutputGeneratedEnvironment RemovalOutputKind = "generated_env"
	RemovalOutputPlainFile            RemovalOutputKind = "plain_file"
	RemovalOutputSecretFile           RemovalOutputKind = "secret_file"
	RemovalOutputRemoveGeneratedEnv   RemovalOutputKind = "remove_generated_env"
	RemovalOutputRemovePlainFile      RemovalOutputKind = "remove_plain_file"
	RemovalOutputRemoveSecretFile     RemovalOutputKind = "remove_secret_file"
)

type RemovalSourceKind string

const (
	RemovalSourceGeneratedEnvironment RemovalSourceKind = "generated_environment"
	RemovalSourceRemoval              RemovalSourceKind = "removal"
)

type RemovalValueStorage string

const (
	RemovalValueStoragePlain  RemovalValueStorage = "plain"
	RemovalValueStorageSecret RemovalValueStorage = "secret"
)

type RemovalSource struct {
	Kind                 RemovalSourceKind
	GeneratedEnvironment *RemovalGeneratedEnvironment
}

type RemovalGeneratedEnvironment struct {
	FormatVersion uint32
	Values        []RemovalGeneratedValue
}

type RemovalGeneratedValue struct {
	Name              string
	EntryID           string
	ValueGenerationID string
	Storage           RemovalValueStorage
}

type Planner interface {
	PrepareEntryRemoval(context.Context, RemovalPlanRequest) (RemovalTaskPlan, error)
}
