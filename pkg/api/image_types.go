package api

type ImageFetchRequest struct {
	Image string `json:"image" minLength:"1" maxLength:"512" doc:"Explicit tag or SHA-256 digest in a public registry or GP's managed private registry"`
}

// HostImage is a live local-daemon observation. Containers includes stopped
// containers; it is not a claim that the image is safe to delete.
type HostImage struct {
	ID               string             `json:"id"`
	Tags             []string           `json:"tags" nullable:"false"`
	Digests          []string           `json:"digests" nullable:"false"`
	SizeBytes        int64              `json:"size_bytes"`
	CreatedAt        string             `json:"created_at"`
	Containers       int                `json:"containers"`
	ContainerUses    []ImageContainer   `json:"container_uses" nullable:"false"`
	ProtectionReason string             `json:"protection_reason" doc:"Additional non-container removal protection; empty when none is reported"`
	RemovalBlocked   string             `json:"removal_blocked" doc:"Empty when current observation permits removal; admission and execution recheck protection"`
	Fetches          []ImageFetchRecord `json:"fetches" nullable:"false" doc:"Fetch requests still retained in the Task journal, with each attempt's status; not current Docker tags"`
}

type ImageContainer struct {
	ID      string               `json:"id"`
	Name    string               `json:"name"`
	State   string               `json:"state"`
	Managed bool                 `json:"managed" doc:"Container carries Groundplane management labels; not an authorization claim"`
	Owner   *ImageContainerOwner `json:"owner,omitempty"`
}

type ImageContainerOwner struct {
	ServiceID       string `json:"service_id"`
	ServiceName     string `json:"service_name"`
	EnvironmentID   string `json:"environment_id"`
	EnvironmentName string `json:"environment_name"`
	ProjectID       string `json:"project_id"`
	ProjectSlug     string `json:"project_slug"`
	TenantSlug      string `json:"tenant_slug"`
	Backing         bool   `json:"backing"`
}

type ImageFetchRecord struct {
	Requested   string     `json:"requested"`
	Image       string     `json:"image" doc:"Immutable manifest reference selected for this Fetch"`
	TaskID      string     `json:"task_id"`
	RequestedAt string     `json:"requested_at"`
	Status      TaskStatus `json:"status" enum:"pending,running,completed,failed,aborted,timed_out"`
}

type ImageList struct {
	Images     []HostImage `json:"images" nullable:"false"`
	ObservedAt string      `json:"observed_at"`
}

// ImageFetchAccepted identifies the selected content, not a successful fetch.
// Wait for Task completion before using Image in a Service Deploy.
type ImageFetchAccepted struct {
	TaskID       string `json:"task_id"`
	Image        string `json:"image" doc:"Immutable registry reference selected for this operation"`
	ConfigDigest string `json:"config_digest" doc:"Pinned OCI configuration digest, verified before Task completion"`
}
