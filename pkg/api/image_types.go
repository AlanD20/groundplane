package api

type ImageFetchRequest struct {
	Image string `json:"image" minLength:"1" maxLength:"512" doc:"Explicit tag or SHA-256 digest in a public registry or GP's managed private registry"`
}

// HostImage is a live local-daemon observation. Containers includes stopped
// containers; it is not a claim that the image is safe to delete.
type HostImage struct {
	ID             string   `json:"id"`
	Tags           []string `json:"tags" nullable:"false"`
	Digests        []string `json:"digests" nullable:"false"`
	SizeBytes      int64    `json:"size_bytes"`
	CreatedAt      string   `json:"created_at"`
	Containers     int      `json:"containers"`
	RemovalBlocked string   `json:"removal_blocked" doc:"Empty when current observation permits removal; admission and execution recheck protection"`
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
