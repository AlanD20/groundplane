package api

type ImageFetchRequest struct {
	Image string `json:"image" minLength:"1" maxLength:"512" doc:"Explicit tag or SHA-256 digest in the managed private registry"`
}

// ImageFetchAccepted identifies the selected content, not a successful fetch.
// Wait for Task completion before using Image in a Service Deploy.
type ImageFetchAccepted struct {
	TaskID  string `json:"task_id"`
	Image   string `json:"image" doc:"Immutable registry reference selected for this operation"`
	ImageID string `json:"image_id" doc:"Expected host image ID, verified before Task completion"`
}
