package api

type ReleaseGroupRollbackRequest struct {
	Tag             *string `json:"tag,omitempty"`
	PreviewRevision *string `json:"preview_revision,omitempty" pattern:"^[1-9][0-9]*$"`
}

type ReleaseGroupRollbackSource struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
	Tag       string `json:"tag"`
}

type ReleaseGroupRollbackPreview struct {
	ReleaseGroupID string                       `json:"release_group_id"`
	Revision       string                       `json:"revision" pattern:"^[1-9][0-9]*$"`
	Sources        []ReleaseGroupRollbackSource `json:"sources"`
}
