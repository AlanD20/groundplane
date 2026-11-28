package api

type RemoteImageFetchRequest struct {
	After []string `json:"after,omitempty" maxItems:"16" uniqueItems:"true"`
	Image string   `json:"image" minLength:"1" maxLength:"512" doc:"Exact Linux platform manifest digest in a supported public registry"`
}
