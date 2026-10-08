package api

type BackingAdapterVersion struct {
	Version      string `json:"version"`
	ImagePattern string `json:"image_pattern" doc:"Accepted upstream image references for this server version."`
}

type BackingAdapter struct {
	Key                        string                  `json:"key"`
	Label                      string                  `json:"label"`
	Versions                   []BackingAdapterVersion `json:"versions"`
	RequiresAuthenticationMode bool                    `json:"requires_authentication_mode"`
	Custom                     bool                    `json:"custom"`
}

type BackingAdapterCatalog struct {
	Items []BackingAdapter `json:"items"`
}
