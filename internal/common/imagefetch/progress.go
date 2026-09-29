package imagefetch

// Progress is an observation, never execution authority. Byte totals cover only
// layers Docker has reported so far and exclude cached layers.
type Progress struct {
	Phase           string `json:"phase,omitempty"`
	DownloadedBytes int64  `json:"downloaded_bytes,omitempty"`
	TotalBytes      int64  `json:"total_bytes,omitempty"`
	ErrorCode       string `json:"error_code,omitempty"`
	ErrorDetail     string `json:"error_detail,omitempty"`
}

type Reporter func(Progress) error
