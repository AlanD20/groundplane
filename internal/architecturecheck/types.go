package architecturecheck

// Limits are the physical line limits for production and test source files.
type Limits struct {
	Production int `json:"production"`
	Test       int `json:"test"`
}

// FileLines records a path and its accepted physical line count.
type FileLines struct {
	Path  string `json:"path"`
	Lines int    `json:"lines"`
}

// OversizedFile is an accepted line count for a file over its normal limit.
type OversizedFile = FileLines

// FrozenTotal is an exact physical line total for one direct Go package.
type FrozenTotal = FileLines

// LegacyFinding records one exact pre-Q0 architecture violation.
type LegacyFinding struct {
	Path    string `json:"path"`
	Rule    string `json:"rule"`
	Subject string `json:"subject"`
	Reason  string `json:"reason"`
}

// Baseline is the versioned architecture-check baseline document.
type Baseline struct {
	Version        int             `json:"version"`
	Limits         Limits          `json:"limits"`
	OversizedFiles []OversizedFile `json:"oversized_files"`
	FrozenTotals   []FrozenTotal   `json:"frozen_totals"`
	LegacyFindings []LegacyFinding `json:"legacy_findings"`
}

// Finding is one deterministic architecture-check diagnostic.
type Finding struct {
	Path    string `json:"path"`
	Line    int    `json:"line"`
	Column  int    `json:"column"`
	Rule    string `json:"rule"`
	Subject string `json:"subject,omitempty"`
	Message string `json:"message"`
}
