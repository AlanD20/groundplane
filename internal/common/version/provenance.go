package version

import "github.com/AlanD20/groundplane/pkg/errs"

// SourceKind and SourceCommit are injected by the build that owns the resolved
// checkout. They are independent of version labels and artifact digests.
var SourceKind, SourceCommit string

type BuildProvenance struct {
	Kind   string `json:"kind" enum:"source_ref,release"`
	Commit string `json:"commit" minLength:"40" maxLength:"40"`
}

func Provenance() BuildProvenance {
	return BuildProvenance{Kind: SourceKind, Commit: SourceCommit}
}

func (source BuildProvenance) Known() bool { return source.Kind != "" }

func (source BuildProvenance) Validate() error {
	if source.Kind == "" && source.Commit == "" {
		return nil // Unlabelled local development builds have no recorded provenance.
	}
	if (source.Kind != "source_ref" && source.Kind != "release") || len(source.Commit) != 40 {
		return errs.New(errs.KindValidationFailed, "build provenance requires its source kind and exact commit")
	}
	for _, character := range source.Commit {
		if !(character >= 'a' && character <= 'f' || character >= '0' && character <= '9') {
			return errs.New(errs.KindValidationFailed, "build provenance commit is invalid")
		}
	}
	return nil
}
