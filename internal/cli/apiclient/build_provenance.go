package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func buildProvenanceFromGenerated(source *generated.BuildProvenance) (*version.BuildProvenance, error) {
	if source == nil {
		return nil, nil
	}
	result := &version.BuildProvenance{Kind: string(source.Kind), Commit: source.Commit}
	if !source.Kind.Valid() || !result.Known() || result.Validate() != nil {
		return nil, errs.New(errs.KindInternal, "apiclient: installed build provenance is invalid")
	}
	return result, nil
}
