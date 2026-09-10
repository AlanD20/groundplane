package controllerupgrade

import (
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Release retains a validated manifest without requiring old staging bytes to
// remain present forever. The manifest's byte identity is independently checked.
type Release struct {
	Release  Digest   `json:"release"`
	Manifest Manifest `json:"manifest"`
}

func (release Release) Validate() error {
	raw, err := json.Marshal(release.Manifest)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil {
		return err
	}
	_, err = ParseManifest(canonical, release.Release)
	return err
}
