package registryimages

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
)

// Index grouping must not hide a separately tagged image or assign a shared
// platform image arbitrarily to one of several indexes. Exact removal stays separate.
func TestImageParentSelectionPreservesIndependentImages(t *testing.T) {
	for _, scenario := range []string{"unique", "tagged", "shared"} {
		t.Run(scenario, func(t *testing.T) {
			images := []imagefetch.LocalImage{{ID: "index", ContentIDs: []string{"index", "child"}}, {ID: "child"}}
			if scenario == "tagged" {
				images[1].Tags = []string{"postgres:16-alpine"}
			}
			if scenario == "shared" {
				images = append(images, imagefetch.LocalImage{ID: "other", ContentIDs: []string{"other", "child"}})
			}
			assignImageParents(images)
			want := ""
			if scenario == "unique" {
				want = "index"
			}
			if images[1].ParentID != want {
				t.Fatalf("parent=%q, want %q", images[1].ParentID, want)
			}
		})
	}
}
