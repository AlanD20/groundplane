package cli

import (
	"strings"
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the CLI must preserve the API's optional granted-fact operand in
// the one flat Entry source rather than inventing a nested wire alias.
func TestBuildEntrySourceIncludesGrantedAttach(t *testing.T) {
	t.Parallel()
	source, err := buildEntrySource(entrySourceOptions{
		factAttach: "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		factGrant:  "att_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		factKey:    "pg16_URL",
		factSet:    true,
	})
	if err != nil || source.Kind != "fact" || source.GrantAttachID == "" {
		t.Fatalf("buildEntrySource() = %#v, %v", source, err)
	}
}

// Rationale: Entry stdin/file input must enforce its own 256 KiB API ceiling
// before an idempotent request containing plaintext is constructed.
func TestReadEntryValueRejectsOversizedInput(t *testing.T) {
	t.Parallel()
	_, err := readEntryValue("-", strings.NewReader(strings.Repeat("x", apiTypes.MaximumEntryValueBytes+1)))
	kind, _ := errs.KindOf(err)
	if kind != errs.KindValidationFailed {
		t.Fatalf("readEntryValue(oversized) error = %v", err)
	}
}
