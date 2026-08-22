package app

import (
	"testing"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the public create boundary must retain a secret literal only as
// transient generation input while producing a valid redacted durable shape.
func TestPrepareEntryCreationAcceptsTransientSecretLiteral(t *testing.T) {
	t.Parallel()
	input := apiTypes.EntryCreateRequest{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Type:          "env",
		Key:           "TOKEN",
		Source:        apiTypes.EntrySource{Kind: "literal", Literal: "private"},
		Exposure:      []string{"worker", "api"},
		Secret:        true,
	}
	entry, err := prepareEntryCreation(input)
	if err != nil {
		t.Fatalf("prepareEntryCreation() error = %v", err)
	}
	if entry.Source.Literal != "private" || len(entry.Exposure) != 2 ||
		entry.Exposure[0] != "api" || entry.Exposure[1] != "worker" {
		t.Fatalf("prepareEntryCreation() = %#v", entry)
	}
	persisted := entry
	persisted.Source.Literal = ""
	if err := persisted.Validate(); err != nil {
		t.Fatalf("redacted Entry validation error = %v", err)
	}
}

// Rationale: file ownership is explicit even at zero, and the all-ones uid
// sentinel rejected by the Agent must be rejected before mutation.
func TestPrepareEntryCreationValidatesFileOwnershipRange(t *testing.T) {
	t.Parallel()
	zero := int64(0)
	invalid := int64(1<<32 - 1)
	base := apiTypes.EntryCreateRequest{
		EnvironmentID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Type:          "file", Path: "config/app.ini", UID: &zero, GID: &zero,
		Source:   apiTypes.EntrySource{Kind: "literal", Literal: "enabled=true"},
		Exposure: []string{"all"},
	}
	if _, err := prepareEntryCreation(base); err != nil {
		t.Fatalf("prepareEntryCreation(explicit zero) error = %v", err)
	}
	base.UID = &invalid
	_, err := prepareEntryCreation(base)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindValidationFailed {
		t.Fatalf("prepareEntryCreation(invalid uid) error = %v", err)
	}
}
