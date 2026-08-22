package hierarchy

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
)

func TestServiceCreatesOptionalTenantDescription(t *testing.T) {
	t.Parallel()

	repository := &repositoryStub{
		createTenant: func(_ context.Context, record core.Tenant) (Versioned[core.Tenant], error) {
			return Versioned[core.Tenant]{Record: record, Revision: 1, ReadRevision: 1}, nil
		},
	}
	description := "Production workloads"
	created, err := mustService(t, repository).CreateTenant(context.Background(), CreateTenantInput{
		Slug: "acme", Description: description,
	})
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if created.Record.Name != "acme" || created.Record.Description != description {
		t.Fatalf("created Tenant = %#v", created.Record)
	}
}
