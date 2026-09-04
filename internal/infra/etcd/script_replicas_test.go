package etcd

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: direct Script creation targets an operator-owned logical Service
// once and must remain valid when that Service has multiple replicas.
func TestScriptRepositoryAcceptsReplicatedLogicalService(t *testing.T) {
	ctx := context.Background()
	_, store, environment, project, target := routeRepositoryTestHierarchy(t)
	target.Record.Desired.Replicas = 3
	repository, err := newScriptRepository(store)
	if err != nil {
		t.Fatalf("newScriptRepository() error = %v", err)
	}
	record, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
		ID: ids.New(ids.KindScript), Slug: "release", ServiceName: target.Record.Desired.Name,
		Body: "printf release", When: core.ScriptManual,
	})
	if err != nil {
		t.Fatalf("NewScriptRecord() error = %v", err)
	}
	created, err := repository.CreateScript(ctx, environment, project, target, record)
	if err != nil {
		t.Fatalf("CreateScript() error = %v", err)
	}
	if created.Record.ServiceID != target.Record.Desired.ID {
		t.Fatalf("created Script target = %q, want %q", created.Record.ServiceID, target.Record.Desired.ID)
	}
}

// Rationale: replica eligibility broadens only to positive operator workloads;
// stopped and adapter-managed backing targets remain invalid API hierarchy.
func TestScriptRepositoryRejectsIneligibleLogicalServices(t *testing.T) {
	tests := map[string]func(*Versioned[ProjectRecord], *Versioned[ServiceRecord]){
		"zero replicas": func(_ *Versioned[ProjectRecord], target *Versioned[ServiceRecord]) {
			target.Record.Desired.Replicas = 0
		},
		"negative replicas": func(_ *Versioned[ProjectRecord], target *Versioned[ServiceRecord]) {
			target.Record.Desired.Replicas = -1
		},
		"managed backing": func(project *Versioned[ProjectRecord], target *Versioned[ServiceRecord]) {
			project.Record.Kind = ProjectKindBacking
			project.Record.TenantID = ""
			target.Record.Desired.Adapter = "postgres:16"
			target.Record.BackingNetworkID = ids.New(ids.KindNetwork)
		},
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			_, store, environment, project, target := routeRepositoryTestHierarchy(t)
			change(&project, &target)
			repository, err := newScriptRepository(store)
			if err != nil {
				t.Fatalf("newScriptRepository() error = %v", err)
			}
			record, err := NewScriptRecord(environment.Record.ID, target.Record.Desired.ID, core.Script{
				ID: ids.New(ids.KindScript), Slug: "release", ServiceName: target.Record.Desired.Name,
				Body: "true", When: core.ScriptManual,
			})
			if err != nil {
				t.Fatalf("NewScriptRecord() error = %v", err)
			}
			_, err = repository.CreateScript(ctx, environment, project, target, record)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("CreateScript() error = %v, want validation.failed", err)
			}
		})
	}
}
