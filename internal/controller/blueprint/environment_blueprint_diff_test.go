package blueprint

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/core"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Rationale: a reapply must not advertise existing native resources as new;
// unsupported omission paths cannot promise removal in Validate.
func TestBlueprintDiffRecognizesExistingNativeResources(t *testing.T) {
	current := &composetypes.Project{
		Services: composetypes.Services{"api": {Name: "api"}},
		Networks: composetypes.Networks{"private": {}},
		Volumes:  composetypes.Volumes{"storage": {}},
	}
	for _, included := range []bool{false, true} {
		project := &composetypes.Project{}
		want := apiTypes.BlueprintChangeRetain
		if included {
			project.DisabledServices = composetypes.Services{"api": {Name: "api"}}
			project.Networks = composetypes.Networks{"private": {}}
			project.Volumes = composetypes.Volumes{"storage": {}}
			want = apiTypes.BlueprintChangeUpdate
		}
		changes := environmentBlueprintChanges(blueprintparser.AuthoringDocument{},
			blueprintparser.Result{Project: project}, true, current, nil)
		found := 0
		for _, change := range changes {
			if change.Resource == "compose" {
				continue
			}
			found++
			if change.Action != want {
				t.Fatalf("included=%v: %s %s action=%s, want %s",
					included, change.Resource, change.Key, change.Action, want)
			}
		}
		if found != 3 {
			t.Fatalf("included=%v: found %d native resources, want 3", included, found)
		}
	}
}

// Rationale: a persistent Volume needs its protected removal Task; validating
// a Blueprint without it must fail before Apply can silently preserve it.
func TestBlueprintRequiresExplicitVolumeRemoval(t *testing.T) {
	volumes := []testenvironmentprojection.EnvironmentVolumeIdentity{{Key: "data"}}
	if err := requireExplicitBlueprintVolumes(&composetypes.Project{}, volumes); err == nil {
		t.Fatal("omitted persistent Volume was accepted")
	}
	if err := requireExplicitBlueprintVolumes(&composetypes.Project{
		Volumes: composetypes.Volumes{"data": {}},
	}, volumes); err != nil {
		t.Fatalf("included persistent Volume was rejected: %v", err)
	}
}

// Rationale: Console and CLI Validate must reveal the destructive Entry effect
// that Apply will execute, including one first created through a direct action.
func TestBlueprintDiffReportsOmittedEntryRemoval(t *testing.T) {
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{Entries: map[string]core.EntrySpec{
			"TOKEN": {Kind: core.EntryKindEnv, Secret: true},
		}},
		blueprintparser.Result{Project: &composetypes.Project{}}, true,
		nil, nil,
	)
	for _, change := range changes {
		if change.Resource == "entry" && change.Key == "TOKEN" {
			if change.Action != apiTypes.BlueprintChangeRemove {
				t.Fatalf("omitted Entry action = %s, want remove", change.Action)
			}
			return
		}
	}
	t.Fatal("omitted Entry was not reported")
}

// Rationale: Validate must show the Script removal that Apply publishes, not
// describe an omitted Script as retained.
func TestBlueprintDiffReportsOmittedScriptRemoval(t *testing.T) {
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{Scripts: map[string]core.ScriptSpec{"migrate": {}}},
		blueprintparser.Result{Project: &composetypes.Project{}}, true,
		nil, nil,
	)
	for _, change := range changes {
		if change.Resource == "script" && change.Key == "migrate" {
			if change.Action != apiTypes.BlueprintChangeRemove {
				t.Fatalf("omitted Script action = %s, want remove", change.Action)
			}
			return
		}
	}
	t.Fatal("omitted Script was not reported")
}

// Rationale: Validate must expose native Compose definition removal and
// replacement before Apply drops omitted definitions from the candidate.
func TestBlueprintDiffReportsNativeConfigAndSecretChanges(t *testing.T) {
	current := &composetypes.Project{
		Configs: composetypes.Configs{"old-config": {}},
		Secrets: composetypes.Secrets{"shared-secret": {}},
	}
	candidate := &composetypes.Project{
		Configs: composetypes.Configs{"new-config": {}},
		Secrets: composetypes.Secrets{"shared-secret": {}},
	}
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{}, blueprintparser.Result{Project: candidate}, true,
		current, nil,
	)
	want := map[string]apiTypes.BlueprintChangeAction{
		"config/old-config":    apiTypes.BlueprintChangeRemove,
		"config/new-config":    apiTypes.BlueprintChangeCreate,
		"secret/shared-secret": apiTypes.BlueprintChangeUpdate,
	}
	for _, change := range changes {
		key := change.Resource + "/" + change.Key
		if action, exists := want[key]; exists {
			if change.Action != action {
				t.Fatalf("%s action = %s, want %s", key, change.Action, action)
			}
			delete(want, key)
		}
	}
	if len(want) != 0 {
		t.Fatalf("native Compose review omitted changes: %#v", want)
	}
}

// Rationale: review may label a newly created key-only literal as empty, but
// never infer emptiness from an existing redacted secret declaration.
func TestBlueprintDiffMarksNewEmptySecretLiteral(t *testing.T) {
	changes := environmentBlueprintChanges(
		blueprintparser.AuthoringDocument{},
		blueprintparser.Result{
			Project: &composetypes.Project{},
			Extensions: blueprintparser.Extensions{Entries: map[string]core.EntrySpec{
				"TOKEN": {Kind: core.EntryKindEnv, Secret: true, Source: core.EntrySourceSpec{}},
			}},
		}, false, nil, nil,
	)
	for _, change := range changes {
		if change.Resource == "entry" && change.Key == "TOKEN" {
			if change.Action != apiTypes.BlueprintChangeCreate || !change.EmptySecretValue {
				t.Fatalf("new secret Entry preview = %#v", change)
			}
			return
		}
	}
	t.Fatal("new secret Entry was not reported")
}
