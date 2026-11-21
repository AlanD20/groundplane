package blueprint

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
)

// A Tunnel must pin an exact protected generation, reject empty setup values,
// and never read a token from another Environment or a workload-wide grant.
func TestComponentEntryCaptureScopeAndGeneration(t *testing.T) {
	environmentID := ids.New(ids.KindEnvironment)
	entry := entries.Record{EnvironmentID: environmentID,
		CurrentValueGenerationID: ids.New(ids.KindConfig),
		Entry: core.EnvEntry{ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv,
			Key: "CLOUDFLARE_TUNNEL_TOKEN", Secret: true,
			Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{core.EntryExposureEdgeTunnel}},
	}
	for _, name := range []string{"valid", "empty", "other-environment", "workloads", "plain", "missing"} {
		t.Run(name, func(t *testing.T) {
			candidate := entry
			resolver := &environmentBlueprintMaterializationResolverFake{content: []byte("private-token")}
			service := &Service{materials: resolver}
			switch name {
			case "empty":
				resolver.content = []byte(" \n")
			case "other-environment":
				candidate.EnvironmentID = ids.New(ids.KindEnvironment)
			case "workloads":
				candidate.Entry.Exposure = []string{"all"}
			case "plain":
				candidate.Entry.Secret = false
			case "missing":
				candidate.Entry.ID = ids.New(ids.KindEnvEntry)
			}
			result, err := service.componentEntryReference(
				t.Context(),
				environmentID,
				entry.Entry.ID,
				[]entries.Record{candidate},
			)
			if name != "valid" {
				if err == nil {
					t.Fatal("unsafe token capture accepted")
				}
				if name != "empty" && resolver.source.Kind != "" {
					t.Fatal("unauthorized token was read")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := taskmaterialization.EntryValueReference{
				EntryID:           entry.Entry.ID,
				ValueGenerationID: entry.CurrentValueGenerationID,
				Storage:           taskmaterialization.EntryValueStorageSecret,
			}
			if result != want || resolver.source.EntryValue == nil || *resolver.source.EntryValue != want {
				t.Fatal("token capture did not pin the selected encrypted generation")
			}
		})
	}
}
