package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Publication-shape tests use real bodyless primary metadata and wire context;
// their checkpoint records still do not pretend to be full executable plans.
func scriptContextBlueprintPrimaryFixture(
	t *testing.T, store *memoryHierarchyStore, execution ScriptExecutionRecord, explicit bool,
) ReleaseHookExecutionPublication {
	t.Helper()
	key := scriptSetScriptKey(execution.EnvironmentID, execution.ScriptSetGeneration, execution.ScriptID)
	read, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	var record ScriptRecord
	if read.Entry != nil {
		record, err = decodeScriptRecord(read.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		record.Desired.Body = "exit 0"
	} else {
		record, err = NewScriptRecord(execution.EnvironmentID, execution.ServiceID, core.Script{
			ID: execution.ScriptID, Slug: "setup", ServiceName: "worker", When: core.ScriptPreDeploy, Body: "exit 0",
		})
		if err != nil {
			t.Fatal(err)
		}
		record.ScriptSetGeneration = execution.ScriptSetGeneration
		// Synthetic shape fixtures have no body membership lifecycle; represent
		// already prepared ownership. Existing real primaries keep their count.
		record.ActiveReferences = 1
	}
	if explicit {
		record.Desired.Execution = &core.ScriptExecution{
			Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
		}
	}
	revision := writeScriptContextPrimaryFixture(t, store, record)
	body := scriptBlueprintGeneration(record)
	execution.ScriptGeneration, execution.BodySHA256 = record.ActiveGeneration, body.BodySHA256
	execution = withScriptContextSnapshot(t, execution)
	sources := ScriptExecutionSources{
		Revision:       revision,
		Script:         Versioned[ScriptRecord]{Record: record, Revision: revision, ReadRevision: revision},
		BodyGeneration: Versioned[ScriptBodyGenerationRecord]{Record: body},
	}
	sources.Script.Record.Desired.Body = ""
	if explicit {
		sources.Release.Intent.CandidateWorkload.LocalImageID = "sha256:" + strings.Repeat("a", 64)
		var snapshot agentpb.ResolvedRunnerSnapshot
		if err := proto.Unmarshal(execution.Snapshot, &snapshot); err != nil {
			t.Fatal(err)
		}
		captured, err := scriptContextFromStoredDesired(record)
		if err != nil {
			t.Fatal(err)
		}
		digest, err := executionplan.ScriptExecutionContextDigest(captured)
		if err != nil {
			t.Fatal(err)
		}
		snapshot.LocalImageId = "sha256:" + strings.Repeat("b", 64)
		snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
			Context: captured, ContextSha256: digest, ScriptModRevision: uint64(revision),
			ReleaseLocalImageId: sources.Release.Intent.CandidateWorkload.LocalImageID,
		}
		execution.Snapshot, err = proto.Marshal(&snapshot)
		if err != nil {
			t.Fatal(err)
		}
		execution.SnapshotSHA256 = scriptSourceReferenceBytesDigest(execution.Snapshot)
	}
	return ReleaseHookExecutionPublication{Sources: sources, Execution: execution}
}

func writeScriptContextPrimaryFixture(t *testing.T, store *memoryHierarchyStore, record ScriptRecord) int64 {
	t.Helper()
	value, err := encodeScriptRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(value)
	key := scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.ID)
	result, err := store.Transact(context.Background(), nil, []Mutation{{Type: MutationPut, Key: key, Value: value}})
	if err != nil || !result.Succeeded {
		t.Fatalf("write Script primary fixture: %v", err)
	}
	return result.Revision
}
