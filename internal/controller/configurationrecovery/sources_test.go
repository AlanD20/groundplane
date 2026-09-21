package configurationrecovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	runtimeconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	testtaskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SVC-15/JOURNEY-02: a failed edit must use the preceding value and metadata,
// including after reconstruction, without replacing its retained content key.
func TestPinnedFileSourcesPreservePriorContentIdentityAcrossReconstruction(t *testing.T) {
	t.Parallel()
	task, reader := sourceFixture()
	sources, err := NewSources(reader)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := sources.Prepare(context.Background(), task)
	if err != nil || len(prepared.Files) != 1 {
		t.Fatalf("prepare: %#v/%v", prepared, err)
	}
	file := prepared.Files[0]
	if file.Original.StepID != reader.snapshot.Files[0].StepID ||
		file.Original.MaterializationID != reader.snapshot.Files[0].MaterializationID ||
		file.Probe.UID != 101 || file.Probe.GID != 102 || file.Probe.Mode != 0o444 ||
		file.Probe.SHA256 != sourceDigest("working") || file.Probe.Length != 7 ||
		file.Probe.Source.EntryValue.ValueGenerationID != reader.snapshot.Files[0].Source.EntryValue.ValueGenerationID {
		t.Fatal("recovery replaced the original content identity or prior bytes/metadata")
	}
	if file.Probe.StepID == file.Original.StepID || file.Probe.StepID == file.Compensate.StepID ||
		file.Probe.MaterializationID == file.Original.MaterializationID || file.Probe.MaterializationID == file.Compensate.MaterializationID {
		t.Fatal("recovery reused a consumed execution identity")
	}
	rebuilt, err := sources.Prepare(context.Background(), task)
	if err != nil || !proto.Equal(prepared.Procedure, rebuilt.Procedure) ||
		rebuilt.Files[0].Probe.MaterializationID != file.Probe.MaterializationID {
		t.Fatalf("restart changed recovery authority: %v", err)
	}
	plan := &agentpb.ExecutionPlan{
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: prepared.Procedure},
	}
	for _, stepID := range []string{file.Probe.StepID, file.Compensate.StepID} {
		original, metadata, err := sources.Resolve(context.Background(), task, plan, stepID)
		if err != nil || original.MaterializationID != file.Original.MaterializationID || metadata.StepID != stepID {
			t.Fatalf("resolve retained source: %v", err)
		}
	}
	file.Probe.Source.EntryValue.ValueGenerationID = "caller mutation"
	if reader.snapshot.Files[0].Source.EntryValue.ValueGenerationID == "caller mutation" ||
		file.Compensate.Source.EntryValue.ValueGenerationID == "caller mutation" {
		t.Fatal("retained source aliases execution metadata")
	}
}

// SVC-15: missing sources, foreign Environment ownership and a plan that
// selects another snapshot cannot authorize restoration. Initial absence is
// explicit and never needs a lookup that could silently choose current state.
func TestPinnedFileSourcesRejectMissingAndForeignAuthority(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"missing source", "foreign owner", "changed plan", "unbound step", "initial absence"} {
		t.Run(scenario, func(t *testing.T) {
			task, reader := sourceFixture()
			sources, err := NewSources(reader)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := sources.Prepare(context.Background(), task)
			if err != nil {
				t.Fatal(err)
			}
			plan := &agentpb.ExecutionPlan{
				CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{
					ConfigurationRestoration: prepared.Procedure,
				},
			}
			stepID := prepared.Files[0].Probe.StepID
			switch scenario {
			case "missing source":
				reader.fail = true
			case "foreign owner":
				reader.snapshot.EnvironmentID = ids.New(ids.KindEnvironment)
			case "changed plan":
				prepared.Procedure.PriorSnapshotSha256[0] ^= 1
			case "unbound step":
				stepID = task.Materializations[0].StepID
			case "initial absence":
				task.Configuration.Prior, task.Configuration.PriorRevision = nil, 0
				reader.fail = true
				absent, err := sources.Prepare(context.Background(), task)
				if err != nil || len(absent.Files) != 1 ||
					absent.Files[0].Probe.Source.Kind != taskmaterialization.SourceRemoval ||
					absent.Files[0].Probe.OutputKind != taskmaterialization.OutputRemovePlainFile ||
					absent.Files[0].Probe.Length != 0 ||
					absent.Files[0].Probe.SHA256 != sourceDigest("") {
					t.Fatalf("explicit initial absence: %#v/%v", absent, err)
				}
				return
			}
			if _, _, err := sources.Resolve(context.Background(), task, plan, stepID); err == nil {
				t.Fatal("invalid authority allowed a recovery source")
			}
		})
	}
}

type sourceReader struct {
	reference runtimeconfiguration.Reference
	snapshot  runtimeconfiguration.Snapshot
	fail      bool
}

func (reader *sourceReader) LoadRetained(
	_ context.Context,
	reference runtimeconfiguration.Reference,
) (runtimeconfiguration.Snapshot, error) {
	if reader.fail || reference != reader.reference {
		return runtimeconfiguration.Snapshot{}, errs.New(errs.KindStateConflict, "retained source unavailable")
	}
	return reader.snapshot, nil
}

func sourceFixture() (etcd.TaskRecord, *sourceReader) {
	at := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	environmentID := ids.New(ids.KindEnvironment)
	prior := taskmaterialization.Record{StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID: environmentID, Destination: "config/application", OutputKind: taskmaterialization.OutputPlainFile,
		UID: 101, GID: 102, Mode: 0o444, Length: 7, SHA256: sourceDigest("working"),
		Source: taskmaterialization.Source{Kind: taskmaterialization.SourceEntryValue,
			EntryValue: &taskmaterialization.EntryValueReference{EntryID: ids.New(ids.KindEnvEntry),
				ValueGenerationID: ids.New(ids.KindConfig), Storage: taskmaterialization.EntryValueStoragePlain}}}
	reference := runtimeconfiguration.Reference{
		ID:            ids.New(ids.KindConfig),
		EnvironmentID: environmentID,
		Generation:    1,
		SHA256:        sourceDigest("snapshot manifest"),
	}
	reader := &sourceReader{reference: reference, snapshot: runtimeconfiguration.Snapshot{
		ID: reference.ID, EnvironmentID: environmentID, Generation: 1, Files: []taskmaterialization.Record{prior}}}
	next := taskmaterialization.Clone([]taskmaterialization.Record{prior})[0]
	next.StepID, next.MaterializationID = ids.New(ids.KindStep), ids.New(ids.KindConfig)
	next.Source.EntryValue.ValueGenerationID = ids.New(ids.KindConfig)
	next.UID, next.GID, next.Mode, next.Length, next.SHA256 = 201, 202, 0o444, 6, sourceDigest("broken")
	current := runtimeconfiguration.Reference{ID: ids.New(ids.KindConfig), EnvironmentID: environmentID, Generation: 2,
		SHA256: sourceDigest("candidate manifest")}
	return etcd.TaskRecord{ID: ids.New(ids.KindTask), PlanID: ids.New(ids.KindPlan), CreatedAt: at, RenderGeneration: 2,
		Owner: testtaskjournal.TaskOwner{
			EnvironmentID: environmentID,
		}, Materializations: []taskmaterialization.Record{next},
		Configuration: &testtaskconfiguration.TaskConfiguration{
			Current:       current,
			Prior:         &reference,
			PriorRevision: 7,
		}}, reader
}

func sourceDigest(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
