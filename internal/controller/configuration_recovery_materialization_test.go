package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/configurationrecovery"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-15/JOURNEY-02: new recovery execution IDs must still resolve the old
// Component's retained bytes. Changed transfer metadata or missing bytes must
// fail instead of rerendering or reading the failed candidate's output.
func TestRecoveryMaterializationDeliversOriginalContentForNewExecutionIDs(t *testing.T) {
	t.Parallel()
	expected := []byte("original config")
	digest := sha256.Sum256(expected)
	old := etcd.TaskMaterializationRecord{StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
		EnvironmentID: resolverEnvironmentID, Destination: "components/caddy/Caddyfile",
		OutputKind: etcd.TaskMaterializationOutputPlainFile, UID: 101, GID: 102, Mode: 0o444,
		Length: uint64(len(expected)), SHA256: hex.EncodeToString(digest[:]),
		Source: etcd.TaskMaterializationSource{Kind: etcd.TaskMaterializationSourceComponentFile,
			ComponentFile: &etcd.TaskComponentFileValueReference{RevisionID: ids.New(ids.KindTask),
				ComponentID: ids.New(ids.KindComponent), Path: "components/caddy/Caddyfile"}}}
	reference := runtimeconfiguration.Reference{ID: ids.New(ids.KindConfig), EnvironmentID: resolverEnvironmentID,
		Generation: 1, SHA256: hex.EncodeToString(digest[:])}
	reader := &recoverySnapshotReader{reference: reference, snapshot: runtimeconfiguration.Snapshot{
		ID: reference.ID, EnvironmentID: resolverEnvironmentID, Generation: 1, Files: []etcd.TaskMaterializationRecord{old}}}
	sources, err := configurationrecovery.NewSources(reader)
	if err != nil {
		t.Fatal(err)
	}
	resolver := testTaskMaterializationResolver(t, nil)
	if err := resolver.EnableConfigurationRecovery(reader); err != nil {
		t.Fatal(err)
	}
	content := &resolverRetainedContent{content: expected}
	if err := resolver.EnableComponentMaterializationContent(content); err != nil {
		t.Fatal(err)
	}
	changed := old
	changed.StepID, changed.MaterializationID, changed.UID = ids.New(ids.KindStep), ids.New(ids.KindConfig), 201
	current := reference
	current.ID, current.Generation = ids.New(ids.KindConfig), 2
	planHash := sha256.Sum256([]byte("source resolver fixture"))
	task := etcd.TaskRecord{
		ID:               ids.New(ids.KindTask),
		PlanID:           resolverPlanID,
		PlanHash:         hex.EncodeToString(planHash[:]),
		CreatedAt:        time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC),
		RenderGeneration: 2,
		Owner: etcd.TaskOwner{
			EnvironmentID: resolverEnvironmentID,
		},
		Materializations: []etcd.TaskMaterializationRecord{changed},
		Configuration:    &etcd.TaskConfiguration{Current: current, Prior: &reference, PriorRevision: 7},
	}
	prepared, err := sources.Prepare(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	plan := &agentpb.ExecutionPlan{PlanId: task.PlanID, PlanHash: planHash[:], RenderGeneration: 2,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: prepared.Procedure}}
	for _, record := range []etcd.TaskMaterializationRecord{prepared.Files[0].Probe, prepared.Files[0].Compensate} {
		step, err := BuildTaskMaterializationStep(record, ids.New(ids.KindConfig), 30)
		if err != nil {
			t.Fatal(err)
		}
		step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE
		if record.StepID == prepared.Files[0].Compensate.StepID {
			step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE
		}
		stream, err := resolver.ResolveMaterialization(context.Background(), task, plan, step)
		if err != nil {
			t.Fatal(err)
		}
		actual, readErr := io.ReadAll(stream)
		closeErr := stream.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(actual, expected) ||
			content.loaded.StepID != old.StepID || content.loaded.MaterializationID != old.MaterializationID {
			t.Fatalf("wrong retained content or identity: %v/%v", readErr, closeErr)
		}
		clear(actual)
		step.GetMaterializeFile().Uid++
		if _, err := resolver.ResolveMaterialization(context.Background(), task, plan, step); err == nil {
			t.Fatal("changed recovery file metadata was accepted")
		}
		step.GetMaterializeFile().Uid--
		content.content = []byte("missing retained output")
		if _, err := resolver.ResolveMaterialization(context.Background(), task, plan, step); err == nil {
			t.Fatal("missing exact content fell back to another source")
		}
		content.content = expected
	}
}

type recoverySnapshotReader struct {
	reference runtimeconfiguration.Reference
	snapshot  runtimeconfiguration.Snapshot
}

func (reader *recoverySnapshotReader) LoadRetained(
	_ context.Context,
	reference runtimeconfiguration.Reference,
) (runtimeconfiguration.Snapshot, error) {
	if reference != reader.reference {
		return runtimeconfiguration.Snapshot{}, errs.New(errs.KindStateConflict, "unexpected recovery snapshot")
	}
	return reader.snapshot, nil
}
