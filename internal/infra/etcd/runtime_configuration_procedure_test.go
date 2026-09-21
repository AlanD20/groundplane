package etcd

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SVC-15/JOURNEY-02: a publication/claim must not bind a valid Task source set
// to another plan's snapshot or omit one of its file write/recovery steps.
func TestRuntimeConfigurationProcedureRejectsDivergentTaskAuthority(t *testing.T) {
	task := configurationTaskFixture()
	store := newMemoryHierarchyStore()
	seed, err := store.Transact(
		context.Background(),
		nil,
		[]testkeyvalue.Mutation{
			{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: []byte("seed")},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	task, err = prepareRuntimeConfigurationTask(context.Background(), store, task, seed.Revision)
	if err != nil {
		t.Fatal(err)
	}
	prior := task.Configuration.Current
	task.Configuration.Prior, task.Configuration.PriorRevision = &prior, seed.Revision
	digest, err := hex.DecodeString(prior.SHA256)
	if err != nil {
		t.Fatal(err)
	}
	procedure := &agentpb.CandidateReleaseProcedure{ConfigurationRestoration: &agentpb.ConfigurationRestoration{
		PriorSnapshotId: prior.ID, PriorSnapshotSha256: digest}}
	steps := make(map[string]struct{})
	for _, record := range task.Materializations {
		file := &agentpb.ConfigurationFileRestoration{ForwardStepId: record.StepID,
			ProbeStepId: ids.New(ids.KindStep), CompensateStepId: ids.New(ids.KindStep)}
		procedure.ConfigurationRestoration.Files = append(procedure.ConfigurationRestoration.Files, file)
		for _, id := range []string{file.ForwardStepId, file.ProbeStepId, file.CompensateStepId} {
			steps[id] = struct{}{}
		}
	}
	if err := validateTaskConfigurationProcedure(task, procedure, steps); err != nil {
		t.Fatal(err)
	}
	for _, scenario := range []string{"snapshot", "digest", "missing procedure", "missing file", "foreign forward", "absent step"} {
		t.Run(scenario, func(t *testing.T) {
			changed := proto.CloneOf(procedure)
			selected := make(map[string]struct{}, len(steps))
			for id := range steps {
				selected[id] = struct{}{}
			}
			switch scenario {
			case "snapshot":
				changed.ConfigurationRestoration.PriorSnapshotId = ids.New(ids.KindConfig)
			case "digest":
				changed.ConfigurationRestoration.PriorSnapshotSha256[0] ^= 1
			case "missing procedure":
				changed.ConfigurationRestoration = nil
			case "missing file":
				changed.ConfigurationRestoration.Files = changed.ConfigurationRestoration.Files[1:]
			case "foreign forward":
				changed.ConfigurationRestoration.Files[0].ForwardStepId = ids.New(ids.KindStep)
			case "absent step":
				delete(selected, changed.ConfigurationRestoration.Files[0].CompensateStepId)
			}
			if err := validateTaskConfigurationProcedure(task, changed, selected); err == nil {
				t.Fatal("divergent authority accepted")
			}
		})
	}
}
