package runnercredential

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// StageCompletion supplies the evidence and deletion to publish atomically
// with the exact Cutover journal and its captured Runner ownership. Only the
// digest record is needed to authenticate the committed candidate.
func (repository *Repository) StageCompletion(
	ctx context.Context, expected keyvalue.Versioned[runnerscoped.Migration],
) ([]keyvalue.Condition, keyvalue.Mutation, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return nil, keyvalue.Mutation{}, err
	}
	runnerID := expected.Record.Candidate.RunnerID
	if !validIdentity(runnerID, "") || expected.Revision <= 0 {
		return nil, keyvalue.Mutation{}, errs.New(errs.KindValidationFailed, "Runner credential completion is invalid")
	}
	handoffKey := stageKey(runnerID)
	read, err := repository.read(ctx, []string{runnerKey(runnerID), handoffKey, runnerscoped.Key(runnerID)}, 0)
	if err != nil {
		return nil, keyvalue.Mutation{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	setValue, handoff, journal := read.Values[0], read.Values[1], read.Values[2]
	migration, err := decodeStageJournal(runnerID, journal, expected)
	if err != nil {
		return nil, keyvalue.Mutation{}, err
	}
	if migration.Phase != runnerscoped.Cutover {
		return nil, keyvalue.Mutation{}, credentialConflict()
	}
	indexed, err := repository.readSet(ctx, runnerID, setValue, read.ReadRevision)
	if err != nil {
		return nil, keyvalue.Mutation{}, err
	}
	stage := stageIdentity{MigrationID: migration.ID, RunnerID: runnerID, Epoch: migration.Candidate.RuntimeEpoch}
	receipt, item, err := readStageRecord(handoff, indexed, stage)
	if err != nil {
		return nil, keyvalue.Mutation{}, err
	}
	clear(receipt.Ciphertext)
	conditions := []keyvalue.Condition{compare(handoffKey, handoff), {Key: item.key, ModRevision: item.revision}}
	return conditions, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: handoffKey}, nil
}
