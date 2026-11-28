package runnercredential

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	identity "github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	stageKind     = "runner_credential_stage"
	stageSealKind = "runner_credential_stage_secret"
)

type StageCipher interface {
	Seal(context.Context, []byte) ([]byte, error)
	Open(context.Context, []byte) ([]byte, error)
}

type MintCredential func() (string, [sha256.Size]byte, error)

type stageIdentity struct {
	MigrationID string `json:"migration_id"`
	RunnerID    string `json:"runner_id"`
	Epoch       uint64 `json:"epoch"`
	Digest      string `json:"digest"`
}

type stagedCredential struct {
	Identity   stageIdentity `json:"identity"`
	Ciphertext []byte        `json:"ciphertext"`
}

type stageSecret struct {
	Identity stageIdentity `json:"identity"`
	Bearer   []byte        `json:"bearer"`
}

func stageKey(runnerID string) string { return "/v1/runner-credentials/migration-stage/" + runnerID }

// StageScopedBase supplies the epoch-bound credential for the captured Runner
// migration. Retry reopens the original handoff. The credential authenticates
// only once that exact candidate epoch owns the live registration.
func (repository *Repository) StageScopedBase(
	ctx context.Context, expected keyvalue.Versioned[runnerscoped.Migration], cipher StageCipher, mint MintCredential,
) (string, identity.Binding, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return "", identity.Binding{}, err
	}
	runnerID := expected.Record.Candidate.RunnerID
	if !validIdentity(runnerID, "") || expected.Revision <= 0 || cipher == nil || mint == nil {
		return "", identity.Binding{}, errs.New(errs.KindValidationFailed, "Runner credential staging is invalid")
	}
	// One retry handles another writer winning this exact stage. Changes to the
	// captured migration revision still fail closed on a fresh read.
	for range 2 {
		bearer, binding, published, err := repository.stageScopedBase(ctx, runnerID, expected, cipher, mint)
		if err != nil || published {
			return bearer, binding, err
		}
	}
	return "", identity.Binding{}, credentialConflict()
}

func (repository *Repository) stageScopedBase(
	ctx context.Context,
	runnerID string,
	expected keyvalue.Versioned[runnerscoped.Migration],
	cipher StageCipher,
	mint MintCredential,
) (string, identity.Binding, bool, error) {
	setKey, handoffKey, journalKey := runnerKey(runnerID), stageKey(runnerID), runnerscoped.Key(runnerID)
	keys := append(registrationKeys(runnerID), setKey, handoffKey, journalKey)
	read, err := repository.read(ctx, keys, 0)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	defer keyvalue.ClearValues(read.Values)
	setValue, handoff, journal := read.Values[3], read.Values[4], read.Values[5]
	migration, err := decodeStageJournal(runnerID, journal, expected)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	conditions, live, err := liveRegistration(read.Values, runnerID, migration.Legacy.Plan.RuntimeEpoch)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	if !live {
		return "", identity.Binding{}, false, credentialConflict()
	}
	indexed, err := repository.readSet(ctx, runnerID, setValue, read.ReadRevision)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	stage := stageIdentity{MigrationID: migration.ID, RunnerID: runnerID, Epoch: migration.Candidate.RuntimeEpoch}
	if handoff != nil {
		bearer, binding, err := reopenStage(ctx, handoff, indexed, stage, cipher)
		return bearer, binding, true, err
	}
	conditions = append(conditions, compare(setKey, setValue), compare(handoffKey, nil), compare(journalKey, journal))
	return repository.publishStage(ctx, stage, indexed, conditions, cipher, mint)
}

func reopenStage(
	ctx context.Context,
	handoff *keyvalue.KeyValue,
	indexed []indexedCredential,
	stage stageIdentity,
	cipher StageCipher,
) (string, identity.Binding, error) {
	receipt, item, err := readStageRecord(handoff, indexed, stage)
	if err != nil {
		return "", identity.Binding{}, err
	}
	defer clear(receipt.Ciphertext)
	plaintext, err := cipher.Open(ctx, receipt.Ciphertext)
	if err != nil {
		return "", identity.Binding{}, errs.New(errs.KindInternal, "Runner credential handoff could not be opened")
	}
	defer clear(plaintext)
	secret, err := recordcodec.Decode[stageSecret](plaintext, stageSealKind)
	defer clear(secret.Bearer)
	if err != nil || secret.Identity != receipt.Identity || len(secret.Bearer) == 0 {
		return "", identity.Binding{}, corrupt()
	}
	return string(secret.Bearer), item.record.binding(item.revision), nil
}

func readStageRecord(
	handoff *keyvalue.KeyValue,
	indexed []indexedCredential,
	stage stageIdentity,
) (stagedCredential, indexedCredential, error) {
	if handoff == nil {
		return stagedCredential{}, indexedCredential{}, credentialConflict()
	}
	if len(handoff.Value) > maximumRecordBytes {
		return stagedCredential{}, indexedCredential{}, corrupt()
	}
	receipt, err := recordcodec.Decode[stagedCredential](handoff.Value, stageKind)
	stage.Digest = receipt.Identity.Digest
	if err != nil || receipt.Identity != stage || !recordcodec.ValidNonZeroSHA256(stage.Digest) ||
		len(receipt.Ciphertext) == 0 {
		clear(receipt.Ciphertext)
		return stagedCredential{}, indexedCredential{}, corrupt()
	}
	for _, item := range indexed {
		if item.record.Digest != stage.Digest {
			continue
		}
		if !item.record.EpochBound || item.record.RunnerEpoch != stage.Epoch || item.revision != handoff.ModRevision {
			clear(receipt.Ciphertext)
			return stagedCredential{}, indexedCredential{}, corrupt()
		}
		return receipt, item, nil
	}
	clear(receipt.Ciphertext)
	return stagedCredential{}, indexedCredential{}, credentialConflict()
}

func decodeStageJournal(
	runnerID string,
	journal *keyvalue.KeyValue,
	expected keyvalue.Versioned[runnerscoped.Migration],
) (runnerscoped.Migration, error) {
	if journal == nil || journal.ModRevision != expected.Revision {
		return runnerscoped.Migration{}, credentialConflict()
	}
	migration, err := runnerscoped.Decode(journal.Value, runnerID)
	if err != nil {
		return runnerscoped.Migration{}, err
	}
	if (migration.Phase != runnerscoped.Pending && migration.Phase != runnerscoped.Cutover) ||
		migration.ID != expected.Record.ID || migration.Candidate.Digest() != expected.Record.Candidate.Digest() {
		return runnerscoped.Migration{}, credentialConflict()
	}
	return migration, nil
}

func (repository *Repository) publishStage(
	ctx context.Context,
	stage stageIdentity,
	indexed []indexedCredential,
	conditions []keyvalue.Condition,
	cipher StageCipher,
	mint MintCredential,
) (string, identity.Binding, bool, error) {
	set := credentialSet{RunnerID: stage.RunnerID}
	var mutations []keyvalue.Mutation
	for _, item := range indexed {
		switch {
		case item.record.RunnerEpoch == stage.Epoch && item.record.RequestID == "":
			return "", identity.Binding{}, false, credentialConflict()
		case item.record.RunnerEpoch < stage.Epoch-1 || !item.record.binding(item.revision).ActiveAt(repository.now()):
			conditions = append(conditions, keyvalue.Condition{Key: item.key, ModRevision: item.revision})
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: item.key})
		default:
			set.Digests = append(set.Digests, item.record.Digest)
		}
	}
	if len(set.Digests) >= maximumCredentials {
		return "", identity.Binding{}, false, errs.New(errs.KindResourceInUse, "Runner credential capacity is in use")
	}
	bearer, digest, err := mint()
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	stage.Digest = hex.EncodeToString(digest[:])
	record := credentialRecord{
		Digest:      stage.Digest,
		RunnerID:    stage.RunnerID,
		RunnerEpoch: stage.Epoch,
		EpochBound:  true,
	}
	if bearer == "" || !validCredentialRecord(record) {
		return "", identity.Binding{}, false, corrupt()
	}
	credentialBytes, err := recordcodec.Encode(credentialKind, record)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	set.Digests = append(set.Digests, stage.Digest)
	setBytes, err := recordcodec.Encode(credentialSetKind, set)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	handoffBytes, err := sealStage(ctx, stage, bearer, cipher)
	if err != nil {
		return "", identity.Binding{}, false, err
	}
	defer clear(handoffBytes)
	tokenKey := credentialKey(digest)
	conditions = append(conditions, compare(tokenKey, nil))
	mutations = append(mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: runnerKey(stage.RunnerID), Value: setBytes},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: tokenKey, Value: credentialBytes},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: stageKey(stage.RunnerID), Value: handoffBytes},
	)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil || !result.Succeeded {
		return "", identity.Binding{}, false, err
	}
	if result.Revision <= 0 {
		return "", identity.Binding{}, false, corrupt()
	}
	return bearer, record.binding(result.Revision), true, nil
}

func sealStage(ctx context.Context, stage stageIdentity, bearer string, cipher StageCipher) ([]byte, error) {
	secret := stageSecret{Identity: stage, Bearer: []byte(bearer)}
	defer clear(secret.Bearer)
	plaintext, err := recordcodec.Encode(stageSealKind, secret)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Runner credential handoff could not be encoded")
	}
	defer clear(plaintext)
	ciphertext, err := cipher.Seal(ctx, plaintext)
	defer clear(ciphertext)
	if err != nil || len(ciphertext) == 0 || len(ciphertext) > maximumRecordBytes/2 {
		return nil, errs.New(errs.KindInternal, "Runner credential handoff could not be sealed")
	}
	return recordcodec.Encode(stageKind, stagedCredential{Identity: stage, Ciphertext: ciphertext})
}
