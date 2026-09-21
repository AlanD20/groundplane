package agentregistration

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *Repository) MarkReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	readyAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := recordcodec.ValidateTimestamp("local Agent ready_at", readyAt); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	return repository.transitionPhase(
		ctx,
		agentID,
		generation,
		revision,
		localagentrecord.LocalAgentPhaseProvisioning,
		localagentrecord.LocalAgentPhaseReady,
		false,
		readyAt,
	)
}

// ReplaceGeneration atomically rotates the complete authenticated runtime
// identity while retaining the stable Agent aggregate and first Ready time.
func (repository *Repository) ReplaceGeneration(
	ctx context.Context,
	current etcdstore.Versioned[localagentrecord.LocalAgentRecord],
	image string,
	encryptedToken []byte,
	tokenDigest string,
	updatedAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ID == "" || !imageref.IsDigestPinned(image) ||
		len(encryptedToken) == 0 || !localagentrecord.ValidLocalAgentDigest(tokenDigest) ||
		current.Record.Generation == ^uint64(0) {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"local Agent replacement identity is invalid",
		)
	}
	if err := recordcodec.ValidateTimestamp("local Agent token updated_at", updatedAt); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != current.Record.ID ||
		evidence.record.Generation != current.Record.Generation ||
		evidence.record.Image != current.Record.Image ||
		evidence.record.Phase != current.Record.Phase ||
		evidence.primaryRevision != current.Revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase != localagentrecord.LocalAgentPhaseReady &&
		evidence.record.Phase != localagentrecord.LocalAgentPhaseUpdating {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent is not replaceable",
		)
	}
	if evidence.digest == nil || tokenDigest == evidence.record.TokenDigest ||
		updatedAt.Before(evidence.record.TokenUpdatedAt) {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent replacement token is not a new generation",
		)
	}

	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Image = image
	replacement.Generation++
	replacement.Phase = localagentrecord.LocalAgentPhaseUpdating
	replacement.EncryptedToken = append([]byte(nil), encryptedToken...)
	replacement.TokenDigest = tokenDigest
	replacement.TokenUpdatedAt = updatedAt
	primaryValue, configValue, tokenValue, reference, err := localagentrecord.EncodeLocalAgentValues(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	defer clear(configValue)
	defer clear(tokenValue)
	defer clear(reference)
	newDigestKey := localagentrecord.LocalAgentDigestKey(tokenDigest)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
		{Key: localagentrecord.LocalAgentPrimaryKey(replacement.ID), ModRevision: evidence.primary.ModRevision},
		{Key: localagentrecord.LocalAgentConfigKey(replacement.ID), ModRevision: evidence.config.ModRevision},
		{Key: localagentrecord.LocalAgentTokenKey(replacement.ID), ModRevision: evidence.token.ModRevision},
		{Key: evidence.digest.Key, ModRevision: evidence.digest.ModRevision},
		{Key: newDigestKey},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(replacement.ID), Value: primaryValue},
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentConfigKey(replacement.ID), Value: configValue},
		{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentTokenKey(replacement.ID), Value: tokenValue},
		{Type: etcdstore.MutationDelete, Key: evidence.digest.Key},
		{Type: etcdstore.MutationPut, Key: newDigestKey, Value: reference},
	})
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	etcdstore.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent replacement state changed",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *Repository) MarkReplacementReady(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	return repository.transitionPhase(
		ctx,
		agentID,
		generation,
		revision,
		localagentrecord.LocalAgentPhaseUpdating,
		localagentrecord.LocalAgentPhaseReady,
		true,
		time.Time{},
	)
}

func (repository *Repository) transitionPhase(
	ctx context.Context,
	agentID string,
	generation uint64,
	revision int64,
	expected localagentrecord.LocalAgentPhase,
	next localagentrecord.LocalAgentPhase,
	idempotent bool,
	readyAt time.Time,
) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	if evidence.record.ID != agentID || evidence.record.Generation != generation ||
		evidence.primaryRevision != revision {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if idempotent && evidence.record.Phase == next {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
			Record: evidence.record, Revision: evidence.primaryRevision,
			ReadRevision: evidence.readRevision,
		}, nil
	}
	if evidence.record.Phase != expected {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent phase changed",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Phase = next
	if expected != localagentrecord.LocalAgentPhaseUpdating {
		replacement.ReadyAt = readyAt
	}
	primaryValue, err := localagentrecord.EncodeLocalAgentPrimary(replacement)
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	defer clear(primaryValue)
	result, err := repository.store.Transact(ctx, []etcdstore.Condition{{
		Key: localagentrecord.LocalAgentPrimaryKey(agentID), ModRevision: evidence.primary.ModRevision,
	}}, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: localagentrecord.LocalAgentPrimaryKey(agentID), Value: primaryValue}})
	if err != nil {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, err
	}
	etcdstore.ClearValues(result.FailureReads)
	if !result.Succeeded {
		return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{}, errs.New(
			errs.KindStateConflict,
			"local Agent phase changed",
		)
	}
	return etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}
