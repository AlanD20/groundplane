package agentregistration

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareConfigurationUpdate captures the singleton, generation and configuration
// comparisons. Its caller joins this closed update to request replay atomically.
func (repository *Repository) PrepareConfigurationUpdate(
	ctx context.Context,
	current etcdstore.Versioned[localagentrecord.LocalAgentRecord],
	config localagentrecord.LocalAgentConfig,
	marker idempotencyrecord.IdempotencyMarker,
) (ConfigurationUpdate, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return ConfigurationUpdate{}, err
	}
	if err := localagentrecord.ValidateLocalAgentConfig(config); err != nil {
		return ConfigurationUpdate{}, err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision ||
		current.Record.ID == "" || marker.Kind != idempotencyrecord.IdempotencyMarkerDirect ||
		marker.State != idempotencyrecord.IdempotencyMarkerCompleted || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopePlatform ||
		marker.Locator.ScopeID != "-" || marker.Locator.Method != "PUT" ||
		marker.Locator.Route != "/agents/{id}/config" {
		return ConfigurationUpdate{}, errs.New(
			errs.KindValidationFailed,
			"local Agent config mutation identity is invalid",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return ConfigurationUpdate{}, err
	}
	evidence, err := repository.readSingleton(ctx)
	if err != nil {
		return ConfigurationUpdate{}, err
	}
	if evidence.record.ID != current.Record.ID || evidence.record.Generation != current.Record.Generation ||
		evidence.primaryRevision != current.Revision {
		return ConfigurationUpdate{}, errs.New(
			errs.KindStateConflict,
			"local Agent generation or revision changed",
		)
	}
	if evidence.record.Phase == localagentrecord.LocalAgentPhaseDeleting {
		return ConfigurationUpdate{}, errs.New(
			errs.KindStateConflict,
			"deleting local Agent config cannot be changed",
		)
	}
	replacement := localagentrecord.CloneLocalAgentRecord(evidence.record)
	replacement.Config = localagentrecord.CloneLocalAgentConfig(config)
	configValue, err := localagentrecord.EncodeLocalAgentConfig(replacement)
	if err != nil {
		return ConfigurationUpdate{}, err
	}
	defer clear(configValue)
	return ConfigurationUpdate{
		record: etcdstore.Versioned[localagentrecord.LocalAgentRecord]{
			Record:       replacement,
			Revision:     evidence.primaryRevision,
			ReadRevision: current.ReadRevision,
		},
		conditions: []etcdstore.Condition{
			{Key: localagentrecord.LocalAgentSingletonKey, ModRevision: evidence.singleton.ModRevision},
			{Key: localagentrecord.LocalAgentPrimaryKey(current.Record.ID), ModRevision: evidence.primary.ModRevision},
			{Key: localagentrecord.LocalAgentConfigKey(current.Record.ID), ModRevision: evidence.config.ModRevision},
		},
		value: append([]byte(nil), configValue...),
	}, nil
}

type ConfigurationUpdate struct {
	record     etcdstore.Versioned[localagentrecord.LocalAgentRecord]
	conditions []etcdstore.Condition
	value      []byte
}

func (update ConfigurationUpdate) Changes() ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if update.record.Revision <= 0 || len(update.conditions) != 3 || len(update.value) == 0 {
		return nil, nil, errs.New(errs.KindInternal, "Agent configuration update has not been prepared")
	}
	return append([]etcdstore.Condition(nil), update.conditions...), []etcdstore.Mutation{{Type: etcdstore.MutationPut,
		Key: localagentrecord.LocalAgentConfigKey(
			update.record.Record.ID,
		), Value: append([]byte(nil), update.value...)}}, nil
}

func (update ConfigurationUpdate) ClassifyConflict(revision int64, values []*etcdstore.KeyValue) error {
	return classifyLocalAgentConfigConflict(update.record.Record.ID)(revision, values)
}

func (update ConfigurationUpdate) Stored(revision int64) etcdstore.Versioned[localagentrecord.LocalAgentRecord] {
	result := update.record
	result.Record = localagentrecord.CloneLocalAgentRecord(result.Record)
	result.ReadRevision = revision
	return result
}

func (update *ConfigurationUpdate) Clear() {
	clear(update.value)
	*update = ConfigurationUpdate{}
}

func classifyLocalAgentConfigConflict(agentID string) func(int64, []*etcdstore.KeyValue) error {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 3 {
			return errs.New(errs.KindInternal, "local Agent config compare evidence is incomplete")
		}
		if values[0] == nil || values[1] == nil {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		if values[2] == nil {
			return errs.New(errs.KindInternal, "local Agent config record is missing")
		}
		resolvedID, err := localagentrecord.DecodeLocalAgentReference(values[0].Value)
		if err != nil {
			return err
		}
		if resolvedID != agentID {
			return errs.New(errs.KindAgentNotFound, "local Agent was not found")
		}
		return errs.New(errs.KindStateConflict, "local Agent config changed")
	}
}
