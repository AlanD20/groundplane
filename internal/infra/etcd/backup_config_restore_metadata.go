package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

type configRestoreMetadata struct {
	projection, identities             []byte
	projectionSHA256, identitiesSHA256 string
}

func (metadata *configRestoreMetadata) clear() {
	clear(metadata.projection)
	clear(metadata.identities)
}

func prepareConfigRestoreMetadata(generation etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord],
	root blueprints.EnvironmentBlueprintSeal, projection environmentprojection.EnvironmentComposeProjection,
	identities environmentprojection.EnvironmentOwnedIdentities,
) (configRestoreMetadata, error) {
	var result configRestoreMetadata
	owner := generation.Record.Owner
	if generation.Revision <= 0 || generation.Record.Completed.GetContent() == nil ||
		projection.EnvironmentID != owner.EnvironmentID || projection.RevisionID != owner.Transfer.Binding.TaskID ||
		projection.RenderGeneration != owner.RenderGeneration ||
		identities.EnvironmentID != projection.EnvironmentID || identities.RevisionID != projection.RevisionID ||
		identities.RenderGeneration != projection.RenderGeneration ||
		uint32(len(projection.Entries)) != generation.Record.Completed.Content.EntryCount {
		return result, configTransferAuthorityConflict()
	}
	digest, err := blueprints.EnvironmentBlueprintDependencyDigest(projection)
	if err != nil || digest != root.DependencyDigest {
		return result, configTransferAuthorityConflict()
	}
	result.projection, err = environmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		return result, err
	}
	result.identities, err = environmentprojection.EncodeEnvironmentOwnedIdentities(identities)
	if err != nil {
		result.clear()
		return configRestoreMetadata{}, err
	}
	projectionSHA, identitiesSHA := sha256.Sum256(result.projection), sha256.Sum256(result.identities)
	result.projectionSHA256, result.identitiesSHA256 = hex.EncodeToString(
		projectionSHA[:],
	), hex.EncodeToString(
		identitiesSHA[:],
	)
	return result, nil
}

// Stage each ordinary immutable projection record before any live mutation.
// The native receipt pins both encodings; a resumed writer cannot replace them.
func (repository *BackupRuntimeRepository) stageConfigRestoreMetadata(ctx context.Context,
	generation etcdstore.Versioned[backupconfiguration.ConfigRestoreGenerationRecord], metadata configRestoreMetadata,
) error {
	owner := generation.Record.Owner
	for _, item := range []struct {
		key   string
		value []byte
	}{
		{blueprints.EnvironmentBlueprintEffectiveProjectionKey(owner.EnvironmentID, owner.Transfer.Binding.TaskID), metadata.projection},
		{blueprints.EnvironmentBlueprintOwnedIdentitiesKey(owner.EnvironmentID, owner.Transfer.Binding.TaskID), metadata.identities},
	} {
		authority, err := repository.loadConfigRestorePublication(ctx, generation)
		if err != nil {
			return err
		}
		if authority.current.Record.ConfigProgress.ProjectionSHA256 != metadata.projectionSHA256 ||
			authority.current.Record.ConfigProgress.IdentitiesSHA256 != metadata.identitiesSHA256 {
			return configTransferAuthorityConflict()
		}
		read, err := repository.store.GetMany(
			ctx,
			etcdstore.GetManyRequest{Keys: []string{item.key}, Revision: authority.revision},
		)
		if err != nil {
			return err
		}
		if read == nil || read.ReadRevision != authority.revision || len(read.Values) != 1 {
			return configTransferAuthorityConflict()
		}
		value := read.Values[0]
		if value != nil {
			valid := value.Key == item.key && value.Version == 1 && value.ModRevision > 0 &&
				bytes.Equal(value.Value, item.value)
			etcdstore.ClearValues(read.Values)
			if !valid {
				return configTransferAuthorityConflict()
			}
			continue
		}
		etcdstore.ClearValues(read.Values)
		if authority.current.Record.MutationStarted {
			return configTransferAuthorityConflict()
		}
		conditions := authority.conditions
		mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: item.key, Value: item.value}}
		budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
		if err != nil {
			return err
		}
		if budget.Operations > etcdstore.MaximumOperations || budget.Bytes > etcdstore.MaximumBytes {
			return configTransferAuthorityConflict()
		}
		result, err := repository.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearValues(result.FailureReads)
		if err != nil {
			return err
		}
		if !result.Succeeded {
			return configTransferAuthorityConflict()
		}
	}
	return nil
}
