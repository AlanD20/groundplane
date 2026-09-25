package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) validateBlueprintReleaseChildPublication(
	ctx context.Context,
	child TaskRecord,
	unit blueprintunits.Unit,
	publication BlueprintReleasePublication,
	revision int64,
) error {
	manifestKey := releases.ReleaseManifestStagingKey(child.Params[releaserender.TaskReleasePublicationParam])
	manifestRead, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{manifestKey}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if manifestRead == nil || len(manifestRead.Values) != 1 || manifestRead.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint child Release manifest is missing")
	}
	defer keyvalue.ClearValues(manifestRead.Values)
	manifest, err := releases.DecodeReleaseRecord[releases.ReleaseStagedManifest](
		manifestRead.Values[0].Value, "release-staged-manifest",
	)
	if err != nil || manifest.PublicationID != child.Params[releaserender.TaskReleasePublicationParam] ||
		len(manifest.Members) != 1 || manifest.OperationID != child.OperationID ||
		manifest.Members[0].ServiceID != unit.Target.ID {
		return errs.New(errs.KindStateConflict, "Blueprint child Release does not match its unit")
	}
	if !slices.Contains(publication.conditions, keyvalue.Condition{
		Key: manifestKey, ModRevision: manifestRead.Values[0].ModRevision,
	}) {
		return errs.New(errs.KindStateConflict, "Blueprint child Release manifest is not fenced")
	}
	publicationKey := releases.ReleasePublicationKey(manifest.PublicationID)
	if !slices.Contains(publication.conditions, keyvalue.Condition{Key: publicationKey}) {
		return errs.New(errs.KindStateConflict, "Blueprint child Release publication is not fenced")
	}
	publicationMutations := 0
	for _, mutation := range publication.mutations {
		if mutation.Key == manifestKey {
			return errs.New(errs.KindStateConflict, "Blueprint child Release fragment changes its manifest")
		}
		if mutation.Key != publicationKey {
			continue
		}
		if mutation.Type != keyvalue.MutationPut {
			return errs.New(errs.KindStateConflict, "Blueprint child Release publication does not match its manifest")
		}
		marker, decodeErr := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](
			mutation.Value,
			"release-publication",
		)
		if decodeErr != nil || marker.PublicationID != manifest.PublicationID ||
			marker.OperationID != child.OperationID || marker.ManifestDigest != manifest.Digest {
			return errs.New(errs.KindStateConflict, "Blueprint child Release publication does not match its manifest")
		}
		publicationMutations++
	}
	if publicationMutations != 1 {
		return errs.New(errs.KindStateConflict, "Blueprint child Release publication does not match its manifest")
	}
	return nil
}
