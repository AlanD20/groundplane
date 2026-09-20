package volume

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"path"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultVolumePageLimit = 50
	maximumVolumePageLimit = 200
)

type ReadService struct {
	repository ReadRepository
}

func NewReadService(repository ReadRepository) (*ReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "volume read repository is required")
	}
	return &ReadService{repository: repository}, nil
}

type volumeListCursor struct {
	EnvironmentID string `json:"environment_id"`
	RevisionID    string `json:"revision_id"`
	AfterKey      string `json:"after_key"`
}

func (service *ReadService) ListVolumes(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[etcd.VolumeRecord], error) {
	projection, cursor, err := service.volumeListProjection(ctx, environmentID, request.Cursor)
	if err != nil {
		return etcdstore.Page[etcd.VolumeRecord]{}, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = defaultVolumePageLimit
	}
	if limit < 1 || limit > maximumVolumePageLimit {
		return etcdstore.Page[etcd.VolumeRecord]{}, errs.New(
			errs.KindValidationFailed, "Volume page limit must be between 1 and 200",
		)
	}
	start := sort.Search(len(projection.Record.Volumes), func(index int) bool {
		return projection.Record.Volumes[index].Key > cursor.AfterKey
	})
	end := start + limit
	if end > len(projection.Record.Volumes) {
		end = len(projection.Record.Volumes)
	}
	items := make([]etcdstore.Versioned[etcd.VolumeRecord], 0, end-start)
	for _, volume := range projection.Record.Volumes[start:end] {
		items = append(items, etcdstore.Versioned[etcd.VolumeRecord]{
			Record: etcd.VolumeRecord{
				ID: volume.ID, EnvironmentID: environmentID, Slug: volume.Slug, Key: volume.Key,
			},
			Revision: projection.Revision, ReadRevision: projection.ReadRevision,
		})
	}
	page := etcdstore.Page[etcd.VolumeRecord]{Items: items}
	if end < len(projection.Record.Volumes) {
		page.NextCursor, err = encodeVolumeCursor(volumeListCursor{
			EnvironmentID: environmentID, RevisionID: projection.Record.RevisionID,
			AfterKey: projection.Record.Volumes[end-1].Key,
		})
		if err != nil {
			return etcdstore.Page[etcd.VolumeRecord]{}, err
		}
	}
	return page, nil
}

func (service *ReadService) ListVolumeViews(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (apiTypes.Page[apiTypes.Volume], error) {
	page, err := service.ListVolumes(ctx, environmentID, request)
	if err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, environmentID)
	if err != nil {
		return apiTypes.Page[apiTypes.Volume]{}, err
	}
	items := make([]apiTypes.Volume, len(page.Items))
	for index, item := range page.Items {
		items[index] = volumeView(environment.Record, item.Record, "", "active")
	}
	return apiTypes.Page[apiTypes.Volume]{Items: items, NextCursor: page.NextCursor}, nil
}

func (service *ReadService) GetVolume(ctx context.Context, volumeID string) (apiTypes.Volume, error) {
	projection, identity, err := service.repository.FindEnvironmentVolume(ctx, volumeID)
	if err != nil {
		return apiTypes.Volume{}, err
	}
	environment, err := service.repository.GetEnvironment(ctx, projection.Record.EnvironmentID)
	if err != nil {
		return apiTypes.Volume{}, err
	}
	return volumeView(environment.Record, etcd.VolumeRecord{
		ID: identity.ID, EnvironmentID: projection.Record.EnvironmentID, Slug: identity.Slug, Key: identity.Key,
	}, "", "active"), nil
}

func volumeView(
	environment hierarchyrecord.EnvironmentRecord,
	record etcd.VolumeRecord,
	originTaskID string,
	state string,
) apiTypes.Volume {
	result := apiTypes.Volume{
		ID: record.ID, EnvironmentID: record.EnvironmentID, Slug: record.Slug, Key: record.Key,
		Path: path.Join(environment.VolumeDir, record.Key), State: state,
	}
	if originTaskID != "" {
		origin := originTaskID
		result.OriginTaskID = &origin
	}
	return result
}

func (service *ReadService) volumeListProjection(
	ctx context.Context,
	environmentID string,
	encodedCursor string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], volumeListCursor, error) {
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, errs.New(
			errs.KindValidationFailed, "Volume list requires a stable Environment id",
		)
	}
	if encodedCursor == "" {
		projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
		if err != nil {
			return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, err
		}
		if !found {
			return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, nil
		}
		return projection, volumeListCursor{EnvironmentID: environmentID, RevisionID: projection.Record.RevisionID}, nil
	}
	cursor, err := decodeVolumeCursor[volumeListCursor](encodedCursor)
	if err != nil || cursor.EnvironmentID != environmentID ||
		ids.Validate(ids.KindTask, cursor.RevisionID) != nil {
		return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, errs.New(
			errs.KindValidationFailed, "Volume page cursor is invalid",
		)
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjectionRevision(
		ctx, environmentID, cursor.RevisionID,
	)
	if err != nil {
		return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, err
	}
	if !found {
		return etcdstore.Versioned[etcd.EnvironmentComposeProjection]{}, volumeListCursor{}, errs.New(
			errs.KindStateConflict, "Volume page revision is no longer available",
		)
	}
	return projection, cursor, nil
}

type volumeImpactCursor struct {
	EnvironmentID string `json:"environment_id"`
	RevisionID    string `json:"revision_id"`
	ReadRevision  int64  `json:"read_revision"`
	VolumeID      string `json:"volume_id"`
	Offset        int    `json:"offset"`
}

func (service *ReadService) GetVolumeDeletionImpact(
	ctx context.Context,
	volumeID string,
	encodedCursor string,
	limit int,
) (apiTypes.VolumeDeletionImpactPage, error) {
	if limit < 1 || limit > 40 || len(encodedCursor) > 4096 {
		return apiTypes.VolumeDeletionImpactPage{}, errs.New(
			errs.KindValidationFailed, "Volume deletion-impact page request is invalid",
		)
	}
	var projection etcdstore.Versioned[etcd.EnvironmentComposeProjection]
	var identity etcd.EnvironmentVolumeIdentity
	cursor := volumeImpactCursor{VolumeID: volumeID}
	if encodedCursor == "" {
		var err error
		projection, identity, err = service.repository.FindEnvironmentVolume(ctx, volumeID)
		if err != nil {
			return apiTypes.VolumeDeletionImpactPage{}, err
		}
		cursor.EnvironmentID = projection.Record.EnvironmentID
		cursor.RevisionID = projection.Record.RevisionID
		cursor.ReadRevision = projection.ReadRevision
	} else {
		decoded, err := decodeVolumeCursor[volumeImpactCursor](encodedCursor)
		if err != nil || decoded.VolumeID != volumeID || decoded.Offset < 1 ||
			ids.Validate(ids.KindEnvironment, decoded.EnvironmentID) != nil ||
			ids.Validate(ids.KindTask, decoded.RevisionID) != nil {
			return apiTypes.VolumeDeletionImpactPage{}, errs.New(
				errs.KindValidationFailed, "Volume deletion-impact cursor is invalid",
			)
		}
		cursor = decoded
		if decoded.ReadRevision <= 0 {
			return apiTypes.VolumeDeletionImpactPage{}, errs.New(
				errs.KindValidationFailed, "Volume deletion-impact cursor is invalid",
			)
		}
		projection, identity, err = service.repository.ResolveEnvironmentVolumeAtRevision(
			ctx, cursor.EnvironmentID, volumeID, cursor.RevisionID, cursor.ReadRevision,
		)
		if err != nil {
			return apiTypes.VolumeDeletionImpactPage{}, err
		}
	}
	backupImpact, err := service.repository.ResolveVolumeRemovalImpactAtRevision(
		ctx, projection.Record.EnvironmentID, volumeID, projection.ReadRevision, time.Now().UTC(),
	)
	if err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, err
	}
	items := volumeRemovalImpactItems(projection.Record, volumeID, backupImpact)
	if cursor.Offset > len(items) {
		return apiTypes.VolumeDeletionImpactPage{}, errs.New(
			errs.KindValidationFailed, "Volume deletion-impact cursor is beyond the result",
		)
	}
	end := cursor.Offset + limit
	if end > len(items) {
		end = len(items)
	}
	pageItems := make([]apiTypes.VolumeDeletionImpactItem, end-cursor.Offset)
	copy(pageItems, items[cursor.Offset:end])
	rolling, err := volumeImpactDigest(projection.Record, identity, backupImpact, items[:end])
	if err != nil {
		return apiTypes.VolumeDeletionImpactPage{}, err
	}
	page := apiTypes.VolumeDeletionImpactPage{
		VolumeID: volumeID, Slug: identity.Slug, Key: identity.Key,
		EnvironmentID: projection.Record.EnvironmentID, Revision: projection.Revision,
		EnvironmentHead: projection.Record.RevisionID, Items: pageItems,
		Complete: end == len(items), ItemCount: int64(end), RollingDigest: rolling,
		DataHandling: "recursive_destroy",
	}
	if page.Complete {
		page.ImpactToken, err = volumeImpactDigest(projection.Record, identity, backupImpact, items)
		if err != nil {
			return apiTypes.VolumeDeletionImpactPage{}, err
		}
	} else {
		page.NextCursor, err = encodeVolumeCursor(volumeImpactCursor{
			EnvironmentID: cursor.EnvironmentID, RevisionID: cursor.RevisionID,
			ReadRevision: cursor.ReadRevision, VolumeID: volumeID, Offset: end,
		})
		if err != nil {
			return apiTypes.VolumeDeletionImpactPage{}, err
		}
	}
	return page, nil
}

func volumeMountImpactItems(
	projection etcd.EnvironmentComposeProjection,
	volumeID string,
) []apiTypes.VolumeDeletionImpactItem {
	names := volumeConsumerServiceNames(projection, volumeID)
	items := make([]apiTypes.VolumeDeletionImpactItem, 0)
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID != volumeID {
			continue
		}
		items = append(items, apiTypes.VolumeDeletionImpactItem{
			ID:   "mount:" + mount.ServiceID + ":" + mount.Target,
			Kind: apiTypes.VolumeImpactMount, ServiceID: mount.ServiceID,
			ServiceName: names[mount.ServiceID], MountTarget: mount.Target, ReadOnly: mount.ReadOnly,
		})
	}
	sort.Slice(items, func(left, right int) bool { return items[left].ID < items[right].ID })
	return items
}

func volumeConsumerServiceNames(projection etcd.EnvironmentComposeProjection, volumeID string) map[string]string {
	addressed := make(map[string]struct{})
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			addressed[mount.ServiceID] = struct{}{}
		}
	}
	names := make(map[string]string, len(addressed))
	for _, service := range projection.DesiredServices {
		if _, exists := addressed[service.Desired.ID]; exists {
			names[service.Desired.ID] = service.Desired.Name
			delete(addressed, service.Desired.ID)
		}
	}
	if len(addressed) == 0 {
		return names
	}
	generatedOwners := make(map[string]string, len(addressed))
	for _, component := range projection.Components {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if _, exists := addressed[serviceID]; exists {
				generatedOwners[serviceID] = component.Desired.ID
			}
		}
	}
	artifact := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(projection.ComposeArtifact, artifact) != nil {
		return names
	}
	for _, service := range artifact.GetServices() {
		owner, generated := generatedOwners[service.GetServiceId()]
		if generated && owner == service.GetOwnerComponentId() {
			names[service.GetServiceId()] = service.GetComposeName()
		}
	}
	return names
}

func volumeRemovalImpactItems(
	projection etcd.EnvironmentComposeProjection,
	volumeID string,
	backup etcd.BackupVolumeRemovalImpact,
) []apiTypes.VolumeDeletionImpactItem {
	items := volumeMountImpactItems(projection, volumeID)
	selected := false
	for _, source := range backup.Sources {
		if source.Selected {
			selected = true
			items = append(items, apiTypes.VolumeDeletionImpactItem{
				ID: "backup-source:" + source.SourceID, Kind: apiTypes.VolumeImpactBackupSource,
				SourceID: source.SourceID, RetainsConfiguration: true,
			})
		}
		if source.RecoveryPointCount > 0 {
			items = append(items, apiTypes.VolumeDeletionImpactItem{
				ID: "recovery-points:" + source.SourceID, Kind: apiTypes.VolumeImpactRecoveryPoint,
				SourceID: source.SourceID, RecoveryPointCount: source.RecoveryPointCount,
				HistoricalDigest: source.HistoricalDigest, RetainsConfiguration: true,
			})
		}
	}
	if selected {
		items = append(items, apiTypes.VolumeDeletionImpactItem{
			ID: "backup-policy:" + projection.EnvironmentID, Kind: apiTypes.VolumeImpactBackupPolicy,
			PolicyDisables: backup.PolicyDisables, RetainsConfiguration: true,
		})
	}
	sort.Slice(items, func(left, right int) bool { return items[left].ID < items[right].ID })
	return items
}

func volumeImpactDigest(
	projection etcd.EnvironmentComposeProjection,
	identity etcd.EnvironmentVolumeIdentity,
	backup etcd.BackupVolumeRemovalImpact,
	items []apiTypes.VolumeDeletionImpactItem,
) (string, error) {
	value, err := json.Marshal(struct {
		EnvironmentID string                              `json:"environment_id"`
		RevisionID    string                              `json:"revision_id"`
		Generation    uint64                              `json:"generation"`
		Volume        etcd.EnvironmentVolumeIdentity      `json:"volume"`
		Backup        etcd.BackupVolumeRemovalImpact      `json:"backup"`
		Items         []apiTypes.VolumeDeletionImpactItem `json:"items"`
	}{projection.EnvironmentID, projection.RevisionID, projection.RenderGeneration, identity, backup, items})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	clear(value)
	return hex.EncodeToString(digest[:]), nil
}

func encodeVolumeCursor(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	defer clear(encoded)
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeVolumeCursor[T any](encoded string) (T, error) {
	var result T
	if encoded == "" || len(encoded) > 4096 {
		return result, errs.New(errs.KindValidationFailed, "Volume cursor is invalid")
	}
	value, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return result, errs.New(errs.KindValidationFailed, "Volume cursor is invalid")
	}
	defer clear(value)
	decoderErr := json.Unmarshal(value, &result)
	if decoderErr != nil {
		return result, errs.New(errs.KindValidationFailed, "Volume cursor is invalid")
	}
	return result, nil
}
