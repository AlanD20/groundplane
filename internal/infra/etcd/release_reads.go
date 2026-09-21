package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseView struct {
	Intent     domain.Intent
	Checkpoint domain.Checkpoint
	Terminal   *domain.TerminalSummary
	Retention  *domain.RollbackMaterial
	Projection domain.ServiceProjection
	Attempts   []domain.Attempt
	Revision   int64
}

type ReleasePageRequest struct {
	EnvironmentID string
	ServiceID     string
	Limit         int
	Cursor        string
}

type ReleasePage struct {
	Items      []ReleaseView
	NextCursor string
	Revision   int64
}

type CurrentSuccessfulRelease struct {
	Projection         domain.ServiceProjection
	ProjectionRevision int64
	Intent             domain.Intent
	IntentRevision     int64
	Revision           int64
}

type ServingRelease struct {
	Projection         domain.ServiceProjection
	ProjectionRevision int64
	Intent             domain.Intent
	IntentRevision     int64
	Revision           int64
}

// ResolveCurrentSuccessful resolves the Service projection and its immutable
// successful Release intent at one fixed etcd revision. It never falls back to
// the mutable desired image when a Service has no successful Release.
func (ledger *ReleaseLedger) ResolveCurrentSuccessful(
	ctx context.Context,
	environmentID, serviceID string,
	revision int64,
) (CurrentSuccessfulRelease, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || revision < 0 {
		return CurrentSuccessfulRelease{}, errs.New(
			errs.KindValidationFailed,
			"current successful Release request is invalid",
		)
	}
	projectionRead, err := ledger.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{releases.ReleaseProjectionKey(serviceID)}, Revision: revision},
	)
	if err != nil {
		return CurrentSuccessfulRelease{}, err
	}
	if projectionRead == nil || len(projectionRead.Values) != 1 || projectionRead.Values[0] == nil {
		return CurrentSuccessfulRelease{}, errs.New(errs.KindReleaseNotFound, "Service has no successful Release")
	}
	projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](
		projectionRead.Values[0].Value,
		"service-release-projection",
	)
	if err != nil || projection.EnvironmentID != environmentID || projection.ServiceID != serviceID ||
		ids.Validate(ids.KindDeployment, projection.CurrentSuccessfulReleaseID) != nil {
		return CurrentSuccessfulRelease{}, releases.CorruptReleaseRecord()
	}
	intentRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			releases.ReleaseIntentStagingKey("", projection.CurrentSuccessfulReleaseID),
			releases.ReleaseTerminalKey(projection.CurrentSuccessfulReleaseID),
		},
		Revision: projectionRead.ReadRevision,
	})
	if err != nil {
		return CurrentSuccessfulRelease{}, err
	}
	if intentRead == nil || intentRead.ReadRevision != projectionRead.ReadRevision || len(intentRead.Values) != 2 ||
		intentRead.Values[0] == nil ||
		intentRead.Values[1] == nil {
		return CurrentSuccessfulRelease{}, releases.CorruptReleaseRecord()
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](intentRead.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != projection.CurrentSuccessfulReleaseID ||
		intent.EnvironmentID != environmentID || intent.ServiceID != serviceID {
		return CurrentSuccessfulRelease{}, releases.CorruptReleaseRecord()
	}
	terminal, err := releases.DecodeReleaseRecord[domain.TerminalSummary](intentRead.Values[1].Value, "release-terminal-summary")
	if err != nil || terminal.ReleaseID != intent.ID || terminal.FinalServingReleaseID != intent.ID {
		return CurrentSuccessfulRelease{}, releases.CorruptReleaseRecord()
	}
	return CurrentSuccessfulRelease{
		Projection: projection, ProjectionRevision: projectionRead.Values[0].ModRevision,
		Intent: intent, IntentRevision: intentRead.Values[0].ModRevision,
		Revision: projectionRead.ReadRevision,
	}, nil
}

func (ledger *ReleaseLedger) ResolveServing(
	ctx context.Context,
	environmentID, serviceID string,
	revision int64,
) (ServingRelease, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil || revision < 0 {
		return ServingRelease{}, errs.New(errs.KindValidationFailed, "serving Release request is invalid")
	}
	projectionRead, err := ledger.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{releases.ReleaseProjectionKey(serviceID)}, Revision: revision},
	)
	if err != nil {
		return ServingRelease{}, err
	}
	if projectionRead == nil || len(projectionRead.Values) != 1 || projectionRead.Values[0] == nil {
		return ServingRelease{}, errs.New(errs.KindReleaseNotFound, "Service has no serving Release")
	}
	projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](
		projectionRead.Values[0].Value,
		"service-release-projection",
	)
	if err != nil || projection.EnvironmentID != environmentID || projection.ServiceID != serviceID {
		return ServingRelease{}, releases.CorruptReleaseRecord()
	}
	if projection.ServingReleaseID == "" {
		return ServingRelease{}, errs.New(errs.KindReleaseNotFound, "Service has no serving Release")
	}
	if ids.Validate(ids.KindDeployment, projection.ServingReleaseID) != nil {
		return ServingRelease{}, releases.CorruptReleaseRecord()
	}
	intentRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{releases.ReleaseIntentStagingKey("", projection.ServingReleaseID)}, Revision: projectionRead.ReadRevision,
	})
	if err != nil {
		return ServingRelease{}, err
	}
	if intentRead == nil || intentRead.ReadRevision != projectionRead.ReadRevision || len(intentRead.Values) != 1 ||
		intentRead.Values[0] == nil {
		return ServingRelease{}, releases.CorruptReleaseRecord()
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](intentRead.Values[0].Value, "release-intent")
	if err != nil || domain.ValidateIntent(intent) != nil || intent.ID != projection.ServingReleaseID ||
		intent.EnvironmentID != environmentID || intent.ServiceID != serviceID {
		return ServingRelease{}, releases.CorruptReleaseRecord()
	}
	return ServingRelease{
		Projection: projection, ProjectionRevision: projectionRead.Values[0].ModRevision,
		Intent: intent, IntentRevision: intentRead.Values[0].ModRevision, Revision: projectionRead.ReadRevision,
	}, nil
}

type releaseCursor struct {
	Schema        int    `json:"schema"`
	Revision      int64  `json:"revision"`
	EnvironmentID string `json:"environment_id"`
	ServiceID     string `json:"service_id,omitempty"`
	LastReleaseID string `json:"last_release_id"`
	Direction     string `json:"direction"`
	Limit         int    `json:"limit"`
	FilterDigest  string `json:"filter_digest"`
}

func (ledger *ReleaseLedger) Get(ctx context.Context, releaseID string) (ReleaseView, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindDeployment, releaseID) != nil {
		return ReleaseView{}, errs.New(errs.KindValidationFailed, "release id is invalid")
	}
	intentRead, err := ledger.store.Get(ctx, releases.ReleaseIntentStagingKey("", releaseID))
	if err != nil {
		return ReleaseView{}, err
	}
	if intentRead == nil || intentRead.Entry == nil {
		return ReleaseView{}, errs.New(errs.KindReleaseNotFound, "release was not found")
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](intentRead.Entry.Value, "release-intent")
	if err != nil || intent.ID != releaseID || domain.ValidateIntent(intent) != nil {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	headRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{releases.ReleaseOperationKey(intent.OperationID)}, Revision: intentRead.ReadRevision,
	})
	if err != nil {
		return ReleaseView{}, err
	}
	if headRead == nil || len(headRead.Values) != 1 || headRead.Values[0] == nil {
		if intent.OperationKind != domain.OperationBlueprintApply {
			return ReleaseView{}, errs.New(errs.KindReleaseNotFound, "release was not published")
		}
		indexRead, indexErr := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     []string{releases.ReleaseServiceIndexKey(intent.EnvironmentID, intent.ServiceID, releaseID)},
			Revision: intentRead.ReadRevision,
		})
		if indexErr != nil {
			return ReleaseView{}, indexErr
		}
		if indexRead == nil || indexRead.ReadRevision != intentRead.ReadRevision ||
			len(indexRead.Values) != 1 || indexRead.Values[0] == nil {
			return ReleaseView{}, errs.New(errs.KindReleaseNotFound, "release was not published")
		}
		publicationID, decodeErr := decodeReleaseIndex(indexRead.Values[0].Value, intent.ServiceID)
		if decodeErr != nil {
			return ReleaseView{}, decodeErr
		}
		return ledger.readViewAt(ctx, publicationID, releaseID, intentRead.ReadRevision)
	}
	head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](headRead.Values[0].Value, "release-operation")
	if err != nil || head.OperationID != intent.OperationID {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	return ledger.readViewAt(ctx, head.PublicationID, releaseID, headRead.ReadRevision)
}

func (ledger *ReleaseLedger) List(ctx context.Context, request ReleasePageRequest) (ReleasePage, error) {
	if ctx == nil || ledger == nil || ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
		request.Limit < 1 || request.Limit > 200 || request.ServiceID != "" && ids.Validate(ids.KindService, request.ServiceID) != nil {
		return ReleasePage{}, errs.New(errs.KindValidationFailed, "release page request is invalid")
	}
	prefix := releases.ReleaseEnvironmentIndexScope(request.EnvironmentID)
	if request.ServiceID != "" {
		prefix = releases.ReleaseServiceIndexScope(request.EnvironmentID, request.ServiceID)
	}
	revision := int64(0)
	start := ""
	if request.Cursor != "" {
		cursor, err := decodeReleaseCursor(request.Cursor)
		if err != nil || cursor.EnvironmentID != request.EnvironmentID || cursor.ServiceID != request.ServiceID ||
			cursor.Limit != request.Limit || cursor.Direction != "ascending" || cursor.FilterDigest != releaseFilterDigest(request.EnvironmentID, request.ServiceID) {
			return ReleasePage{}, errs.New(errs.KindMalformedRequest, "release cursor is invalid")
		}
		revision = cursor.Revision
		start = prefix + cursor.LastReleaseID
	}
	page, err := ledger.store.Range(
		ctx,
		etcdstore.RangeRequest{Prefix: prefix, StartExclusive: start, Limit: int64(request.Limit + 1), Revision: revision},
	)
	if err != nil {
		return ReleasePage{}, err
	}
	if page == nil || page.ReadRevision <= 0 || page.ResponseRevision < page.ReadRevision {
		return ReleasePage{}, releases.CorruptReleaseRecord()
	}
	values := page.Values
	hasMore := len(values) > request.Limit
	if hasMore {
		values = values[:request.Limit]
	}
	items := make([]ReleaseView, len(values))
	lastID := ""
	for index, value := range values {
		releaseID := strings.TrimPrefix(value.Key, prefix)
		if strings.Contains(releaseID, "/") || ids.Validate(ids.KindDeployment, releaseID) != nil {
			return ReleasePage{}, releases.CorruptReleaseRecord()
		}
		publicationID, err := decodeReleaseIndex(value.Value, request.ServiceID)
		if err != nil {
			return ReleasePage{}, err
		}
		view, err := ledger.readViewAt(ctx, publicationID, releaseID, page.ReadRevision)
		if err != nil {
			return ReleasePage{}, err
		}
		if view.Intent.EnvironmentID != request.EnvironmentID ||
			request.ServiceID != "" && view.Intent.ServiceID != request.ServiceID {
			return ReleasePage{}, releases.CorruptReleaseRecord()
		}
		items[index] = view
		lastID = releaseID
	}
	nextCursor := ""
	if hasMore && lastID != "" {
		nextCursor, err = encodeReleaseCursor(releaseCursor{
			Schema: 1, Revision: page.ReadRevision, EnvironmentID: request.EnvironmentID, ServiceID: request.ServiceID,
			LastReleaseID: lastID, Direction: "ascending", Limit: request.Limit,
			FilterDigest: releaseFilterDigest(request.EnvironmentID, request.ServiceID),
		})
		if err != nil {
			return ReleasePage{}, err
		}
	}
	return ReleasePage{Items: items, NextCursor: nextCursor, Revision: page.ReadRevision}, nil
}

func (ledger *ReleaseLedger) readViewAt(
	ctx context.Context,
	publicationID string,
	releaseID string,
	revision int64,
) (ReleaseView, error) {
	keys := []string{
		releases.ReleaseIntentStagingKey(publicationID, releaseID), releases.ReleaseCheckpointStagingKey(publicationID, releaseID),
		releases.ReleasePublicationKey(publicationID), releases.ReleaseTerminalKey(releaseID), releases.ReleaseRetentionKey(releaseID),
	}
	read, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return ReleaseView{}, err
	}
	if read == nil || len(read.Values) != len(keys) || read.Values[0] == nil || read.Values[1] == nil ||
		read.Values[2] == nil {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	intent, err := releases.DecodeReleaseRecord[domain.Intent](read.Values[0].Value, "release-intent")
	if err != nil || intent.ID != releaseID || domain.ValidateIntent(intent) != nil {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	checkpoint, err := releases.DecodeReleaseRecord[domain.Checkpoint](read.Values[1].Value, "release-checkpoint")
	if err != nil || checkpoint.ReleaseID != releaseID || domain.ValidateCheckpoint(checkpoint) != nil {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	marker, err := releases.DecodeReleaseRecord[releases.ReleasePublicationMarker](read.Values[2].Value, "release-publication")
	if err != nil || marker.PublicationID != publicationID || marker.OperationID != intent.OperationID {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	view := ReleaseView{Intent: intent, Checkpoint: checkpoint, Revision: read.ReadRevision}
	if read.Values[3] != nil {
		terminal, err := releases.DecodeReleaseRecord[domain.TerminalSummary](read.Values[3].Value, "release-terminal-summary")
		if err != nil || terminal.ReleaseID != releaseID {
			return ReleaseView{}, releases.CorruptReleaseRecord()
		}
		view.Terminal = &terminal
	}
	if read.Values[4] != nil {
		retention, err := releases.DecodeReleaseRecord[domain.RollbackMaterial](read.Values[4].Value, "release-retention")
		if err != nil || retention.ReleaseID != releaseID {
			return ReleaseView{}, releases.CorruptReleaseRecord()
		}
		view.Retention = &retention
	}
	projectionRead, err := ledger.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			releases.ReleaseProjectionKey(intent.ServiceID),
			releases.ReleaseOperationKey(intent.OperationID),
		}, Revision: read.ReadRevision,
	})
	if err != nil {
		return ReleaseView{}, err
	}
	if projectionRead == nil || len(projectionRead.Values) != 2 {
		return ReleaseView{}, releases.CorruptReleaseRecord()
	}
	if projectionRead.Values[0] != nil {
		view.Projection, err = releases.DecodeReleaseRecord[domain.ServiceProjection](
			projectionRead.Values[0].Value,
			"service-release-projection",
		)
		if err != nil || view.Projection.ServiceID != intent.ServiceID {
			return ReleaseView{}, releases.CorruptReleaseRecord()
		}
	}
	if projectionRead.Values[1] == nil {
		if intent.OperationKind != domain.OperationBlueprintApply {
			return ReleaseView{}, releases.CorruptReleaseRecord()
		}
		return view, nil
	}
	head, err := releases.DecodeReleaseRecord[releases.ReleaseOperationHead](projectionRead.Values[1].Value, "release-operation")
	if err != nil {
		return ReleaseView{}, err
	}
	view.Attempts = slices.Clone(head.Attempts)
	return view, nil
}

func decodeReleaseIndex(value []byte, serviceFilter string) (string, error) {
	if recordcodec.RejectDuplicateFields(value) != nil {
		return "", releases.CorruptReleaseRecord()
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	publicationID := ""
	if serviceFilter == "" {
		var decoded releases.ReleaseEnvironmentIndexValue
		if decoder.Decode(&decoded) != nil || recordcodec.RequireEOF(decoder) != nil || decoded.Schema != 1 ||
			ids.Validate(
				ids.KindService,
				decoded.ServiceID,
			) != nil || releases.ValidatePublicationID(decoded.PublicationID) != nil {
			return "", releases.CorruptReleaseRecord()
		}
		publicationID = decoded.PublicationID
	} else {
		var decoded releases.ReleaseServiceIndexValue
		if decoder.Decode(&decoded) != nil || recordcodec.RequireEOF(decoder) != nil || decoded.Schema != 1 || releases.ValidatePublicationID(decoded.PublicationID) != nil {
			return "", releases.CorruptReleaseRecord()
		}
		publicationID = decoded.PublicationID
	}
	return publicationID, nil
}

func encodeReleaseCursor(value releaseCursor) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeReleaseCursor(value string) (releaseCursor, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || recordcodec.RejectDuplicateFields(decoded) != nil {
		return releaseCursor{}, errs.New(errs.KindMalformedRequest, "release cursor is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	decoder.DisallowUnknownFields()
	var cursor releaseCursor
	if decoder.Decode(&cursor) != nil || recordcodec.RequireEOF(decoder) != nil || cursor.Schema != 1 || cursor.Revision <= 0 ||
		ids.Validate(
			ids.KindEnvironment,
			cursor.EnvironmentID,
		) != nil || cursor.ServiceID != "" && ids.Validate(ids.KindService, cursor.ServiceID) != nil ||
		ids.Validate(
			ids.KindDeployment,
			cursor.LastReleaseID,
		) != nil || cursor.Direction != "ascending" || cursor.Limit < 1 || cursor.Limit > 200 {
		return releaseCursor{}, errs.New(errs.KindMalformedRequest, "release cursor is invalid")
	}
	return cursor, nil
}

func releaseFilterDigest(environmentID, serviceID string) string {
	digest := sha256.Sum256([]byte(environmentID + "\x00" + serviceID + "\x00ascending"))
	return hex.EncodeToString(digest[:])
}
