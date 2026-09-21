package operations

import (
	"context"
	"encoding/base64"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	defaultEntryProjectionPageSize = 50
	entryProjectionCursorPrefix    = "gpe1."
)

func (service *entryReadService) listProjectedEntries(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[entryrecord.Record], bool, error) {
	var projection etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	var found bool
	var err error
	offset := 0
	if request.Cursor == "" {
		projection, found, err = service.repository.GetEnvironmentComposeProjection(ctx, environmentID)
	} else {
		if !strings.HasPrefix(request.Cursor, entryProjectionCursorPrefix) {
			return etcdstore.Page[entryrecord.Record]{}, false, nil
		}
		revisionID, decodedOffset, decodeErr := decodeEntryProjectionCursor(request.Cursor)
		if decodeErr != nil {
			return etcdstore.Page[entryrecord.Record]{}, false, decodeErr
		}
		offset = decodedOffset
		projection, found, err = service.repository.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
	}
	if err != nil || !found {
		return etcdstore.Page[entryrecord.Record]{}, found, err
	}
	if offset < 0 || offset > len(projection.Record.Entries) {
		return etcdstore.Page[entryrecord.Record]{}, false, errs.New(
			errs.KindValidationFailed,
			"Entry page cursor is invalid",
		)
	}
	limit := request.Limit
	if limit == 0 {
		limit = defaultEntryProjectionPageSize
	}
	end := offset + limit
	if end > len(projection.Record.Entries) {
		end = len(projection.Record.Entries)
	}
	page := etcdstore.Page[entryrecord.Record]{Revision: projection.Revision}
	page.Items = make([]etcdstore.Versioned[entryrecord.Record], end-offset)
	for index := offset; index < end; index++ {
		page.Items[index-offset] = etcdstore.Versioned[entryrecord.Record]{
			Record: projection.Record.Entries[index], Revision: projection.Revision, ReadRevision: projection.ReadRevision,
		}
	}
	if end < len(projection.Record.Entries) {
		page.NextCursor = encodeEntryProjectionCursor(projection.Record.RevisionID, end)
	}
	return page, true, nil
}

func encodeEntryProjectionCursor(revisionID string, offset int) string {
	value := revisionID + ":" + strconv.Itoa(offset)
	return entryProjectionCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(value))
}

func decodeEntryProjectionCursor(value string) (string, int, error) {
	encoded, found := strings.CutPrefix(value, entryProjectionCursorPrefix)
	if !found {
		return "", 0, errs.New(errs.KindValidationFailed, "Entry page cursor is invalid")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return "", 0, errs.New(errs.KindValidationFailed, "Entry page cursor is invalid")
	}
	revisionID, rawOffset, found := strings.Cut(string(decoded), ":")
	offset, err := strconv.Atoi(rawOffset)
	if !found || err != nil || offset < 0 || ids.Validate(ids.KindTask, revisionID) != nil {
		return "", 0, errs.New(errs.KindValidationFailed, "Entry page cursor is invalid")
	}
	return revisionID, offset, nil
}
