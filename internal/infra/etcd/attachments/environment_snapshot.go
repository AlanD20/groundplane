package attachments

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func LoadEnvironmentAttachesAtRevision(
	ctx context.Context,
	store snapshotReadStore,
	environmentID string,
	revision int64,
) ([]etcdstore.Versioned[Record], error) {
	var result []etcdstore.Versioned[Record]
	request := etcdstore.PageRequest{Limit: 200}
	for {
		page, err := recordquery.ListIndexAtRevision(ctx, store, "attaches", "environment", environmentID,
			AttachOwnerPrefix(environmentID), AttachKey, ids.KindAttach, request, DecodeAttachRecord,
			func(record Record) string { return record.ID },
			func(record Record) bool { return record.EnvironmentID == environmentID }, revision)
		if err != nil {
			return nil, err
		}
		result = append(result, page.Items...)
		if page.NextCursor == "" {
			return result, nil
		}
		request.Cursor = page.NextCursor
	}
}
