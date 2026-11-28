package runnercredential

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	identity "github.com/AlanD20/groundplane/internal/common/runnercredential"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReadActiveBaseCredential captures the current migration-installed epoch
// identity without opening or returning its bearer handoff.
func (repository *Repository) ReadActiveBaseCredential(
	ctx context.Context, runnerID string, epoch uint64,
) ([sha256.Size]byte, identity.Binding, []keyvalue.Condition, error) {
	var digest [sha256.Size]byte
	if !validIdentity(runnerID, "") || epoch == 0 {
		return digest, identity.Binding{}, nil, errs.New(
			errs.KindValidationFailed,
			"Runner base credential identity is invalid",
		)
	}
	indexKey := runnerKey(runnerID)
	read, err := repository.read(ctx, append(registrationKeys(runnerID), indexKey), 0)
	if err != nil {
		return digest, identity.Binding{}, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	conditions, live, err := liveRegistration(read.Values, runnerID, epoch)
	if err != nil || !live {
		return digest, identity.Binding{}, nil, err
	}
	index := read.Values[3]
	indexed, err := repository.readSet(ctx, runnerID, index, read.ReadRevision)
	if err != nil {
		return digest, identity.Binding{}, nil, err
	}
	for _, item := range indexed {
		if item.record.RequestID != "" || item.record.RunnerEpoch != epoch || !item.record.EpochBound {
			continue
		}
		written, err := hex.Decode(digest[:], []byte(item.record.Digest))
		if err != nil || written != sha256.Size {
			return [sha256.Size]byte{}, identity.Binding{}, nil, corrupt()
		}
		conditions = append(conditions,
			keyvalue.Condition{Key: item.key, ModRevision: item.revision},
		)
		return digest, item.record.binding(item.revision), conditions, nil
	}
	return digest, identity.Binding{}, nil, nil
}
