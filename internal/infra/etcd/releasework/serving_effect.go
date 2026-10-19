package releasework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func readServingEffect(ctx context.Context, store Reader, input releaseexecution.Input, serving *ServingProofReference,
	revision int64, notAfter time.Time,
) (releaseexecution.EffectEvidence, *agentpb.ReleaseHelperResponse, keyvalue.Condition, error) {
	key := EffectEvidenceKey(input.Attempt.ID, serving.Boundary.Sequence)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return releaseexecution.EffectEvidence{}, nil, keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return releaseexecution.EffectEvidence{}, nil, keyvalue.Condition{}, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil || value.Key != key || value.ModRevision <= 0 || value.ModRevision > revision {
		return releaseexecution.EffectEvidence{}, nil, keyvalue.Condition{}, invalid()
	}
	evidence, err := releaseexecution.DecodeEffectEvidence(input, serving.Boundary, value.Value, serving.Evidence)
	if err != nil || evidence.RecordedAt.After(notAfter) {
		return releaseexecution.EffectEvidence{}, nil, keyvalue.Condition{}, invalid()
	}
	response := &agentpb.ReleaseHelperResponse{}
	if len(evidence.Result.Helper) == 0 || proto.Unmarshal(evidence.Result.Helper, response) != nil ||
		releaseexecution.ValidateHelperResult(input, serving.Boundary.Progress, response) != nil {
		return releaseexecution.EffectEvidence{}, nil, keyvalue.Condition{}, invalid()
	}
	return evidence, response, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}
