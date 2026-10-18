package releases

import (
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// WorkPublication binds Release visibility to the native executable input.
// Runtime artifacts live once in that captured input, not in diagnostic records
// or a second serialized execution plan. Settlement records applied runtime.
type WorkPublication struct {
	PublicationID      string                                `json:"publication_id"`
	OperationID        string                                `json:"operation_id"`
	TaskID             string                                `json:"task_id"`
	WorkItemID         string                                `json:"work_item_id"`
	EnvironmentID      string                                `json:"environment_id"`
	ServiceID          string                                `json:"service_id"`
	ManifestDigest     string                                `json:"manifest_digest"`
	Input              workexecution.Input                   `json:"input"`
	NativePredecessors []BlueprintNativePredecessorReference `json:"native_predecessors,omitempty"`
	PublishedAt        time.Time                             `json:"published_at"`
}

func (publication WorkPublication) Validate() error {
	digest, digestErr := hex.DecodeString(publication.ManifestDigest)
	if ValidatePublicationID(publication.PublicationID) != nil ||
		ids.Validate(ids.KindOperation, publication.OperationID) != nil ||
		ids.Validate(ids.KindTask, publication.TaskID) != nil ||
		ids.Validate(ids.KindWorkItem, publication.WorkItemID) != nil ||
		ids.Validate(ids.KindEnvironment, publication.EnvironmentID) != nil ||
		ids.Validate(ids.KindService, publication.ServiceID) != nil ||
		digestErr != nil || len(digest) != 32 || publication.Input.Validate() != nil ||
		publication.PublishedAt.IsZero() || publication.PublishedAt.Location() != time.UTC {
		return errs.New(errs.KindValidationFailed, "Release work publication identity is invalid")
	}
	if len(publication.NativePredecessors) != 1 ||
		publication.NativePredecessors[0].ServiceID != publication.ServiceID ||
		publication.NativePredecessors[0].Validate() != nil {
		return errs.New(errs.KindValidationFailed, "Release work publication requires its exact Service predecessor")
	}
	return nil
}

func EncodeWorkPublication(publication WorkPublication) ([]byte, error) {
	if err := publication.Validate(); err != nil {
		return nil, err
	}
	return recordcodec.Encode("release-work-publication", publication)
}

func DecodeWorkPublication(value []byte) (WorkPublication, error) {
	publication, err := recordcodec.Decode[WorkPublication](value, "release-work-publication")
	if err != nil {
		return WorkPublication{}, err
	}
	if err := publication.Validate(); err != nil {
		return WorkPublication{}, err
	}
	return publication, nil
}
