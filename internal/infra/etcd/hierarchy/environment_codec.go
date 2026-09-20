package hierarchy

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func EncodeEnvironment(record EnvironmentRecord) ([]byte, error) {
	type environmentData struct {
		ID                string                       `json:"id"`
		ProjectID         string                       `json:"project_id"`
		Name              string                       `json:"name"`
		NetworkPool       string                       `json:"network_pool"`
		VolumeDir         string                       `json:"volume_dir"`
		ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
		CreateTaskID      string                       `json:"create_task_id"`
		CreatedAt         string                       `json:"created_at"`
		DeletionTaskID    string                       `json:"deletion_task_id,omitempty"`
	}
	return recordcodec.Encode("environment", environmentData{
		ID: record.ID, ProjectID: record.ProjectID, Name: record.Name, NetworkPool: record.NetworkPool,
		VolumeDir: record.VolumeDir, ProvisioningState: record.ProvisioningState,
		CreateTaskID: record.CreateTaskID, CreatedAt: record.CreatedAt.Format(time.RFC3339Nano),
		DeletionTaskID: record.DeletionTaskID,
	})
}

func DecodeEnvironment(value []byte) (EnvironmentRecord, error) {
	type environmentData struct {
		ID                string                       `json:"id"`
		ProjectID         string                       `json:"project_id"`
		Name              string                       `json:"name"`
		NetworkPool       string                       `json:"network_pool"`
		VolumeDir         string                       `json:"volume_dir"`
		ProvisioningState EnvironmentProvisioningState `json:"provisioning_state"`
		CreateTaskID      string                       `json:"create_task_id"`
		CreatedAt         string                       `json:"created_at"`
		DeletionTaskID    string                       `json:"deletion_task_id,omitempty"`
	}
	data, err := recordcodec.Decode[environmentData](value, "environment")
	if err != nil {
		return EnvironmentRecord{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, data.CreatedAt)
	if err != nil || data.CreatedAt != createdAt.UTC().Format(time.RFC3339Nano) {
		return EnvironmentRecord{}, errs.New(errs.KindInternal, "environment record has an invalid created_at")
	}
	record := EnvironmentRecord{
		ID: data.ID, ProjectID: data.ProjectID, Name: data.Name, NetworkPool: data.NetworkPool,
		DeletionTaskID: data.DeletionTaskID,
		VolumeDir:      data.VolumeDir, ProvisioningState: data.ProvisioningState,
		CreateTaskID: data.CreateTaskID, CreatedAt: createdAt,
	}
	if err := ValidateEnvironment(record); err != nil {
		return EnvironmentRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
