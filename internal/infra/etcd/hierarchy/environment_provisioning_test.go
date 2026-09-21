package hierarchy

import (
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testing "testing"
	time "time"
)

func TestEnvironmentProvisioningFieldsRoundTrip(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, createdAt, 1)
	projectID := ids.NewAt(ids.KindProject, createdAt, 2)
	record := EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ID:                environmentID,
		ProjectID:         projectID,
		Name:              "Production",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningProvisioning,
		CreateTaskID:      ids.NewAt(ids.KindTask, createdAt, 3),
		CreatedAt:         createdAt,
	}

	encoded, err := EncodeEnvironment(record)
	if err != nil {
		t.Fatalf("encodeEnvironment() error = %v", err)
	}
	decoded, err := DecodeEnvironment(encoded)
	if err != nil {
		t.Fatalf("decodeEnvironment() error = %v", err)
	}
	if decoded != record {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}
}

func TestEnvironmentProvisioningValidationRejectsMissingStateAndWrongTaskKind(t *testing.T) {
	t.Parallel()
	createdAt := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	valid := EnvironmentRecord{NetworkPool: "10.40.0.0/16",
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, createdAt, 4),
	}

	missingState := valid
	missingState.ProvisioningState = ""
	if err := validateEnvironmentProvisioning(missingState); err == nil {
		t.Fatal("validateEnvironmentProvisioning() accepted an empty state")
	}

	wrongTaskKind := valid
	wrongTaskKind.CreateTaskID = ids.NewAt(ids.KindEnvironment, createdAt, 5)
	if err := validateEnvironmentProvisioning(wrongTaskKind); err == nil {
		t.Fatal("validateEnvironmentProvisioning() accepted a non-Task id")
	}
}
