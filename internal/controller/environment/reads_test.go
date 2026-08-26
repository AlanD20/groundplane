package environment

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	readTestProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	readTestEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	readTestTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: list scope and pagination are application inputs, so invalid
// values must fail before the capability reaches durable storage.
func TestReaderListValidatesInputBeforeReadingRecords(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		input ListInput
	}{
		{name: "invalid project", input: ListInput{ProjectID: "production"}},
		{name: "negative limit", input: ListInput{ProjectID: readTestProjectID, Limit: -1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &readRepositoryFake{}
			_, err := NewReader(repository).List(context.Background(), test.input)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || repository.listCalls != 0 {
				t.Fatalf("List() error/calls = %v, %d", err, repository.listCalls)
			}
		})
	}
}

// Rationale: an invalid Environment id must be rejected by the capability so
// no persistence implementation can accidentally interpret or normalize it.
func TestReaderGetValidatesIDBeforeReadingRepository(t *testing.T) {
	t.Parallel()
	repository := &readRepositoryFake{}
	_, err := NewReader(repository).Get(context.Background(), GetInput{ID: "production"})
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || repository.getCalls != 0 {
		t.Fatalf("Get() error/calls = %v, %d", err, repository.getCalls)
	}
}

// Rationale: valid reads preserve capability-owned allocation identity,
// provisioning state, active Task projection, and opaque pagination.
func TestReaderReturnsRepositoryProjection(t *testing.T) {
	t.Parallel()
	repository := &readRepositoryFake{
		environment: readTestEnvironment(Failed),
		page: Page{
			Items:      []Environment{readTestEnvironment(Ready)},
			NextCursor: "next-environment",
		},
	}
	reader := NewReader(repository)
	page, err := reader.List(context.Background(), ListInput{
		ProjectID: readTestProjectID,
		Limit:     2,
		Cursor:    "current-environment",
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if repository.projectID != readTestProjectID || repository.request.Limit != 2 ||
		repository.request.Cursor != "current-environment" || page.NextCursor != "next-environment" ||
		len(page.Items) != 1 || page.Items[0].ProvisioningState != Ready {
		t.Fatalf("List() = %#v; repository = %q, %#v", page, repository.projectID, repository.request)
	}
	detail, err := reader.Get(context.Background(), GetInput{ID: readTestEnvironmentID})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if repository.environmentID != readTestEnvironmentID || detail.ID != readTestEnvironmentID ||
		detail.ProjectID != readTestProjectID || detail.NetworkPool != "10.40.0.0/16" ||
		detail.VolumeDir == "" || detail.CreateTaskID == nil || *detail.CreateTaskID != readTestTaskID ||
		detail.ProvisioningState != Failed {
		t.Fatalf("Get() = %#v; environment id = %q", detail, repository.environmentID)
	}
}

type readRepositoryFake struct {
	environment   Environment
	page          Page
	environmentID string
	projectID     string
	request       PageRequest
	getCalls      int
	listCalls     int
}

func (repository *readRepositoryFake) GetEnvironment(
	_ context.Context,
	id string,
) (Environment, error) {
	repository.getCalls++
	repository.environmentID = id
	return repository.environment, nil
}

func (repository *readRepositoryFake) ListEnvironments(
	_ context.Context,
	projectID string,
	request PageRequest,
) (Page, error) {
	repository.listCalls++
	repository.projectID = projectID
	repository.request = request
	return repository.page, nil
}

func readTestEnvironment(state ProvisioningState) Environment {
	taskID := readTestTaskID
	return Environment{
		ID: readTestEnvironmentID, ProjectID: readTestProjectID, Name: "production",
		NetworkPool:       "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + readTestProjectID + "/" + readTestEnvironmentID,
		ProvisioningState: state,
		CreateTaskID:      &taskID,
	}
}
