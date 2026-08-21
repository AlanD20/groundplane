package entrymaterialization

import (
	"crypto/sha256"
	"math"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	testTaskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testStepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// L0 - pure function tests. See docs/standards.md, section 13.

// Rationale: every stage must receive the same immutable procedure metadata,
// so construction copies the input and exposes no mutation surface.
func TestNewHeaderCopiesAndPreservesCanonicalMetadata(t *testing.T) {
	t.Parallel()

	digest := Digest(sha256.Sum256([]byte("secret")))
	spec := HeaderSpec{
		TaskID:        testTaskID,
		StepID:        testStepID,
		EnvironmentID: testEnvironmentID,
		Generation:    7,
		Destination:   "config/app/settings.yaml",
		OutputKind:    OutputPlainFile,
		UID:           1000,
		GID:           1001,
		Mode:          ModeReadOnly,
		Length:        6,
		Digest:        digest,
	}

	header, err := NewHeader(spec)
	if err != nil {
		t.Fatalf("NewHeader: %v", err)
	}
	spec.Digest[0] ^= 0xff
	spec.TaskID = "changed"

	if header.TaskID() != testTaskID || header.StepID() != testStepID ||
		header.EnvironmentID() != testEnvironmentID || header.Generation() != 7 {
		t.Fatalf("identity metadata changed: %#v", header)
	}
	if header.Destination() != "config/app/settings.yaml" ||
		header.ServiceID() != "" || header.OutputKind() != OutputPlainFile || header.UID() != 1000 ||
		header.GID() != 1001 || header.Mode() != ModeReadOnly || header.Length() != 6 {
		t.Fatalf("output metadata changed: %#v", header)
	}
	if header.Digest() != digest {
		t.Fatal("digest was not copied into the immutable header")
	}
}

// Rationale: the helper must reject any procedure that weakens the exact
// ownership and mode table accepted for generated, plain, and secret files.
func TestNewHeaderEnforcesClosedOutputTable(t *testing.T) {
	t.Parallel()
	environmentDestination, err := GeneratedEnvDestination(testEnvironmentID, "")
	if err != nil {
		t.Fatalf("GeneratedEnvDestination(environment): %v", err)
	}
	serviceDestination, err := GeneratedEnvDestination(testEnvironmentID, testServiceID)
	if err != nil {
		t.Fatalf("GeneratedEnvDestination(service): %v", err)
	}
	serviceSpec := validHeaderSpec(OutputGeneratedEnv, serviceDestination, 0, 0, ModePrivate)
	serviceSpec.ServiceID = testServiceID

	tests := []struct {
		name string
		spec HeaderSpec
	}{
		{
			name: "generated environment",
			spec: validHeaderSpec(OutputGeneratedEnv, environmentDestination, 0, 0, ModePrivate),
		},
		{
			name: "generated service",
			spec: serviceSpec,
		},
		{
			name: "plain file",
			spec: validHeaderSpec(OutputPlainFile, "config/app/config.yaml", 1000, 1001, ModeReadOnly),
		},
		{
			name: "secret file",
			spec: validHeaderSpec(OutputSecretFile, "secrets/api-token", 1000, 1001, ModePrivate),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewHeader(test.spec); err != nil {
				t.Fatalf("NewHeader: %v", err)
			}
		})
	}
}

// Rationale: malformed dispatch metadata is an impossible internal procedure,
// never operator validation at the helper boundary.
func TestNewHeaderRejectsInvalidProcedureMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*HeaderSpec)
	}{
		{name: "task id", mutate: func(spec *HeaderSpec) { spec.TaskID = testStepID }},
		{name: "step id", mutate: func(spec *HeaderSpec) { spec.StepID = testTaskID }},
		{name: "environment id", mutate: func(spec *HeaderSpec) { spec.EnvironmentID = testServiceID }},
		{name: "zero generation", mutate: func(spec *HeaderSpec) { spec.Generation = 0 }},
		{name: "empty path", mutate: func(spec *HeaderSpec) { spec.Destination = "" }},
		{name: "absolute path", mutate: func(spec *HeaderSpec) { spec.Destination = "/config/app" }},
		{name: "traversal", mutate: func(spec *HeaderSpec) { spec.Destination = "../config/app" }},
		{name: "unclean path", mutate: func(spec *HeaderSpec) { spec.Destination = "config//app" }},
		{name: "backslash", mutate: func(spec *HeaderSpec) { spec.Destination = `config\app` }},
		{name: "nul", mutate: func(spec *HeaderSpec) { spec.Destination = "config/\x00app" }},
		{name: "invalid utf8", mutate: func(spec *HeaderSpec) { spec.Destination = string([]byte{0xff}) }},
		{
			name: "reserved temporary prefix",
			mutate: func(spec *HeaderSpec) {
				spec.Destination = "config/" + TemporaryPrefix + "owned"
			},
		},
		{name: "unknown output", mutate: func(spec *HeaderSpec) { spec.OutputKind = OutputKind(99) }},
		{name: "plain mode", mutate: func(spec *HeaderSpec) { spec.Mode = ModePrivate }},
		{name: "secret mode", mutate: func(spec *HeaderSpec) {
			spec.OutputKind = OutputSecretFile
			spec.Mode = ModeReadOnly
		}},
		{name: "generated uid", mutate: func(spec *HeaderSpec) {
			spec.OutputKind = OutputGeneratedEnv
			spec.Destination = "secrets/.env." + testEnvironmentID
			spec.Mode = ModePrivate
			spec.UID = 1
		}},
		{name: "generated gid", mutate: func(spec *HeaderSpec) {
			spec.OutputKind = OutputGeneratedEnv
			spec.Destination = "secrets/.env." + testEnvironmentID
			spec.Mode = ModePrivate
			spec.GID = 1
		}},
		{name: "generated destination", mutate: func(spec *HeaderSpec) {
			spec.OutputKind = OutputGeneratedEnv
			spec.ServiceID = testServiceID
			spec.Destination = "secrets/.env." + testEnvironmentID + ".human-slug"
			spec.Mode = ModePrivate
			spec.UID = 0
			spec.GID = 0
		}},
		{name: "irrelevant service scope", mutate: func(spec *HeaderSpec) { spec.ServiceID = testServiceID }},
		{name: "uid sentinel", mutate: func(spec *HeaderSpec) { spec.UID = math.MaxUint32 }},
		{name: "gid sentinel", mutate: func(spec *HeaderSpec) { spec.GID = math.MaxUint32 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			spec := validHeaderSpec(OutputPlainFile, "config/app", 1000, 1001, ModeReadOnly)
			test.mutate(&spec)
			_, err := NewHeader(spec)
			if err == nil {
				t.Fatal("NewHeader accepted invalid procedure metadata")
			}
			if kind, ok := errs.KindOf(err); !ok || kind != errs.KindInternal {
				t.Fatalf("error kind = %v, %t; want internal", kind, ok)
			}
		})
	}
}

// Rationale: generated env filenames are derived from stable scope ids in one
// place, so renderers and protocol validators cannot drift back to names.
func TestGeneratedEnvDestinationUsesExactStableScope(t *testing.T) {
	t.Parallel()

	environment, err := GeneratedEnvDestination(testEnvironmentID, "")
	if err != nil {
		t.Fatalf("GeneratedEnvDestination(environment): %v", err)
	}
	if environment != "secrets/.env."+testEnvironmentID {
		t.Fatalf("environment destination = %q", environment)
	}
	service, err := GeneratedEnvDestination(testEnvironmentID, testServiceID)
	if err != nil {
		t.Fatalf("GeneratedEnvDestination(service): %v", err)
	}
	if service != "secrets/.env."+testEnvironmentID+"."+testServiceID {
		t.Fatalf("service destination = %q", service)
	}
	if _, err := GeneratedEnvDestination(testEnvironmentID, "api-name"); err == nil {
		t.Fatal("GeneratedEnvDestination accepted a service name")
	}
}

func validHeaderSpec(kind OutputKind, destination string, uid, gid uint32, mode Mode) HeaderSpec {
	payload := []byte("secret")
	return HeaderSpec{
		TaskID:        testTaskID,
		StepID:        testStepID,
		EnvironmentID: testEnvironmentID,
		Generation:    7,
		Destination:   destination,
		OutputKind:    kind,
		UID:           uid,
		GID:           gid,
		Mode:          mode,
		Length:        uint64(len(payload)),
		Digest:        Digest(sha256.Sum256(payload)),
	}
}
