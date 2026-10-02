package postgres16protocol

import (
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const ManagedReleaseManifestPath = "/usr/local/share/groundplane/postgres16-release.json"

// ManagedReleaseManifest is an image-build artifact, never desired state.
// Its bytes and the image containing them must both match the selected release.
type ManagedReleaseManifest struct {
	Schema              uint32 `json:"schema"`
	OS                  string `json:"os"`
	Architecture        string `json:"architecture"`
	PostgreSQLMajor     uint32 `json:"postgresql_major"`
	HelperSHA256        string `json:"helper_sha256"`
	GateSHA256          string `json:"gate_sha256"`
	PGDumpSHA256        string `json:"pg_dump_sha256"`
	PGRestoreSHA256     string `json:"pg_restore_sha256"`
	PSQLSHA256          string `json:"psql_sha256"`
	LaunchProfileSHA256 string `json:"launch_profile_sha256"`
	GateSeccompSHA256   string `json:"gate_seccomp_sha256"`
}

func (manifest ManagedReleaseManifest) Authority() (ConfinementReleaseAuthority, error) {
	var authority ConfinementReleaseAuthority
	if manifest.Schema != 1 || manifest.OS != "linux" ||
		(manifest.Architecture != "amd64" && manifest.Architecture != "arm64") ||
		manifest.PostgreSQLMajor != PostgreSQLMajor {
		return authority, invalidConfinement("managed PostgreSQL release platform is invalid")
	}
	values := []struct {
		text string
		into *Digest
	}{
		{manifest.HelperSHA256, &authority.HelperSHA256},
		{manifest.GateSHA256, &authority.GateSHA256},
		{manifest.PGDumpSHA256, &authority.PGDumpSHA256},
		{manifest.PGRestoreSHA256, &authority.PGRestoreSHA256},
		{manifest.PSQLSHA256, &authority.PSQLSHA256},
		{manifest.LaunchProfileSHA256, &authority.LaunchProfileSHA256},
		{manifest.GateSeccompSHA256, &authority.GateSeccompSHA256},
	}
	for _, value := range values {
		if len(value.text) != 64 || strings.ToLower(value.text) != value.text {
			return ConfinementReleaseAuthority{}, invalidConfinement("managed PostgreSQL release digest is invalid")
		}
		if _, err := hex.Decode(value.into[:], []byte(value.text)); err != nil {
			return ConfinementReleaseAuthority{}, invalidConfinement("managed PostgreSQL release digest is invalid")
		}
	}
	if authority.LaunchProfileSHA256 != ManagedLaunchProfileSHA256() {
		return ConfinementReleaseAuthority{}, invalidConfinement("managed PostgreSQL launch profile changed")
	}
	return authority, authority.Validate()
}

func DecodeManagedReleaseManifest(data []byte) (ManagedReleaseManifest, error) {
	var manifest ManagedReleaseManifest
	if len(data) == 0 || len(data) > 4096 {
		return manifest, invalidConfinement("managed PostgreSQL release manifest is invalid")
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return ManagedReleaseManifest{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	// Canonical encoding rejects duplicate keys and trailing values as well as
	// alternate representations. The release packager writes this exact form.
	canonical, err := json.Marshal(manifest)
	if err != nil || string(canonical) != strings.TrimSpace(string(data)) {
		return ManagedReleaseManifest{}, invalidConfinement("managed PostgreSQL release manifest is not canonical")
	}
	if _, err := manifest.Authority(); err != nil {
		return ManagedReleaseManifest{}, err
	}
	return manifest, nil
}
