// Package backingcatalog owns supported database families and server versions.
// Provisioning policy belongs to the family adapter, not a version-named kind.
package backingcatalog

import (
	"github.com/AlanD20/groundplane/pkg/errs"
	"regexp"
)

type Version struct {
	Number       string
	Image        string
	ImagePattern string
}

type Family struct {
	Key      string
	Label    string
	Versions []Version
}

// Families returns independent catalog data. Exact managed PostgreSQL image
// bytes are selected from the authenticated release catalog during creation.
func Families() []Family {
	return []Family{
		{Key: "postgres", Label: "PostgreSQL",
			Versions: []Version{
				{
					Number:       "16",
					Image:        "postgres:16-alpine",
					ImagePattern: `^(?:(?:docker\.io|registry-1\.docker\.io)/)?(?:library/)?postgres:16(?:\.[0-9]+)?-alpine(?:[0-9]+\.[0-9]+)?(?:@sha256:[0-9a-f]{64})?$`,
				},
			}},
		{Key: "valkey", Label: "Valkey",
			Versions: []Version{
				{
					Number:       "9",
					Image:        "valkey/valkey:9-alpine",
					ImagePattern: `^(?:(?:docker\.io|registry-1\.docker\.io)/)?valkey/valkey:9(?:\.[0-9]+){0,2}-alpine(?:[0-9]+\.[0-9]+)?(?:@sha256:[0-9a-f]{64})?$`,
				},
			}},
		{Key: "mysql", Label: "MySQL",
			Versions: []Version{
				{
					Number:       "8.4",
					Image:        "mysql:8.4",
					ImagePattern: `^(?:(?:docker\.io|registry-1\.docker\.io)/)?(?:library/)?mysql:8\.4(?:\.[0-9]+)?(?:@sha256:[0-9a-f]{64})?$`,
				},
			}},
	}
}

func Resolve(family, version string) (Version, error) {
	for _, candidate := range Families() {
		if candidate.Key != family {
			continue
		}
		for _, selected := range candidate.Versions {
			if selected.Number == version {
				return selected, nil
			}
		}
		return Version{}, errs.Newf(errs.KindValidationFailed,
			"unsupported %s server version %q", candidate.Label, version)
	}
	return Version{}, errs.Newf(errs.KindValidationFailed,
		"unsupported managed database family %q", family)
}

func ValidateSelection(family, version string) error {
	if family == "" || family == "custom" {
		if version != "" {
			return errs.New(errs.KindValidationFailed, "server version requires a managed database adapter")
		}
		return nil
	}
	_, err := Resolve(family, version)
	return err
}

func Managed(family string) bool {
	for _, candidate := range Families() {
		if candidate.Key == family {
			return true
		}
	}
	return false
}

// ValidImage retains the selected server line and upstream image layout. A
// digest pins bytes; it does not authorize changing the database major version.
func ValidImage(family, version, reference string) bool {
	selected, err := Resolve(family, version)
	if err != nil {
		return false
	}
	pattern, err := regexp.Compile(selected.ImagePattern)
	return err == nil && pattern.MatchString(reference)
}
