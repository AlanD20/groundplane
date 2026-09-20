package runners

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/slug"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/url"
	"slices"
	"sort"
	"strings"
)

func ValidateRunnerDesired(desired RunnerDesiredRecord) error {
	if err := ValidateRunnerOwnership(desired); err != nil {
		return err
	}
	normalized, err := NormalizeRunnerDesired(desired)
	if err != nil || normalized.Slug != desired.Slug || normalized.GitHubURL != desired.GitHubURL ||
		normalized.ImageRef != desired.ImageRef || !slices.Equal(normalized.Labels, desired.Labels) {
		return errs.New(errs.KindValidationFailed, "runner desired state is not canonical")
	}
	return nil
}

func ValidateRunnerOwnership(desired RunnerDesiredRecord) error {
	if ids.Validate(ids.KindRunner, desired.ID) != nil || ids.Validate(ids.KindTenant, desired.TenantID) != nil {
		return errs.New(errs.KindValidationFailed, "runner identity is invalid")
	}
	switch desired.OwnerKind {
	case RunnerOwnerTenant:
		if desired.OwnerID != desired.TenantID {
			return errs.New(errs.KindValidationFailed, "tenant-owned runner has mismatched ownership")
		}
	case RunnerOwnerProject:
		if ids.Validate(ids.KindProject, desired.OwnerID) != nil {
			return errs.New(errs.KindValidationFailed, "project-owned runner owner is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "runner owner kind is invalid")
	}
	return nil
}

func NormalizeRunnerDesired(desired RunnerDesiredRecord) (RunnerDesiredRecord, error) {
	if err := ValidateRunnerOwnership(desired); err != nil {
		return RunnerDesiredRecord{}, err
	}
	if err := ValidateRunnerSlug(desired.Slug); err != nil {
		return RunnerDesiredRecord{}, err
	}
	githubURL, err := CanonicalRunnerGitHubURL(desired.OwnerKind, desired.GitHubURL)
	if err != nil {
		return RunnerDesiredRecord{}, err
	}
	labels, err := canonicalRunnerLabels(desired.Labels)
	if err != nil {
		return RunnerDesiredRecord{}, err
	}
	if !validRunnerImageRef(desired.ImageRef) {
		return RunnerDesiredRecord{}, errs.New(errs.KindValidationFailed, "runner image_ref is invalid")
	}
	desired.GitHubURL = githubURL
	desired.Labels = labels
	return desired, nil
}

func ValidateRunnerSlug(value string) error {
	if !slug.Valid(value) {
		return errs.New(errs.KindValidationFailed, "runner slug must be a lowercase ASCII label of 1-63 bytes")
	}
	return nil
}

func RunnerName(runnerID string) (string, error) {
	if ids.Validate(ids.KindRunner, runnerID) != nil {
		return "", errs.New(errs.KindValidationFailed, "runner id is invalid")
	}
	return "gp-" + strings.ToLower(strings.TrimPrefix(runnerID, string(ids.KindRunner)+"_")), nil
}

func CanonicalRunnerGitHubURL(ownerKind RunnerOwnerKind, value string) (string, error) {
	for index := 0; index < len(value); index++ {
		if value[index] < 0x20 || value[index] >= 0x7f || value[index] == '\\' || value[index] == '%' {
			return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
		}
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host != "github.com" || parsed.User != nil ||
		parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" {
		return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
	}
	path := strings.TrimPrefix(parsed.Path, "/")
	if strings.HasSuffix(path, "//") {
		return "", errs.New(errs.KindValidationFailed, "runner github_url is invalid")
	}
	path = strings.TrimSuffix(path, "/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || !validGitHubOrganization(parts[0]) {
		return "", errs.New(errs.KindValidationFailed, "runner github_url organization is invalid")
	}
	switch ownerKind {
	case RunnerOwnerTenant:
		if len(parts) != 1 {
			return "", errs.New(errs.KindValidationFailed, "tenant runner github_url must identify an organization")
		}
	case RunnerOwnerProject:
		if len(parts) != 2 || !validGitHubRepository(parts[1]) {
			return "", errs.New(errs.KindValidationFailed, "project runner github_url must identify a repository")
		}
	default:
		return "", errs.New(errs.KindValidationFailed, "runner owner kind is invalid")
	}
	return "https://github.com/" + strings.ToLower(strings.Join(parts, "/")), nil
}

func validGitHubOrganization(value string) bool {
	if len(value) < 1 || len(value) > 39 || value[0] == '-' || value[len(value)-1] == '-' ||
		strings.Contains(value, "--") {
		return false
	}
	for index := range value {
		character := value[index]
		if !asciiAlphanumeric(character) && character != '-' {
			return false
		}
	}
	return true
}

func validGitHubRepository(value string) bool {
	if len(value) < 1 || len(value) > 100 || value == "." || value == ".." || strings.HasSuffix(value, ".") ||
		strings.HasSuffix(strings.ToLower(value), ".git") {
		return false
	}
	for index := range value {
		character := value[index]
		if !asciiAlphanumeric(character) && character != '.' && character != '-' && character != '_' {
			return false
		}
	}
	return true
}

func canonicalRunnerLabels(values []string) ([]string, error) {
	if len(values) > 32 {
		return nil, errs.New(errs.KindValidationFailed, "runner labels exceed the maximum of 32")
	}
	labels := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if len(value) < 1 || len(value) > 64 {
			return nil, errs.New(errs.KindValidationFailed, "runner labels are invalid")
		}
		for index := range value {
			character := value[index]
			if !asciiAlphanumeric(character) && character != '.' && character != '_' && character != '-' {
				return nil, errs.New(errs.KindValidationFailed, "runner labels are invalid")
			}
		}
		canonical := strings.ToLower(value)
		if canonical == "self-hosted" || canonical == "linux" || canonical == "x64" || canonical == "arm64" {
			return nil, errs.New(errs.KindValidationFailed, "runner label is reserved")
		}
		if _, exists := seen[canonical]; exists {
			return nil, errs.New(errs.KindValidationFailed, "runner labels must be case-insensitively unique")
		}
		seen[canonical] = struct{}{}
		labels = append(labels, canonical)
	}
	sort.Strings(labels)
	return labels, nil
}

func asciiAlphanumeric(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'A' && value <= 'Z' || value >= 'a' && value <= 'z'
}

func validRunnerImageRef(value string) bool {
	return imageref.IsDigestPinned(value)
}

func EncodeRunnerDesiredRecord(record RunnerDesiredRecord) ([]byte, error) {
	if err := ValidateRunnerDesired(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_desired", record)
}

func DecodeRunnerDesiredRecord(value []byte) (RunnerDesiredRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerDesiredRecord{}, errs.New(errs.KindInternal, "runner desired record is corrupt")
	}
	record, err := recordcodec.Decode[RunnerDesiredRecord](value, "runner_desired")
	if err != nil || ValidateRunnerDesired(record) != nil {
		return RunnerDesiredRecord{}, errs.New(errs.KindInternal, "runner desired record is corrupt")
	}
	record.Labels = append([]string(nil), record.Labels...)
	return record, nil
}

func DecodeRunnerDesiredAggregate(value []byte) (RunnerRecord, error) {
	desired, err := DecodeRunnerDesiredRecord(value)
	if err != nil {
		return RunnerRecord{}, err
	}
	return RunnerRecord{Desired: desired}, nil
}

func EncodeRunnerLifecycleRecord(record RunnerLifecycleRecord) ([]byte, error) {
	if err := ValidateRunnerLifecycle(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("runner_lifecycle", record)
}

func DecodeRunnerLifecycleRecord(value []byte) (RunnerLifecycleRecord, error) {
	if len(value) > MaximumRunnerPersistenceBytes {
		return RunnerLifecycleRecord{}, errs.New(errs.KindInternal, "runner lifecycle record is corrupt")
	}
	record, err := recordcodec.Decode[RunnerLifecycleRecord](value, "runner_lifecycle")
	if err != nil || ValidateRunnerLifecycle(record) != nil {
		return RunnerLifecycleRecord{}, errs.New(errs.KindInternal, "runner lifecycle record is corrupt")
	}
	return record, nil
}
