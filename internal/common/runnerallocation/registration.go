package runnerallocation

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateRunnerBudget(cpuMillis, memoryBytes int64) error {
	if cpuMillis < 100 || cpuMillis > 1024000 {
		return errs.New(errs.KindValidationFailed, "Runner CPU budget must be between 0.1 and 1024 CPUs")
	}
	if memoryBytes < 512<<20 || memoryBytes > 1<<40 {
		return errs.New(errs.KindValidationFailed, "Runner memory budget must be between 512 MiB and 1 TiB")
	}
	return nil
}

func ValidateGitHubRunnerName(name string) error {
	if len(name) > 64 || strings.TrimSpace(name) != name {
		return errs.New(
			errs.KindValidationFailed,
			"GitHub Runner name must contain at most 64 ASCII letters, digits, dots, underscores or hyphens",
		)
	}
	for _, character := range name {
		if character < 128 && (unicode.IsLetter(character) || unicode.IsDigit(character)) ||
			strings.ContainsRune("._-", character) {
			continue
		}
		return errs.New(
			errs.KindValidationFailed,
			"GitHub Runner name must contain ASCII letters, digits, dots, underscores or hyphens",
		)
	}
	return nil
}

func ResourceSlice(runnerID string) string {
	return "groundplane-runner-" + strings.ToLower(strings.TrimPrefix(runnerID, "run_")) + ".slice"
}

func RegistrationName(runnerID, requested string) string {
	if requested != "" {
		return requested
	}
	return "gp-" + strings.ToLower(strings.TrimPrefix(runnerID, "run_"))
}

// ValidateRunnerGroup protects the line-framed registration input and preserves
// the selected GitHub group name, including its case and internal spaces.
func ValidateRunnerGroup(githubURL, group string) error {
	if group == "" || len(group) > 255 || !utf8.ValidString(group) || strings.TrimSpace(group) != group {
		return errs.New(
			errs.KindValidationFailed,
			"GitHub Runner group must contain 1–255 bytes without surrounding whitespace",
		)
	}
	for _, character := range group {
		if unicode.IsControl(character) {
			return errs.New(errs.KindValidationFailed, "GitHub Runner group cannot contain control characters")
		}
	}
	if strings.Count(strings.TrimPrefix(githubURL, "https://github.com/"), "/") == 1 && group != "Default" {
		return errs.New(
			errs.KindValidationFailed,
			"GitHub Runner groups require an organization URL; repository Runners use Default",
		)
	}
	return nil
}
