package architecturecheck

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"strings"
)

const (
	productionLimit = 600
	testLimit       = 1000
)

// ReadBaseline reads and strictly validates one version-1 JSON baseline.
func ReadBaseline(ctx context.Context, path string) (Baseline, error) {
	var baseline Baseline
	if err := contextErr(ctx); err != nil {
		return baseline, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return baseline, fmt.Errorf("read baseline: %w", err)
	}
	if err := contextErr(ctx); err != nil {
		return baseline, err
	}
	if err := validateJSONDocument(data); err != nil {
		return baseline, fmt.Errorf("decode baseline: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&baseline); err != nil {
		return Baseline{}, fmt.Errorf("decode baseline: %w", err)
	}
	if err := validateRequiredBaselineFields(data); err != nil {
		return Baseline{}, err
	}
	if err := validateBaseline(baseline); err != nil {
		return Baseline{}, err
	}
	return baseline, nil
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func validateJSONDocument(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := walkJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("trailing JSON document")
		}
		return fmt.Errorf("trailing JSON: %w", err)
	}
	return nil
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, isDelim := token.(json.Delim)
	if !isDelim {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("object key is not a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return fmt.Errorf("duplicate object field %q", key)
			}
			seen[key] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim('}') {
			return fmt.Errorf("object does not terminate")
		}
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil {
			return err
		}
		if end != json.Delim(']') {
			return fmt.Errorf("array does not terminate")
		}
	default:
		return fmt.Errorf("unexpected JSON delimiter %q", delim)
	}
	return nil
}

func validateRequiredBaselineFields(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("decode baseline object: %w", err)
	}
	for _, field := range []string{"version", "limits", "oversized_files", "frozen_totals", "legacy_findings"} {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("baseline field %q is required", field)
		}
	}
	return nil
}

func validateBaseline(baseline Baseline) error {
	if baseline.Version != 1 {
		return fmt.Errorf("baseline version must be 1")
	}
	if baseline.Limits.Production != productionLimit || baseline.Limits.Test != testLimit {
		return fmt.Errorf("baseline limits must be production=%d and test=%d", productionLimit, testLimit)
	}
	if baseline.OversizedFiles == nil || baseline.FrozenTotals == nil || baseline.LegacyFindings == nil {
		return fmt.Errorf("baseline arrays must be present")
	}
	if err := validateFileLines("oversized_files", baseline.OversizedFiles, true); err != nil {
		return err
	}
	if err := validateFileLines("frozen_totals", baseline.FrozenTotals, false); err != nil {
		return err
	}
	requiredFrozenTotals := []string{"internal/app", "internal/infra/etcd"}
	if len(baseline.FrozenTotals) != len(requiredFrozenTotals) {
		return fmt.Errorf("frozen_totals must contain internal/app and internal/infra/etcd")
	}
	for index, required := range requiredFrozenTotals {
		if baseline.FrozenTotals[index].Path != required {
			return fmt.Errorf("frozen_totals must contain internal/app and internal/infra/etcd")
		}
	}
	previous := ""
	for index, exception := range baseline.LegacyFindings {
		if err := validateRelativePath(exception.Path); err != nil {
			return fmt.Errorf("legacy_findings[%d].path: %w", index, err)
		}
		if !legacyRule(exception.Rule) {
			return fmt.Errorf("legacy_findings[%d].rule is not eligible for an exception", index)
		}
		if strings.TrimSpace(exception.Subject) == "" || strings.TrimSpace(exception.Reason) == "" {
			return fmt.Errorf("legacy_findings[%d].subject and reason must not be empty", index)
		}
		key := exception.Path + "\x00" + exception.Rule + "\x00" + exception.Subject
		if index > 0 && key <= previous {
			return fmt.Errorf("legacy_findings must be sorted and unique")
		}
		previous = key
	}
	return nil
}

func legacyRule(rule string) bool {
	switch rule {
	case "interface-constructor",
		"reflect-import",
		"layer-import",
		"local-concrete-recovery",
		"open-model-field",
		"json-roundtrip-conversion":
		return true
	default:
		return false
	}
}

func validateFileLines(field string, entries []FileLines, oversized bool) error {
	previous := ""
	for index, entry := range entries {
		if err := validateRelativePath(entry.Path); err != nil {
			return fmt.Errorf("%s[%d].path: %w", field, index, err)
		}
		if index > 0 && entry.Path <= previous {
			return fmt.Errorf("%s paths must be sorted and unique", field)
		}
		previous = entry.Path
		limit := productionLimit
		if isTestPath(entry.Path) {
			limit = testLimit
		}
		if entry.Lines < 1 || (oversized && entry.Lines <= limit) {
			return fmt.Errorf("%s[%d].lines is invalid", field, index)
		}
		if oversized && isExplicitGeneratedPath(entry.Path) {
			return fmt.Errorf("%s[%d].path names generated source", field, index)
		}
	}
	return nil
}

func validateRelativePath(value string) error {
	if value == "" || strings.Contains(value, "\\") || strings.HasPrefix(value, "/") || pathpkg.Clean(value) != value {
		return fmt.Errorf("path must be a normalized relative slash path")
	}
	for _, part := range strings.Split(value, "/") {
		if part == "" || part == "." || part == ".." {
			return fmt.Errorf("path must be a normalized relative slash path")
		}
	}
	return nil
}
