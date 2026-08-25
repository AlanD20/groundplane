package architecturecheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	pathpkg "path"
	"path/filepath"
	"sort"
	"strings"
)

var sourceRoots = []string{"cmd", "internal", "pkg", "console/src"}

var ignoredDirectories = map[string]struct{}{
	".cache":       {},
	".git":         {},
	".hg":          {},
	".next":        {},
	".svn":         {},
	"__pycache__":  {},
	"build":        {},
	"cache":        {},
	"coverage":     {},
	"dist":         {},
	"node_modules": {},
	"target":       {},
	"temp":         {},
	"testdata":     {},
	"tmp":          {},
	"vendor":       {},
}

var catchAllDirectories = map[string]struct{}{
	"common": {}, "helper": {}, "helpers": {}, "interfaces": {}, "misc": {},
	"shared": {}, "types": {}, "util": {}, "utils": {},
}

type sourceFile struct {
	rel    string
	abs    string
	data   []byte
	lines  int
	isTest bool
	ext    string
	dir    string
}

type discoveredSources struct {
	files       []*sourceFile
	directories map[string]struct{}
	findings    []Finding
}

// Check scans the requested source root and returns deterministic findings.
func Check(ctx context.Context, root string, baseline Baseline) ([]Finding, error) {
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateBaseline(baseline); err != nil {
		return nil, fmt.Errorf("invalid baseline: %w", err)
	}
	rootInfo, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("stat checker root: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, fmt.Errorf("checker root is not a directory")
	}
	discovered, err := discoverSources(ctx, root)
	if err != nil {
		return nil, err
	}
	findings := append([]Finding(nil), discovered.findings...)
	findings = append(findings, checkLineRatchets(discovered.files, baseline)...)
	findings = append(findings, checkFrozenTotals(discovered, baseline)...)
	findings = append(findings, checkGoRules(ctx, root, discovered.files)...)
	findings = append(findings, checkTypeScriptRules(discovered.files)...)
	findings = append(findings, checkCatchAllDirectories(discovered.directories)...)
	findings = applyLegacyFindings(findings, baseline)
	sortFindings(findings)
	return findings, nil
}

func applyLegacyFindings(findings []Finding, baseline Baseline) []Finding {
	legacy := make(map[string]LegacyFinding, len(baseline.LegacyFindings))
	for _, exception := range baseline.LegacyFindings {
		legacy[legacyFindingKey(exception.Path, exception.Rule, exception.Subject)] = exception
	}
	used := make(map[string]struct{}, len(legacy))
	filtered := make([]Finding, 0, len(findings))
	for _, finding := range findings {
		key := legacyFindingKey(finding.Path, finding.Rule, finding.Subject)
		if _, exempt := legacy[key]; exempt {
			used[key] = struct{}{}
			continue
		}
		filtered = append(filtered, finding)
	}
	for key, exception := range legacy {
		if _, found := used[key]; found {
			continue
		}
		filtered = append(filtered, Finding{Path: exception.Path, Line: 1, Column: 1, Rule: "stale-legacy-finding", Subject: exception.Subject, Message: fmt.Sprintf("legacy finding %s is no longer present", exception.Rule)})
	}
	return filtered
}

func legacyFindingKey(path, rule, subject string) string {
	return path + "\x00" + rule + "\x00" + subject
}

func discoverSources(ctx context.Context, root string) (discoveredSources, error) {
	result := discoveredSources{directories: make(map[string]struct{})}
	seenFiles := make(map[string]struct{})
	seenDirectories := make(map[string]struct{})
	for _, sourceRoot := range sourceRoots {
		if err := contextErr(ctx); err != nil {
			return result, err
		}
		absoluteRoot := filepath.Join(root, filepath.FromSlash(sourceRoot))
		if _, err := os.Stat(absoluteRoot); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return result, fmt.Errorf("stat source root %s: %w", sourceRoot, err)
		}
		err := filepath.WalkDir(absoluteRoot, func(path string, entry os.DirEntry, walkErr error) error {
			if contextErr(ctx) != nil {
				return contextErr(ctx)
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return relErr
			}
			rel = filepath.ToSlash(rel)
			if walkErr != nil {
				result.findings = append(result.findings, Finding{Path: rel, Line: 1, Column: 1, Rule: "read-error", Message: walkErr.Error()})
				return nil
			}
			if entry.IsDir() {
				if _, ignored := ignoredDirectories[entry.Name()]; ignored {
					return filepath.SkipDir
				}
				if _, seen := seenDirectories[rel]; !seen {
					seenDirectories[rel] = struct{}{}
					result.directories[rel] = struct{}{}
				}
				return nil
			}
			if !isSourcePath(rel) || isExplicitGeneratedPath(rel) {
				return nil
			}
			if _, seen := seenFiles[rel]; seen {
				return nil
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				result.findings = append(result.findings, Finding{Path: rel, Line: 1, Column: 1, Rule: "read-error", Message: readErr.Error()})
				return nil
			}
			seenFiles[rel] = struct{}{}
			result.files = append(result.files, &sourceFile{
				rel: rel, abs: path, data: data, lines: physicalLines(data),
				isTest: isTestPath(rel), ext: filepath.Ext(rel), dir: pathpkg.Dir(rel),
			})
			return nil
		})
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return result, err
			}
			return result, fmt.Errorf("walk source root %s: %w", sourceRoot, err)
		}
	}
	sort.Slice(result.files, func(i, j int) bool { return result.files[i].rel < result.files[j].rel })
	return result, nil
}

func isSourcePath(rel string) bool {
	ext := filepath.Ext(rel)
	return ext == ".go" || ext == ".ts" || ext == ".tsx" || ext == ".mjs"
}

func isExplicitGeneratedPath(rel string) bool {
	return rel == "console/src/lib/api.generated.ts" || strings.HasPrefix(rel, "internal/cli/apiclient/generated/") || rel == "internal/cli/apiclient/generated"
}

func isTestPath(rel string) bool {
	base := filepath.Base(rel)
	if strings.HasSuffix(base, "_test.go") {
		return true
	}
	return strings.HasSuffix(base, ".test.ts") || strings.HasSuffix(base, ".test.tsx") || strings.HasSuffix(base, ".spec.ts") || strings.HasSuffix(base, ".spec.tsx")
}

func physicalLines(data []byte) int {
	if len(data) == 0 {
		return 0
	}
	lines := 1 + strings.Count(string(data), "\n")
	if data[len(data)-1] == '\n' {
		lines--
	}
	return lines
}

func checkLineRatchets(files []*sourceFile, baseline Baseline) []Finding {
	entries := make(map[string]FileLines, len(baseline.OversizedFiles))
	for _, entry := range baseline.OversizedFiles {
		entries[entry.Path] = entry
	}
	findings := make([]Finding, 0)
	current := make(map[string]struct{}, len(files))
	for _, file := range files {
		current[file.rel] = struct{}{}
		limit := productionLimit
		if file.isTest {
			limit = testLimit
		}
		entry, recorded := entries[file.rel]
		switch {
		case file.lines > limit && !recorded:
			findings = append(findings, Finding{Path: file.rel, Line: 1, Column: 1, Rule: "missing-oversized-baseline", Message: fmt.Sprintf("file has %d lines, over the %d-line limit, but is not in oversized_files", file.lines, limit)})
		case file.lines > limit && entry.Lines < file.lines:
			findings = append(findings, Finding{Path: file.rel, Line: 1, Column: 1, Rule: "oversized-file-growth", Message: fmt.Sprintf("baseline allows %d lines but file has %d", entry.Lines, file.lines)})
		case file.lines <= limit && recorded:
			findings = append(findings, Finding{Path: file.rel, Line: 1, Column: 1, Rule: "stale-oversized-baseline", Message: fmt.Sprintf("file has %d lines and no longer exceeds the %d-line limit", file.lines, limit)})
		}
	}
	for _, entry := range baseline.OversizedFiles {
		if _, exists := current[entry.Path]; !exists {
			findings = append(findings, Finding{Path: entry.Path, Line: 1, Column: 1, Rule: "stale-oversized-baseline", Message: "oversized_files entry does not name a current source file"})
		}
	}
	return findings
}

func checkFrozenTotals(discovered discoveredSources, baseline Baseline) []Finding {
	findings := make([]Finding, 0)
	for _, entry := range baseline.FrozenTotals {
		if _, exists := discovered.directories[entry.Path]; !exists {
			findings = append(findings, Finding{Path: entry.Path, Line: 1, Column: 1, Rule: "stale-frozen-total", Message: "frozen_totals entry does not name a current source directory"})
			continue
		}
		total := 0
		fileCount := 0
		for _, file := range discovered.files {
			if file.dir == entry.Path && file.ext == ".go" && !file.isTest {
				total += file.lines
				fileCount++
			}
		}
		if fileCount == 0 {
			findings = append(findings, Finding{Path: entry.Path, Line: 1, Column: 1, Rule: "stale-frozen-total", Message: "frozen_totals entry does not name a direct production Go package"})
			continue
		}
		if total != entry.Lines {
			findings = append(findings, Finding{Path: entry.Path, Line: 1, Column: 1, Rule: "frozen-total-drift", Message: fmt.Sprintf("frozen total is %d lines but current direct production Go total is %d", entry.Lines, total)})
		}
	}
	return findings
}

func checkCatchAllDirectories(directories map[string]struct{}) []Finding {
	findings := make([]Finding, 0)
	for directory := range directories {
		name := pathpkg.Base(directory)
		if _, forbidden := catchAllDirectories[name]; !forbidden || allowedCatchAllDirectory(directory) {
			continue
		}
		findings = append(findings, Finding{Path: directory, Line: 1, Column: 1, Rule: "catch-all-directory", Message: fmt.Sprintf("directory name %q is reserved for narrowly named packages", name)})
	}
	return findings
}

func allowedCatchAllDirectory(directory string) bool {
	return directory == "internal/common" || directory == "internal/cli/common" || directory == "console/src/components/common"
}

func sortFindings(findings []Finding) {
	sort.SliceStable(findings, func(i, j int) bool {
		left, right := findings[i], findings[j]
		if left.Path != right.Path {
			return left.Path < right.Path
		}
		if left.Line != right.Line {
			return left.Line < right.Line
		}
		if left.Column != right.Column {
			return left.Column < right.Column
		}
		if left.Rule != right.Rule {
			return left.Rule < right.Rule
		}
		if left.Subject != right.Subject {
			return left.Subject < right.Subject
		}
		return left.Message < right.Message
	})
}

// WriteFindings writes a stable JSON array of findings.
func WriteFindings(w io.Writer, findings []Finding) error {
	copyFindings := append([]Finding(nil), findings...)
	sortFindings(copyFindings)
	if copyFindings == nil {
		copyFindings = []Finding{}
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(copyFindings)
}
