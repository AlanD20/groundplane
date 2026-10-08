package softwarepreparation

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type githubCommit struct {
	SHA    string `json:"sha"`
	Commit struct {
		Committer struct {
			Date string `json:"date"`
		} `json:"committer"`
	} `json:"commit"`
}

type githubRelease struct {
	TagName    string `json:"tag_name"`
	Draft      bool   `json:"draft"`
	Prerelease bool   `json:"prerelease"`
	Assets     []struct {
		Name   string `json:"name"`
		Size   int64  `json:"size"`
		Digest string `json:"digest"`
	} `json:"assets"`
}

// Resolve converts a branch, tag, commit or published release into immutable
// Task input. It performs no build, registry publication or activation.
func (preparer *Preparer) Resolve(ctx context.Context, input ResolveInput) (ResolvedSource, error) {
	if err := checkContext(ctx); err != nil {
		return ResolvedSource{}, err
	}
	if !input.Selection.valid() || (input.SourceKind != SourceRef && input.SourceKind != Release) ||
		!validOriginalRef(input.OriginalRef) || !input.Platform.valid() || input.Platform != HostPlatform() {
		return ResolvedSource{}, errs.New(errs.KindValidationFailed, "software source selection is invalid")
	}
	if input.SourceKind == Release && !validReleaseTag(input.Selection, input.OriginalRef) {
		return ResolvedSource{}, errs.New(
			errs.KindValidationFailed,
			"published release tag is outside its component namespace",
		)
	}

	resolved := ResolvedSource{
		Selection: input.Selection, SourceKind: input.SourceKind, OriginalRef: input.OriginalRef,
		Platform: input.Platform,
	}
	if input.SourceKind == SourceRef {
		commit, err := preparer.resolveCommit(ctx, input.OriginalRef)
		if err != nil {
			return ResolvedSource{}, err
		}
		resolved.ResolvedSHA, resolved.CommitTime = commit.SHA, commit.time
	} else {
		if input.Selection.includesController() {
			controller, err := preparer.resolveComponentRelease(
				ctx, SelectionController, componentReleaseTag(SelectionController, input.OriginalRef), input.Platform,
			)
			if err != nil {
				return ResolvedSource{}, err
			}
			resolved.ControllerRelease = &controller
		}
		if input.Selection.includesAgent() {
			agent, err := preparer.resolveComponentRelease(
				ctx, SelectionAgent, componentReleaseTag(SelectionAgent, input.OriginalRef), input.Platform,
			)
			if err != nil {
				return ResolvedSource{}, err
			}
			resolved.AgentRelease = &agent
		}
	}
	return resolved, resolved.validate()
}

func componentReleaseTag(component Selection, requested string) string {
	if strings.Contains(requested, "/") {
		return requested
	}
	return string(component) + "/" + requested
}

type resolvedCommit struct {
	SHA  string
	time time.Time
}

func (preparer *Preparer) resolveCommit(ctx context.Context, reference string) (resolvedCommit, error) {
	endpoint := "https://api.github.com/repos/" + githubRepository + "/commits/" + url.PathEscape(reference)
	var document githubCommit
	if err := preparer.readGitHubJSON(ctx, endpoint, &document); err != nil {
		return resolvedCommit{}, err
	}
	timestamp, err := time.Parse(time.RFC3339, document.Commit.Committer.Date)
	if err != nil || !commitPattern.MatchString(document.SHA) || timestamp.Before(time.Unix(0, 0)) ||
		timestamp.Nanosecond() != 0 {
		return resolvedCommit{}, errs.New(errs.KindStateConflict, "resolved GitHub commit metadata is invalid")
	}
	return resolvedCommit{SHA: document.SHA, time: timestamp.UTC()}, nil
}

func (preparer *Preparer) resolveRelease(
	ctx context.Context,
	selection Selection,
	tag string,
	platform Platform,
) ([]ReleaseAsset, error) {
	endpoint := "https://api.github.com/repos/" + githubRepository + "/releases/tags/" + url.PathEscape(tag)
	var release githubRelease
	if err := preparer.readGitHubJSON(ctx, endpoint, &release); err != nil {
		return nil, err
	}
	if release.TagName != tag || release.Draft || release.Prerelease {
		return nil, errs.New(errs.KindStateConflict, "selected GitHub release is not published and stable")
	}
	required := requiredReleaseAssets(selection, tag, platform)
	resolved := make([]ReleaseAsset, 0, len(required))
	for name, maximum := range required {
		var selected *ReleaseAsset
		for _, asset := range release.Assets {
			if asset.Name != name {
				continue
			}
			if selected != nil {
				return nil, errs.New(errs.KindStateConflict, "selected GitHub release repeats a required asset")
			}
			copy := ReleaseAsset{Name: asset.Name, Size: asset.Size, SHA256: asset.Digest}
			selected = &copy
		}
		if selected == nil || selected.Size <= 0 || selected.Size > maximum ||
			!digestPattern.MatchString(selected.SHA256) {
			return nil, errs.Newf(errs.KindStateConflict, "selected GitHub release asset %q is invalid", name)
		}
		resolved = append(resolved, *selected)
	}
	sort.Slice(resolved, func(left, right int) bool { return resolved[left].Name < resolved[right].Name })
	return resolved, nil
}

func (preparer *Preparer) resolveComponentRelease(
	ctx context.Context,
	selection Selection,
	tag string,
	platform Platform,
) (ResolvedRelease, error) {
	commit, err := preparer.resolveCommit(ctx, tag)
	if err != nil {
		return ResolvedRelease{}, err
	}
	assets, err := preparer.resolveRelease(ctx, selection, tag, platform)
	if err != nil {
		return ResolvedRelease{}, err
	}
	return ResolvedRelease{
		OriginalRef: tag, ResolvedSHA: commit.SHA, CommitTime: commit.time, Assets: assets,
	}, nil
}

func requiredReleaseAssets(selection Selection, tag string, platform Platform) map[string]int64 {
	version := releaseVersion(tag)
	required := make(map[string]int64, 3)
	if selection.includesAgent() {
		required["agent.json"] = 64 << 10
	}
	if selection.includesController() {
		name := fmt.Sprintf("groundplane-%s-linux-%s.tar.gz", version, platform.Architecture)
		required[name] = maximumBundleBytes
		required[name+".sha256"] = 1024
	}
	return required
}

func (preparer *Preparer) readGitHubJSON(ctx context.Context, endpoint string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "groundplane-software-preparation")
	response, err := preparer.http.Do(request)
	if err != nil {
		return errs.Wrap(errs.KindRequestFailed, err)
	}
	defer response.Body.Close()
	if response.Request.URL.Scheme != "https" || response.Request.URL.Hostname() != "api.github.com" {
		return errs.New(errs.KindStateConflict, "fixed GitHub metadata request left its endpoint")
	}
	if response.StatusCode != http.StatusOK {
		return errs.Newf(errs.KindStateConflict, "fixed GitHub metadata returned HTTP %d", response.StatusCode)
	}
	value, err := io.ReadAll(io.LimitReader(response.Body, maximumGitHubJSONBytes+1))
	if err != nil || len(value) == 0 || len(value) > maximumGitHubJSONBytes {
		return errs.New(errs.KindStateConflict, "fixed GitHub metadata is unreadable or oversized")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	if err := decoder.Decode(target); err != nil {
		return errs.New(errs.KindStateConflict, "fixed GitHub metadata is invalid")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errs.New(errs.KindStateConflict, "fixed GitHub metadata contains trailing input")
	}
	return nil
}
