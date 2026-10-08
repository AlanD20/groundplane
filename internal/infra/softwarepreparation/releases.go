package softwarepreparation

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type ReleaseChoice struct {
	Ref         string
	Selection   Selection
	Name        string
	PublishedAt time.Time
}

// ListReleases offers at most the newest 100 published repository releases.
// Selection still resolves and authenticates exact component assets at admission;
// this bounded discovery list is not artifact authority.
func (preparer *Preparer) ListReleases(ctx context.Context, selection Selection) ([]ReleaseChoice, error) {
	if !selection.valid() {
		return nil, errs.New(errs.KindValidationFailed, "software release selection is invalid")
	}
	var releases []struct {
		TagName     string `json:"tag_name"`
		Name        string `json:"name"`
		PublishedAt string `json:"published_at"`
		Draft       bool   `json:"draft"`
		Prerelease  bool   `json:"prerelease"`
	}
	if err := preparer.readGitHubJSON(ctx,
		"https://api.github.com/repos/"+githubRepository+"/releases?per_page=100", &releases); err != nil {
		return nil, err
	}
	choices := make([]ReleaseChoice, 0, len(releases))
	controllers := make(map[string]ReleaseChoice)
	agents := make(map[string]ReleaseChoice)
	for _, release := range releases {
		if release.Draft || release.Prerelease {
			continue
		}
		published, err := time.Parse(time.RFC3339, release.PublishedAt)
		if err != nil || published.Before(time.Unix(0, 0)) {
			return nil, errs.New(errs.KindStateConflict, "published software release timestamp is invalid")
		}
		component := SelectionController
		if strings.HasPrefix(release.TagName, "agent/") {
			component = SelectionAgent
		}
		if !validReleaseTag(component, release.TagName) {
			continue
		}
		choice := ReleaseChoice{Ref: release.TagName, Selection: component,
			Name: release.Name, PublishedAt: published.UTC()}
		if choice.Name == "" {
			choice.Name = choice.Ref
		}
		version := strings.TrimPrefix(release.TagName, string(component)+"/")
		if component == SelectionController {
			controllers[version] = choice
		} else {
			agents[version] = choice
		}
		if selection == component {
			choices = append(choices, choice)
		}
	}
	if selection == SelectionBoth {
		for version, controller := range controllers {
			agent, found := agents[version]
			if !found {
				continue
			}
			published := controller.PublishedAt
			if agent.PublishedAt.After(published) {
				published = agent.PublishedAt
			}
			choices = append(choices, ReleaseChoice{Ref: version, Selection: selection,
				Name: version, PublishedAt: published})
		}
	}
	sort.Slice(choices, func(left, right int) bool {
		if !choices[left].PublishedAt.Equal(choices[right].PublishedAt) {
			return choices[left].PublishedAt.After(choices[right].PublishedAt)
		}
		return choices[left].Ref < choices[right].Ref
	})
	return choices, nil
}
