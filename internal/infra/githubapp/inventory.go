package githubapp

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const maxWorkflowBytes = 64 << 10

var workflowPathPattern = regexp.MustCompile(`^\.github/workflows/[A-Za-z0-9_][A-Za-z0-9_.-]{0,119}\.ya?ml$`)

type Workflow struct {
	ID     int64  `json:"id"`
	Path   string `json:"path"`
	State  string `json:"state"`
	Digest string `json:"-"`
}

type WorkflowSummary struct {
	ID    int64  `json:"id"`
	Name  string `json:"name"`
	Path  string `json:"path"`
	State string `json:"state"`
}

// Workflows lists one page for the pinned repository using Actions read access.
func (client *Client) Workflows(ctx context.Context, scope Scope, page, limit int) ([]WorkflowSummary, bool, error) {
	if page < 1 || page > 10000 || limit < 1 || limit > pageSize {
		return nil, false, errs.New(errs.KindValidationFailed, "GitHub workflow page is invalid")
	}
	access, err := client.scopedInstallation(ctx, scope, installationPermissions{Actions: "read", Metadata: "read"})
	if err != nil {
		return nil, false, err
	}
	var result struct {
		Total     int               `json:"total_count"`
		Workflows []WorkflowSummary `json:"workflows"`
	}
	path := access.path() + "/actions/workflows?per_page=" + id(int64(limit)) + "&page=" + id(int64(page))
	if err := client.requestJSON(ctx, http.MethodGet, path, access.token, nil, http.StatusOK, &result); err != nil {
		return nil, false, err
	}
	if !validPage(result.Total, len(result.Workflows), page, limit) {
		return nil, false, errs.New(errs.KindStateConflict, "GitHub returned an incomplete workflow page")
	}
	workflows := make([]WorkflowSummary, 0, len(result.Workflows))
	seen := make(map[int64]struct{}, len(result.Workflows))
	for _, workflow := range result.Workflows {
		if workflow.ID <= 0 || !boundedText(workflow.Name, 1024) || !boundedText(workflow.State, 100) {
			return nil, false, errs.New(errs.KindStateConflict, "GitHub returned invalid workflow metadata")
		}
		if _, exists := seen[workflow.ID]; exists {
			return nil, false, errs.New(errs.KindStateConflict, "GitHub returned duplicate workflow IDs")
		}
		seen[workflow.ID] = struct{}{}
		if workflowPathPattern.MatchString(workflow.Path) {
			workflows = append(workflows, workflow)
		}
	}
	return workflows, page*limit < result.Total, nil
}

// InspectWorkflow identifies the active Actions workflow and hashes its source
// at the commit supplied by the Controller's approval policy.
func (client *Client) InspectWorkflow(
	ctx context.Context,
	scope Scope,
	workflowID int64,
	commit string,
) (Workflow, error) {
	if workflowID <= 0 || !commitPattern.MatchString(commit) {
		return Workflow{}, errs.New(errs.KindValidationFailed, "GitHub workflow ID and commit are required")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return Workflow{}, err
	}
	var workflow Workflow
	if err := client.requestJSON(ctx, http.MethodGet, access.path()+"/actions/workflows/"+id(workflowID), access.token,
		nil, http.StatusOK, &workflow); err != nil {
		return Workflow{}, err
	}
	if workflow.ID != workflowID || workflow.State != "active" || !workflowPathPattern.MatchString(workflow.Path) {
		return Workflow{}, errs.New(errs.KindStateConflict, "GitHub workflow is inactive or has a different identity")
	}
	var file struct {
		Type     string `json:"type"`
		Path     string `json:"path"`
		Size     int    `json:"size"`
		Encoding string `json:"encoding"`
		Content  string `json:"content"`
	}
	if err := client.requestJSON(ctx, http.MethodGet, access.path()+"/contents/"+workflow.Path+"?ref="+commit, access.token,
		nil, http.StatusOK, &file); err != nil {
		return Workflow{}, err
	}
	if file.Type != "file" || file.Path != workflow.Path || file.Encoding != "base64" || file.Size <= 0 ||
		file.Size > maxWorkflowBytes ||
		len(file.Content) > 2*maxWorkflowBytes {
		return Workflow{}, errs.New(errs.KindStateConflict, "GitHub returned invalid workflow source metadata")
	}
	content, err := base64.StdEncoding.Strict().DecodeString(file.Content)
	if err != nil || len(content) != file.Size {
		return Workflow{}, errs.New(errs.KindStateConflict, "GitHub returned invalid workflow source")
	}
	digest := sha256.Sum256(content)
	workflow.Digest = "sha256:" + hex.EncodeToString(digest[:])
	return workflow, nil
}

type Runner struct {
	ID        int64
	Name      string
	Status    string
	Busy      bool
	Labels    []string
	GroupID   int64
	GroupName string
}

// RepositoryRunners finds self-hosted runners with the exact configured name
// using a separate repository-scoped Administration read token.
func (client *Client) RepositoryRunners(ctx context.Context, scope Scope, name string) ([]Runner, error) {
	if !boundedText(name, 256) {
		return nil, errs.New(errs.KindValidationFailed, "GitHub runner name is required")
	}
	access, err := client.scopedInstallation(
		ctx,
		scope,
		installationPermissions{Administration: "read", Metadata: "read"},
	)
	if err != nil {
		return nil, err
	}
	return client.listRunners(ctx, access.token, access.path()+"/actions/runners", name)
}

type runnerGroup struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// OrganizationRunners finds runners within a group visible to the pinned
// repository. GitHub applies its workflow restrictions during scheduling.
func (client *Client) OrganizationRunners(
	ctx context.Context,
	scope Scope,
	groupName, runnerName string,
) ([]Runner, error) {
	if !boundedText(groupName, 256) || !boundedText(runnerName, 256) {
		return nil, errs.New(errs.KindValidationFailed, "GitHub runner group and name are required")
	}
	access, err := client.scopedInstallation(
		ctx,
		scope,
		installationPermissions{Metadata: "read", OrganizationSelfHostedRunners: "read"},
	)
	if err != nil {
		return nil, err
	}
	path := "/orgs/" + access.repo.Owner + "/actions/runner-groups"
	group, err := client.visibleRunnerGroup(ctx, access, path, groupName)
	if err != nil || group.ID == 0 {
		return nil, err
	}
	runners, err := client.listRunners(ctx, access.token, path+"/"+id(group.ID)+"/runners", runnerName)
	if err != nil {
		return nil, err
	}
	for index := range runners {
		runners[index].GroupID = group.ID
		runners[index].GroupName = group.Name
	}
	return runners, nil
}

func (client *Client) visibleRunnerGroup(
	ctx context.Context,
	access installation,
	path, name string,
) (runnerGroup, error) {
	query := url.Values{
		"visible_to_repository": {access.repo.Owner + "/" + access.repo.Name},
		"per_page":              {id(pageSize)},
	}
	var match runnerGroup
	for page := 1; page <= maxPages; page++ {
		query.Set("page", id(int64(page)))
		var result struct {
			Total  int           `json:"total_count"`
			Groups []runnerGroup `json:"runner_groups"`
		}
		if err := client.requestJSON(ctx, http.MethodGet, path+"?"+query.Encode(), access.token, nil,
			http.StatusOK, &result); err != nil {
			return runnerGroup{}, err
		}
		if result.Total > pageSize*maxPages || !validPage(result.Total, len(result.Groups), page, pageSize) {
			return runnerGroup{}, errs.New(errs.KindStateConflict, "GitHub runner group list is incomplete")
		}
		for _, group := range result.Groups {
			if !strings.EqualFold(group.Name, name) {
				continue
			}
			if group.ID <= 0 || match.ID != 0 {
				return runnerGroup{}, errs.New(errs.KindStateConflict, "GitHub runner group identity is ambiguous")
			}
			match = group
		}
		if page*pageSize >= result.Total {
			return match, nil
		}
	}
	return runnerGroup{}, errs.New(errs.KindStateConflict, "GitHub runner group list exceeds the supported bound")
}

func (client *Client) listRunners(ctx context.Context, token, path, name string) ([]Runner, error) {
	query := url.Values{"name": {name}, "per_page": {id(pageSize)}}
	seen := make(map[int64]struct{})
	runners := []Runner{}
	for page := 1; page <= maxPages; page++ {
		query.Set("page", id(int64(page)))
		var result struct {
			Total   int `json:"total_count"`
			Runners []struct {
				ID     int64  `json:"id"`
				Name   string `json:"name"`
				Status string `json:"status"`
				Busy   bool   `json:"busy"`
				Labels []struct {
					Name string `json:"name"`
				} `json:"labels"`
			} `json:"runners"`
		}
		if err := client.requestJSON(ctx, http.MethodGet, path+"?"+query.Encode(), token, nil,
			http.StatusOK, &result); err != nil {
			return nil, err
		}
		if result.Total > pageSize*maxPages || !validPage(result.Total, len(result.Runners), page, pageSize) {
			return nil, errs.New(errs.KindStateConflict, "GitHub repository runner list is incomplete")
		}
		for _, runner := range result.Runners {
			if runner.ID <= 0 || (runner.Status != "online" && runner.Status != "offline") {
				return nil, errs.New(errs.KindStateConflict, "GitHub returned invalid runner metadata")
			}
			if _, duplicate := seen[runner.ID]; duplicate {
				return nil, errs.New(errs.KindStateConflict, "GitHub returned duplicate runner IDs")
			}
			seen[runner.ID] = struct{}{}
			if runner.Name != name {
				continue
			}
			labels := make([]string, 0, len(runner.Labels))
			for _, label := range runner.Labels {
				labels = append(labels, label.Name)
			}
			runners = append(
				runners,
				Runner{ID: runner.ID, Name: runner.Name, Status: runner.Status, Busy: runner.Busy, Labels: labels},
			)
		}
		if page*pageSize >= result.Total {
			return runners, nil
		}
	}
	return nil, errs.New(errs.KindStateConflict, "GitHub repository runner list exceeds the supported bound")
}
