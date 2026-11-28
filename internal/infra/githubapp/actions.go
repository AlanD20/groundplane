package githubapp

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	pageSize     = 100
	maxPages     = 10
	markerPrefix = "Groundplane build "
)

var (
	commitPattern  = regexp.MustCompile(`^[a-f0-9]{40}$`)
	digestPattern  = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	requestPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
	refPattern     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]{0,254}$`)
)

type DispatchInput struct {
	WorkflowID      int64
	WorkflowRef     string
	RequestID       string
	Commit          string
	RunnerLabel     string
	RunnerGroup     string
	Dockerfile      string
	BuildContext    string
	ImageRepository string
}

type dispatchRequest struct {
	Ref    string         `json:"ref"`
	Inputs dispatchInputs `json:"inputs"`
}

type dispatchInputs struct {
	RequestID       string `json:"request_id"`
	Commit          string `json:"source_commit"`
	RunnerLabel     string `json:"runner_label"`
	RunnerGroup     string `json:"runner_group"`
	Dockerfile      string `json:"dockerfile"`
	BuildContext    string `json:"build_context"`
	ImageRepository string `json:"image_repository"`
}

type Run struct {
	ID           int64
	WorkflowID   int64
	Attempt      int64
	Status       string
	Conclusion   string
	HeadSHA      string
	Event        string
	DisplayTitle string
	URL          string
}

type Job struct {
	ID              int64
	RunID           int64
	Attempt         int64
	RunnerID        int64
	RunnerGroupID   int64
	Name            string
	Status          string
	Conclusion      string
	HeadSHA         string
	RunnerName      string
	RunnerGroupName string
	Labels          []string
}

type runRecord struct {
	ID           int64  `json:"id"`
	WorkflowID   int64  `json:"workflow_id"`
	Attempt      int64  `json:"run_attempt"`
	Status       string `json:"status"`
	Conclusion   string `json:"conclusion"`
	HeadSHA      string `json:"head_sha"`
	Event        string `json:"event"`
	DisplayTitle string `json:"display_title"`
	Repository   struct {
		ID int64 `json:"id"`
	} `json:"repository"`
}

type jobRecord struct {
	ID              int64    `json:"id"`
	RunID           int64    `json:"run_id"`
	Attempt         int64    `json:"run_attempt"`
	RunnerID        int64    `json:"runner_id"`
	RunnerGroupID   int64    `json:"runner_group_id"`
	Name            string   `json:"name"`
	Status          string   `json:"status"`
	Conclusion      string   `json:"conclusion"`
	HeadSHA         string   `json:"head_sha"`
	RunnerName      string   `json:"runner_name"`
	RunnerGroupName string   `json:"runner_group_name"`
	Labels          []string `json:"labels"`
}

func (client *Client) ResolveBranch(ctx context.Context, scope Scope, branch string) (string, error) {
	if !validRef(branch) {
		return "", errs.New(errs.KindValidationFailed, "Select a valid GitHub branch name")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return "", err
	}
	var result struct {
		Ref    string `json:"ref"`
		Object struct {
			Type string `json:"type"`
			SHA  string `json:"sha"`
		} `json:"object"`
	}
	if err := client.requestJSON(ctx, http.MethodGet, access.path()+"/git/ref/heads/"+branch,
		access.token, nil, http.StatusOK, &result); err != nil {
		return "", err
	}
	if result.Ref != "refs/heads/"+branch || result.Object.Type != "commit" ||
		!commitPattern.MatchString(result.Object.SHA) {
		return "", errs.New(errs.KindStateConflict, "GitHub returned a different branch or invalid commit")
	}
	return result.Object.SHA, nil
}

// Dispatch submits the approved workflow once. A transport failure or response
// without a run ID must be reconciled by the Controller through FindDispatch.
func (client *Client) Dispatch(ctx context.Context, scope Scope, input DispatchInput) (Run, error) {
	if !validDispatch(input) {
		return Run{}, errs.New(errs.KindValidationFailed, "GitHub build dispatch input is invalid")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return Run{}, err
	}
	if access.repo.Archived || access.repo.Disabled {
		return Run{}, errs.New(errs.KindStateConflict, "GitHub repository is archived or disabled")
	}
	request, err := json.Marshal(dispatchRequest{
		Ref: input.WorkflowRef,
		Inputs: dispatchInputs{RequestID: input.RequestID, Commit: input.Commit, RunnerLabel: input.RunnerLabel,
			RunnerGroup: input.RunnerGroup, Dockerfile: input.Dockerfile, BuildContext: input.BuildContext,
			ImageRepository: input.ImageRepository},
	})
	if err != nil {
		return Run{}, errs.Wrap(errs.KindInternal, err)
	}
	var result struct {
		RunID int64 `json:"workflow_run_id"`
	}
	if err := client.requestJSON(ctx, http.MethodPost, access.path()+"/actions/workflows/"+id(input.WorkflowID)+"/dispatches",
		access.token, request, http.StatusOK, &result); err != nil {
		return Run{}, err
	}
	if result.RunID <= 0 {
		return Run{}, errs.New(errs.KindStateConflict, "GitHub did not identify the dispatched run")
	}
	return Run{ID: result.RunID, WorkflowID: input.WorkflowID, URL: access.runURL(result.RunID)}, nil
}

func (client *Client) Run(ctx context.Context, scope Scope, runID int64) (Run, error) {
	if runID <= 0 {
		return Run{}, errs.New(errs.KindValidationFailed, "GitHub workflow run ID is required")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return Run{}, err
	}
	var result runRecord
	if err := client.requestJSON(ctx, http.MethodGet, access.path()+"/actions/runs/"+id(runID),
		access.token, nil, http.StatusOK, &result); err != nil {
		return Run{}, err
	}
	if result.ID != runID || !result.valid(scope.RepositoryID) {
		return Run{}, errs.New(errs.KindStateConflict, "GitHub workflow run identity differs from the selected run")
	}
	return result.run(access), nil
}

// FindDispatch matches the workflow's reviewed run-name against the unique
// Groundplane request ID. Duplicate matches or a saturated search are uncertain.
func (client *Client) FindDispatch(
	ctx context.Context,
	scope Scope,
	workflowID int64,
	requestID string,
	since time.Time,
) (Run, bool, error) {
	if workflowID <= 0 || !requestPattern.MatchString(requestID) || since.IsZero() {
		return Run{}, false, errs.New(errs.KindValidationFailed, "GitHub dispatch lookup identity is invalid")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return Run{}, false, err
	}
	path := access.path() + "/actions/workflows/" + id(workflowID) + "/runs"
	query := url.Values{
		"event":    {"workflow_dispatch"},
		"created":  {">=" + since.UTC().Format(time.RFC3339)},
		"per_page": {id(pageSize)},
	}
	marker := markerPrefix + requestID
	var match Run
	for page := int64(1); page <= maxPages; page++ {
		query.Set("page", id(page))
		var result struct {
			Total int         `json:"total_count"`
			Runs  []runRecord `json:"workflow_runs"`
		}
		if err := client.requestJSON(ctx, http.MethodGet, path+"?"+query.Encode(), access.token, nil,
			http.StatusOK, &result); err != nil {
			return Run{}, false, err
		}
		// GitHub filters this endpoint to at most 1,000 results. At the cap
		// another exact match could exist beyond the visible result set.
		if result.Total >= pageSize*maxPages || !validPage(result.Total, len(result.Runs), int(page), pageSize) {
			return Run{}, false, errs.New(errs.KindStateConflict, "GitHub dispatch lookup is not complete")
		}
		for _, item := range result.Runs {
			if item.DisplayTitle != marker {
				continue
			}
			if !item.valid(scope.RepositoryID) || item.WorkflowID != workflowID || item.Event != "workflow_dispatch" ||
				(match.ID != 0 && match.ID != item.ID) {
				return Run{}, false, errs.New(errs.KindStateConflict, "GitHub returned an ambiguous dispatch match")
			}
			match = item.run(access)
		}
		if int(page)*pageSize >= result.Total {
			return match, match.ID != 0, nil
		}
	}
	return Run{}, false, errs.New(errs.KindStateConflict, "GitHub dispatch lookup is not complete")
}

func (client *Client) Jobs(ctx context.Context, scope Scope, runID, attempt int64) ([]Job, error) {
	if runID <= 0 || attempt <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "GitHub workflow run and attempt IDs are required")
	}
	access, err := client.installation(ctx, scope)
	if err != nil {
		return nil, err
	}
	path := access.path() + "/actions/runs/" + id(runID) + "/attempts/" + id(attempt) + "/jobs?per_page=" + id(pageSize)
	jobs := []Job{}
	for page := int64(1); page <= maxPages; page++ {
		var result struct {
			Total int         `json:"total_count"`
			Jobs  []jobRecord `json:"jobs"`
		}
		if err := client.requestJSON(ctx, http.MethodGet, path+"&page="+id(page), access.token, nil,
			http.StatusOK, &result); err != nil {
			return nil, err
		}
		if result.Total > pageSize*maxPages || !validPage(result.Total, len(result.Jobs), int(page), pageSize) {
			return nil, errs.New(errs.KindStateConflict, "GitHub workflow job list exceeds the supported bound")
		}
		for _, job := range result.Jobs {
			if job.ID <= 0 || job.RunID != runID || (job.Attempt != 0 && job.Attempt != attempt) ||
				!commitPattern.MatchString(job.HeadSHA) {
				return nil, errs.New(errs.KindStateConflict, "GitHub returned a job for a different workflow attempt")
			}
			jobs = append(jobs, Job{ID: job.ID, RunID: job.RunID, Attempt: attempt, RunnerID: job.RunnerID,
				RunnerGroupID: job.RunnerGroupID, Name: job.Name, Status: job.Status, Conclusion: job.Conclusion,
				HeadSHA: job.HeadSHA, RunnerName: job.RunnerName, RunnerGroupName: job.RunnerGroupName, Labels: job.Labels})
		}
		if int(page)*pageSize >= result.Total {
			return jobs, nil
		}
	}
	return nil, errs.New(errs.KindStateConflict, "GitHub workflow job list exceeds the supported bound")
}

func (record runRecord) valid(repoID int64) bool {
	return record.ID > 0 && record.WorkflowID > 0 && record.Attempt > 0 && record.Repository.ID == repoID &&
		commitPattern.MatchString(record.HeadSHA)
}

func (record runRecord) run(access installation) Run {
	return Run{ID: record.ID, WorkflowID: record.WorkflowID, Attempt: record.Attempt,
		Status: record.Status, Conclusion: record.Conclusion, HeadSHA: record.HeadSHA,
		Event: record.Event, DisplayTitle: record.DisplayTitle, URL: access.runURL(record.ID)}
}

func (access installation) runURL(runID int64) string {
	return "https://github.com/" + access.repo.Owner + "/" + access.repo.Name + "/actions/runs/" + id(runID)
}

func validDispatch(input DispatchInput) bool {
	return input.WorkflowID > 0 && validRef(input.WorkflowRef) && requestPattern.MatchString(input.RequestID) &&
		commitPattern.MatchString(input.Commit) && boundedText(input.RunnerLabel, 100) &&
		(input.RunnerGroup == "" || boundedText(input.RunnerGroup, 100)) &&
		input.Dockerfile != "." && input.Dockerfile != "./" && validBuildPath(input.Dockerfile) && validBuildPath(input.BuildContext) &&
		boundedText(input.ImageRepository, 512) && !strings.ContainsAny(input.ImageRepository, " @\\")
}

func validBuildPath(path string) bool {
	return path == "./" ||
		(fs.ValidPath(strings.TrimPrefix(path, "./")) && boundedText(path, 512) && !strings.Contains(path, "\\"))
}

func validPage(total, count, page, limit int) bool {
	return total >= 0 && count == min(limit, max(0, total-(page-1)*limit))
}

func validRef(ref string) bool {
	if !refPattern.MatchString(ref) || strings.Contains(ref, "..") || strings.HasSuffix(ref, ".") ||
		strings.HasPrefix(ref, "refs/") {
		return false
	}
	for part := range strings.SplitSeq(ref, "/") {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	return true
}

func boundedText(text string, maximum int) bool {
	return len(text) > 0 && len(text) <= maximum && strings.TrimSpace(text) == text &&
		strings.IndexFunc(text, func(char rune) bool {
			return char < ' ' || char == 127
		}) == -1
}
