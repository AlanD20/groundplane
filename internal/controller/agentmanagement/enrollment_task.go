package agentmanagement

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	agentTaskImageKey         = "image"
	agentTaskEnrollmentKey    = "enrollment_task_id"
	agentTaskPullIntervalKey  = "pull_interval_seconds"
	agentTaskMaxConcurrentKey = "max_concurrent_tasks"
	agentTaskLabelPrefix      = "label:"
)

func DecodeEnrollmentTask(task etcd.TaskRecord) (localagent.EnrollRequest, error) {
	request := localagent.EnrollRequest{
		AgentID: task.Target,
		Config:  localagent.Config{Labels: map[string]string{}},
	}
	required := map[string]bool{
		taskjournal.TaskResourceKindParam: false, agentTaskImageKey: false,
		agentTaskEnrollmentKey: false, agentTaskPullIntervalKey: false,
		agentTaskMaxConcurrentKey: false,
	}
	for key, value := range task.Params {
		switch key {
		case taskjournal.TaskResourceKindParam:
			if value != taskjournal.TaskResourceAgent {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			required[key] = true
		case agentTaskImageKey:
			request.Image = value
			required[key] = true
		case agentTaskEnrollmentKey:
			if ids.Validate(ids.KindTask, value) != nil ||
				(task.RetryOf == "" && value != task.ID) {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.EnrollmentTaskID = value
			required[key] = true
		case agentTaskPullIntervalKey:
			parsed, err := parseCanonicalPositiveInt32(value)
			if err != nil {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.PullIntervalSeconds = parsed
			required[key] = true
		case agentTaskMaxConcurrentKey:
			parsed, err := parseCanonicalPositiveInt32(value)
			if err != nil {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.MaxConcurrentTasks = parsed
			required[key] = true
		default:
			if !strings.HasPrefix(key, agentTaskLabelPrefix) {
				return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
			}
			request.Config.Labels[strings.TrimPrefix(key, agentTaskLabelPrefix)] = value
		}
	}
	for _, present := range required {
		if !present {
			return localagent.EnrollRequest{}, invalidAgentEnrollmentTask()
		}
	}
	return request, nil
}

func agentEnrollmentTaskParams(
	enrollmentTaskID string,
	image string,
	config localagent.Config,
) map[string]string {
	params := map[string]string{
		taskjournal.TaskResourceKindParam: taskjournal.TaskResourceAgent,
		agentTaskImageKey:                 image,
		agentTaskEnrollmentKey:            enrollmentTaskID,
		agentTaskPullIntervalKey:          strconv.FormatInt(int64(config.PullIntervalSeconds), 10),
		agentTaskMaxConcurrentKey:         strconv.FormatInt(int64(config.MaxConcurrentTasks), 10),
	}
	for key, value := range config.Labels {
		params[agentTaskLabelPrefix+key] = value
	}
	return params
}

func parseCanonicalPositiveInt32(value string) (int32, error) {
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 || strconv.FormatInt(parsed, 10) != value {
		return 0, errs.New(errs.KindValidationFailed, "positive canonical int32 is required")
	}
	return int32(parsed), nil
}

func invalidAgentEnrollmentTask() error {
	return errs.New(errs.KindValidationFailed, "Agent enrollment Task parameters are invalid")
}
