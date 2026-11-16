package handlers

import apiTypes "github.com/AlanD20/groundplane/pkg/api"

func taskRunAfter(input *apiTypes.TaskRunAfterRequest) apiTypes.TaskRunAfterRequest {
	if input == nil {
		return apiTypes.TaskRunAfterRequest{}
	}
	return apiTypes.TaskRunAfterRequest{After: append([]string(nil), input.After...)}
}
