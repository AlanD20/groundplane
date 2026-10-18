package api

type DNSCheckQuery struct {
	Domain string `json:"domain" required:"true" minLength:"1" maxLength:"254"`
	Name   string `json:"name" required:"true" minLength:"1" maxLength:"254"`
	Type   string `json:"type" required:"true" enum:"A,NS"`
}

type DNSCheckRequest struct {
	Revision int64           `json:"revision" required:"true" minimum:"1"`
	Queries  []DNSCheckQuery `json:"queries,omitempty" maxItems:"10"`
}

type DNSCheckConfiguration struct {
	ComponentID       string          `json:"component_id"`
	ComponentRevision int64           `json:"component_revision"`
	HostID            string          `json:"host_id"`
	AppliedRevision   int64           `json:"applied_revision"`
	AgentID           string          `json:"agent_id"`
	AgentGeneration   uint64          `json:"agent_generation"`
	ServiceID         string          `json:"service_id"`
	ArtifactID        string          `json:"artifact_id"`
	ArtifactSHA256    string          `json:"artifact_sha256"`
	ImageReference    string          `json:"image_reference"`
	RenderGeneration  uint64          `json:"render_generation"`
	Queries           []DNSCheckQuery `json:"queries"`
}

type DNSCheckAnswerSample struct {
	Name  string `json:"name"`
	Type  string `json:"type" enum:"A,NS"`
	Value string `json:"value"`
}

type DNSCheckAnswer struct {
	Domain              string                 `json:"domain"`
	Name                string                 `json:"name"`
	Type                string                 `json:"type" enum:"A,NS"`
	Server              string                 `json:"server"`
	ThroughResolver     bool                   `json:"through_resolver"`
	Outcome             string                 `json:"outcome" enum:"resolved,name_not_found,no_answer,refused,server_error,timeout,query_failed,canceled"`
	Rcode               *uint32                `json:"rcode,omitempty"`
	UsedTCP             bool                   `json:"used_tcp"`
	Answers             []DNSCheckAnswerSample `json:"answers"`
	TotalAnswers        uint32                 `json:"total_answers"`
	ElapsedMilliseconds uint32                 `json:"elapsed_ms"`
}

type DNSCheckResult struct {
	TaskID            string                `json:"task_id"`
	ExecutionLocation string                `json:"execution_location"`
	Configuration     DNSCheckConfiguration `json:"configuration"`
	Stale             bool                  `json:"stale"`
	Phase             string                `json:"phase" enum:"ready,checking,finished"`
	Outcome           string                `json:"outcome,omitempty" enum:"passed,failed,interrupted"`
	Failure           string                `json:"failure,omitempty"`
	Answers           []DNSCheckAnswer      `json:"answers"`
	CreatedAt         string                `json:"created_at"`
	StartedAt         string                `json:"started_at,omitempty"`
	FinishedAt        string                `json:"finished_at,omitempty"`
	ObservedAt        string                `json:"observed_at,omitempty"`
}
