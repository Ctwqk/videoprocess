package contracts

type JobStatus string

const (
	JobStatusPending         JobStatus = "PENDING"
	JobStatusValidating      JobStatus = "VALIDATING"
	JobStatusPlanning        JobStatus = "PLANNING"
	JobStatusRunning         JobStatus = "RUNNING"
	JobStatusSucceeded       JobStatus = "SUCCEEDED"
	JobStatusFailed          JobStatus = "FAILED"
	JobStatusCancelled       JobStatus = "CANCELLED"
	JobStatusPartiallyFailed JobStatus = "PARTIALLY_FAILED"
	JobStatusWaitingWindow   JobStatus = "WAITING_WINDOW"
)

type NodeStatus string

const (
	NodeStatusPending   NodeStatus = "PENDING"
	NodeStatusQueued    NodeStatus = "QUEUED"
	NodeStatusRunning   NodeStatus = "RUNNING"
	NodeStatusSucceeded NodeStatus = "SUCCEEDED"
	NodeStatusFailed    NodeStatus = "FAILED"
	NodeStatusSkipped   NodeStatus = "SKIPPED"
	NodeStatusCancelled NodeStatus = "CANCELLED"
)
