package channelops

import "time"

const (
	QueueAgentTick            = "agent_tick"
	QueuePlanTask             = "plan_task"
	QueueExecuteTask          = "execute_task"
	QueueObserveJob           = "observe_job"
	QueuePublishTask          = "publish_task"
	QueuePromotePublication   = "promote_publication"
	QueueReconcilePublication = "reconcile_publication"
	QueueCollectMetrics       = "collect_metrics"
	QueueAccountHealth        = "account_health"
	QueueSendAlert            = "send_alert"
	QueueCleanupExpired       = "cleanup_expired"
	QueueLearningRecompute    = "learning_recompute"
	QueueIngestDiscovery      = "ingest_discovery"

	QueueStatusQueued       = "queued"
	QueueStatusRunning      = "running"
	QueueStatusSucceeded    = "succeeded"
	QueueStatusFailed       = "failed"
	QueueStatusDeadLettered = "dead_lettered"

	TaskSelected        = "selected"
	TaskPlanning        = "planning"
	TaskProducing       = "producing"
	TaskScheduled       = "scheduled"
	TaskUploadedPrivate = "uploaded_private"
	TaskMeasured        = "measured"
	TaskHeld            = "held"
	TaskFailed          = "failed"
	TaskRejected        = "rejected"

	ApprovalAgent = "agent"
	ApprovalHuman = "human"

	SourceManualSeed = "manual_seed"
	SourceLaneSeed   = "lane_seed"
	SourceTrendYT    = "trend_youtube"

	FailureAuth          = "auth"
	FailureQuota         = "quota"
	FailureUpload        = "upload"
	FailureRender        = "render"
	FailurePlanning      = "planning"
	FailureValidation    = "validation"
	FailurePDS           = "pds"
	FailureYouTubeStatus = "youtube_status"
	FailureMetrics       = "metrics"
	FailureDiscovery     = "discovery"
	FailureLearning      = "learning"
	FailureOther         = "other"

	PromotionReserved   = "reserved"
	PromotionSubmitting = "submitting"
	PromotionConfirmed  = "confirmed"
	PromotionFinalized  = "finalized"
	PromotionUncertain  = "uncertain"

	MetricSchedulePending   = "pending"
	MetricScheduleSucceeded = "succeeded"
	MetricScheduleExpired   = "expired"
	MetricErrorUnavailable  = "metrics_unavailable"
)

type ChannelProfileRow struct {
	ID                   string
	Enabled              bool
	DryRun               bool
	HaltedAt             *time.Time
	IntakePausedAt       *time.Time
	TickIntervalMinutes  int
	ConfigVersion        int
	RiskPolicyJSON       map[string]any
	CadencePolicyJSON    map[string]any
	ContentMixPolicyJSON map[string]any
	DefaultAspectRatio   string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func queueKindRequiresOpenIntake(kind string) bool {
	return kind == QueueAgentTick || kind == QueueIngestDiscovery
}

type TopicLaneRow struct {
	ID                   string
	ChannelProfileID     string
	Name                 string
	Description          string
	KeywordsJSON         []string
	Enabled              bool
	PausedUntil          *time.Time
	Weight               float64
	MaxPostsPerDay       int
	CooldownAfterPostMin int
	MaxConsecutiveStreak int
	CreatedAt            time.Time
}

type LaneFormatRow struct {
	ID                       string
	TopicLaneID              string
	FormatKey                string
	Enabled                  bool
	Weight                   float64
	TargetDurationSec        int
	DefaultPublishVisibility string
	TemplatePoolJSON         []string
	SourcePlatformsJSON      []string
	CreatedAt                time.Time
}

type PublishingAccountRow struct {
	ID                  string
	ChannelProfileID    string
	Platform            string
	AccountLabel        string
	PlatformAccountID   string
	Enabled             bool
	PausedUntil         *time.Time
	DefaultPrivacy      string
	ExternalAutoPublish bool
	CreatedAt           time.Time
}

type ManualSeedRow struct {
	ID                     string
	ChannelProfileID       string
	TopicLaneID            *string
	TargetAccountID        *string
	Prompt                 string
	TitleSeed              string
	SourcePolicy           string
	SourcePlatformsJSON    []string
	MaterialLibraryIDsJSON []string
	ConstraintsJSON        map[string]any
	Status                 string
	CreatedAt              time.Time
}

type DiscoverySignalRow struct {
	ID               string
	ChannelProfileID string
	TopicLaneID      *string
	Source           string
	SourceURL        *string
	SourceExternalID string
	Title            string
	Summary          string
	KeywordsJSON     []string
	TrendScore       float64
	NoveltyScore     float64
	RawJSON          map[string]any
	Status           string
	ExpiresAt        *time.Time
	ObservedAt       time.Time
	CreatedAt        time.Time
}

type ProductionTaskRow struct {
	ID                           string
	ChannelProfileID             string
	TopicLaneID                  *string
	LaneFormatID                 *string
	TargetAccountID              string
	ManualSeedID                 *string
	DiscoverySignalID            *string
	Source                       string
	TitleSeed                    string
	Prompt                       string
	RationaleJSON                map[string]any
	ScoreBreakdownJSON           map[string]any
	SourcePlatformsJSON          []string
	MaterialLibraryIDsJSON       []string
	UsesExternalAssets           bool
	ApprovalMode                 string
	HumanReviewEvidenceJSON      map[string]any
	AutoFlowPlanID               *string
	AutoFlowApprovedRevisionHash *string
	AutoFlowApprovedRevision     *int64
	AutoFlowRunID                *string
	JobID                        *string
	State                        string
	BlockedByGuard               *string
	FailureReason                *string
	FailureCategory              *string
	TransitionHistoryJSON        []map[string]any
	ChannelConfigVersionSnapshot int
	ChannelConfigSnapshotJSON    map[string]any
	StateUpdatedAt               time.Time
}

type QueueItemRow struct {
	ID                string
	Kind              string
	IdempotencyKey    string
	PayloadJSON       map[string]any
	Status            string
	Priority          int
	AttemptCount      int
	MaxAttempts       int
	RunAfter          time.Time
	LockedAt          *time.Time
	LockedBy          *string
	LastError         *string
	DeadLetterAt      *time.Time
	ChannelProfileID  *string
	ParentQueueItemID *string
}

type PublicationRow struct {
	ID                    string
	ProductionTaskID      string
	Platform              string
	AccountID             string
	PlatformContentID     string
	Permalink             *string
	Title                 string
	Description           string
	DesiredPrivacy        string
	CurrentPrivacy        string
	PublishStatus         string
	UploadedAt            *time.Time
	ScheduledPublishAt    *time.Time
	PublicAt              *time.Time
	ComplianceDisposition string
	QuotaUnitsEstimated   int
	LastMetricsPolledAt   *time.Time
	WarningsJSON          []any
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

type MetricScheduleRow struct {
	ID                  string
	PublicationID       string
	SnapshotStage       string
	EffectiveStartAt    time.Time
	DueAt               time.Time
	GraceUntil          time.Time
	Status              string
	AttemptCount        int
	LastAttemptAt       *time.Time
	CompletedAt         *time.Time
	AvailableFieldsJSON []string
	LastErrorCode       *string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

type PromotionOperationRow struct {
	ID                    string
	PublicationID         string
	ProductionTaskID      string
	QueueItemID           string
	PlatformVideoID       string
	TargetPrivacy         string
	ScheduledAt           time.Time
	AttemptKey            string
	Status                string
	Decision              PDSDecision
	ObservedPrivacy       *string
	ObservedPublishStatus *string
	EvidenceJSON          map[string]any
	ErrorMessage          *string
	RequestAttemptedAt    *time.Time
	ConfirmedAt           *time.Time
	CompletedAt           *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}
