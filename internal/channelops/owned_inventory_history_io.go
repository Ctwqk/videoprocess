package channelops

import (
	"context"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type ownedHistoryDBReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// The other twenty projections are the frozen A1 complete-row columns. No
// catalogue lookup, account join, state filter, or secret-column SELECT is used.
var ownedHistoryAdditionalColumns = map[string]string{
	"owned_seed_inventories":           "client_request_id channel_profile_id topic_lane_id lane_format_id target_account_id platform_channel_id request_sha256 manifest_sha256 manifest_json privacy max_admissions minimum_interval_seconds starts_at expires_at state created_by approved_at approved_by approval_reference predecessor_inventory_id predecessor_closeout_sha256 succession_released_at revoked_at revoked_by hold_reason id created_at updated_at",
	"owned_seed_inventory_items":       "inventory_id platform_channel_id ordinal manual_seed_id asset_id content_sha256 byte_size storage_descriptor_json provenance_evidence_json provenance_sha256 seed_sha256 state production_task_id consumed_at completed_at hold_reason id",
	"publication_records":              "production_task_id platform account_id platform_content_id permalink title description tags_json thumbnail_storage_path desired_privacy current_privacy publish_status uploaded_at scheduled_publish_at public_at compliance_disposition quota_units_estimated last_metrics_polled_at warnings_json id created_at updated_at",
	"publication_metric_schedules":     "publication_id snapshot_stage effective_start_at due_at grace_until status attempt_count last_attempt_at completed_at available_fields_json last_error_code id created_at updated_at",
	"feedback_snapshots":               "publication_id snapshot_stage collected_at views likes comments shares avg_view_duration_sec retention_curve_json ctr impressions metrics_completeness_score available_fields_json reward_score reward_components_json virality_score raw_json id",
	"runtime_schedules":                "service_name state guarded_job_id updated_at updated_by",
	"publication_promotion_operations": "publication_id production_task_id queue_item_id platform_video_id target_privacy scheduled_at attempt_key status decision_json observed_privacy observed_publish_status evidence_json error_message request_attempted_at confirmed_at completed_at id created_at updated_at",
}

func ownedHistoryReadSQL() string {
	sets := make([]string, 0, 27)
	for _, table := range strings.Fields(historyTableNames) {
		columns := ownedHistoryAdditionalColumns[table]
		if columns == "" {
			columns = historyCompleteColumns[table][0]
		}
		key := "id"
		if table == "runtime_schedules" {
			key = "service_name"
		}
		sets = append(sets, "'"+table+"', (SELECT COALESCE(json_agg(row_to_json(bounded)), '[]'::json) FROM (SELECT "+strings.Join(strings.Fields(columns), ",")+" FROM public."+table+" ORDER BY "+key+" LIMIT "+strconv.Itoa(ownedHistoryMaxSnapshotRows+1)+") bounded)")
	}
	return "SELECT clock_timestamp(), json_build_object(" + strings.Join(sets, ",") + ")"
}

func loadOwnedHistorySnapshot(ctx context.Context, db ownedHistoryDBReader, platform string) (ownedHistorySnapshot, error) {
	var at time.Time
	var raw []byte
	if err := db.QueryRow(ctx, ownedHistoryReadSQL()).Scan(&at, &raw); err != nil {
		if ctx.Err() != nil {
			return ownedHistorySnapshot{}, ctx.Err()
		}
		return ownedHistorySnapshot{}, ownedHistoryError("owned_history_read_failed")
	}
	return newOwnedHistorySnapshot(raw, platform, at, []byte("[]"))
}

type ownedHistoryObservationRequest struct {
	sourcesJSON string
	digest      string
	observedAt  time.Time
}

func (*ownedHistoryObservationRequest) Error() string { return "owned_history_fresh_redis_required" }
func (r *ownedHistoryObservationRequest) sources() []ownedHistoryRedisObservation {
	values := historyArray(historyDecode([]byte(r.sourcesJSON)))
	sources := make([]ownedHistoryRedisObservation, 0, len(values))
	for _, value := range values {
		sources = append(sources, historyParseRedis(value))
	}
	return sources
}

type ownedHistoryRedisEvidence struct {
	requestDigest string
	observedAt    time.Time
	json          string
	failure       string
}

func ownedHistoryObservationValue(r ownedHistoryRedisObservation) map[string]any {
	return map[string]any{"kind": r.kind, "redis_stream": r.stream, "consumer_group": r.group,
		"message_id": historyOptional(r.message), "dispatch_key": historyOptional(r.key),
		"payload_sha256": r.sha, "marker_message_id": historyOptional(r.marker),
		"pending_message_ids": historyDecode([]byte(r.pending)), "observed_at": historyISO(r.observedAt)}
}

// Locators come from the current complete DB graph and approved retained lineage,
// never from queue payloads, an API request, or the certificate's old observations.
func ownedHistoryRedisRequest(snapshot ownedHistorySnapshot) (request *ownedHistoryObservationRequest, err error) {
	defer historyRecover(&err, "")
	rows := snapshot.rows()
	_, certificate, _ := historyApprovedAuthority(rows, snapshot.observedAt)
	if certificate == nil {
		return nil, nil
	}
	graph := historyTerminalGraph(rows, certificate)
	historyParseGraph(graph)
	historyRequire(historyEqual(historyTerminalProjection(graph), historyTerminalProjection(historyObject(certificate["terminal_graph"]))), "owned_history_retired_changed")
	nodes := historyIndex(graph["node_executions"])
	values, identities := []any{}, []any{}
	appendSource := func(kind, stream, group string, message, key *string, sha string) {
		r := ownedHistoryRedisObservation{kind: kind, stream: stream, group: group, message: message, key: key, sha: sha, pending: "[]", observedAt: snapshot.observedAt}
		value := ownedHistoryObservationValue(r)
		historyParseRedis(value)
		values = append(values, value)
		identities = append(identities, []any{kind, stream, group, historyOptional(message), historyOptional(key), sha})
	}
	for _, d := range historyRows(graph["worker_task_dispatches"]) {
		node := nodes[historyReference(d["node_execution_id"])]
		historyRequire(node != nil, "owned_history_retired_orphan")
		worker := historyWorkerType(historyString(node["node_type"]))
		historyRequire(worker != "" && d["redis_stream"] == "vp:tasks:"+worker && d["consumer_group"] == worker+"-workers", "owned_history_retired_changed")
		appendSource("task", historyString(d["redis_stream"]), historyString(d["consumer_group"]), historyNullableString(d["redis_message_id"]), historyNullableString(d["dispatch_key"]), historyHashValue(d["payload_sha256"]))
	}
	events := map[string]string{}
	for _, d := range historyRows(graph["registered_worker_event_deliveries"]) {
		historyRequire(d["redis_stream"] == "vp:events" && d["consumer_group"] == "orchestrator" && d["message_id"] != nil, "owned_history_retired_changed")
		message, sha := historyString(d["message_id"]), historyHashValue(d["payload_sha256"])
		previous, exists := events[message]
		historyRequire(!exists || previous == sha, "owned_history_retired_changed")
		events[message] = sha
	}
	for _, message := range historySortedKeys(events) {
		appendSource("event", "vp:events", "orchestrator", &message, nil, events[message])
	}
	return &ownedHistoryObservationRequest{historyCanonical(values), historyHash(identities), snapshot.observedAt}, nil
}

func ownedHistoryWithObservations(snapshot ownedHistorySnapshot, evidence *ownedHistoryRedisEvidence) (ownedHistorySnapshot, error) {
	request, err := ownedHistoryRedisRequest(snapshot)
	if err != nil {
		return ownedHistorySnapshot{}, err
	}
	if request == nil {
		return newOwnedHistorySnapshot([]byte(snapshot.rowsJSON), snapshot.platformChannelID, snapshot.observedAt, []byte("[]"))
	}
	if evidence == nil || evidence.requestDigest != request.digest || snapshot.observedAt.Before(evidence.observedAt) || snapshot.observedAt.Sub(evidence.observedAt) > 60*time.Second {
		return ownedHistorySnapshot{}, request
	}
	if evidence.failure != "" {
		return ownedHistorySnapshot{}, ownedHistoryError(evidence.failure)
	}
	return newOwnedHistorySnapshot([]byte(snapshot.rowsJSON), snapshot.platformChannelID, snapshot.observedAt, []byte(evidence.json))
}

type ownedHistoryRedisClient interface {
	Do(context.Context, ...any) *redis.Cmd
	Close() error
}

type ownedHistoryRedisFactory func(*redis.Options) ownedHistoryRedisClient

func ownedHistoryRedisOptions(rawURL string) (*redis.Options, error) {
	parsed, parseErr := url.Parse(rawURL)
	if parseErr != nil || parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") || parsed.User == nil {
		return nil, ownedHistoryError("owned_history_redis_configuration")
	}
	password, hasPassword := parsed.User.Password()
	principal := parsed.User.Username()
	if principal == "" || principal == "default" || !hasPassword || password == "" {
		return nil, ownedHistoryError("owned_history_redis_configuration")
	}
	options, parseErr := redis.ParseURL(rawURL)
	if parseErr != nil || options.DB < 0 || options.DB > 15 {
		return nil, ownedHistoryError("owned_history_redis_configuration")
	}
	options.MaxRetries, options.PoolSize = -1, 1
	options.DialTimeout, options.ReadTimeout, options.WriteTimeout = 5*time.Second, 5*time.Second, 5*time.Second
	options.ContextTimeoutEnabled, options.DisableIdentity = true, true
	options.Protocol = 2
	return options, nil
}

func observeOwnedHistoryRedis(ctx context.Context, request *ownedHistoryObservationRequest, rawURL string, factory ownedHistoryRedisFactory) (evidence *ownedHistoryRedisEvidence, err error) {
	defer historyRecover(&err, "owned_history_redis_read_failed")
	options, parseErr := ownedHistoryRedisOptions(rawURL)
	if parseErr != nil {
		return nil, parseErr
	}
	principal := options.Username
	if factory == nil {
		factory = func(options *redis.Options) ownedHistoryRedisClient { return redis.NewClient(options) }
	}
	client := factory(options)
	if client == nil {
		return nil, ownedHistoryError("owned_history_redis_read_failed")
	}
	defer func() {
		if closeErr := client.Close(); closeErr != nil {
			evidence, err = nil, ownedHistoryError("owned_history_redis_close_failed")
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	who, readErr := client.Do(ctx, "ACL", "WHOAMI").Text()
	if readErr != nil || who != principal {
		return nil, ownedHistoryError("owned_history_redis_identity")
	}
	values := []any{}
	for _, source := range request.sources() {
		if source.kind == "task" {
			marker, readErr := client.Do(ctx, "GET", "vp:worker-task-dispatch:"+*source.key).Text()
			if readErr != nil && !errors.Is(readErr, redis.Nil) {
				return nil, ownedHistoryError("owned_history_redis_read_failed")
			}
			if readErr == nil {
				source.marker = &marker
			}
		}
		start, end := "-", "+"
		if source.message != nil {
			start, end = *source.message, *source.message
		}
		pending, readErr := client.Do(ctx, "XPENDING", source.stream, source.group, start, end, 1).Slice()
		if readErr != nil || len(pending) > 1 {
			return nil, ownedHistoryError("owned_history_redis_read_failed")
		}
		ids := []any{}
		for _, raw := range pending {
			entry := historyArray(raw)
			historyRequire(len(entry) == 4)
			id := historyString(entry[0])
			historyRequire(historyRedisID.MatchString(id))
			ids = append(ids, id)
		}
		source.pending = historyCanonical(ids)
		values = append(values, ownedHistoryObservationValue(source))
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return &ownedHistoryRedisEvidence{requestDigest: request.digest, observedAt: request.observedAt, json: historyCanonical(values)}, nil
}

// A required observation aborts the caller-owned DB phase before external I/O.
// The phase must start a fresh transaction on reentry; only Redis evidence crosses
// the boundary. A second long lock wait refuses instead of looping indefinitely.
func (h HandlerService) withOwnedTickQueuePhase(ctx context.Context, item QueueItemRow, phase func(HandlerService) error) error {
	return h.withOwnedHistoryObservation(ctx, func(evidence *ownedHistoryRedisEvidence) error {
		return h.withQueueExecutionPhase(ctx, item, func(fenced HandlerService) error {
			clone := *fenced.Store
			clone.ownedHistoryEvidence = evidence
			fenced.Store = &clone
			return phase(fenced)
		})
	})
}

func (s *Store) withOwnedTickChannelPhase(ctx context.Context, channelID string, h HandlerService, phase func(*Store) error) error {
	h.Store = s
	return h.withOwnedHistoryObservation(ctx, func(evidence *ownedHistoryRedisEvidence) error {
		return s.withChannelExecutionFence(ctx, channelID, true, func(fenced *Store) error {
			clone := *fenced
			clone.ownedHistoryEvidence = evidence
			return phase(&clone)
		})
	})
}

func (h HandlerService) withOwnedHistoryObservation(ctx context.Context, phase func(*ownedHistoryRedisEvidence) error) error {
	if h.Store != nil && h.Store.hasExecutionTransaction() {
		return ownedHistoryError("owned_history_external_phase_locked")
	}
	var evidence *ownedHistoryRedisEvidence
	for attempt := 0; attempt <= 2; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := phase(evidence)
		var request *ownedHistoryObservationRequest
		if !errors.As(err, &request) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 2 {
			return ErrHandlerSnapshotStale
		}
		evidence, err = observeOwnedHistoryRedis(ctx, request, h.Config.OwnedHistoryRedisURL, h.ownedHistoryRedisFactory)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			// Read failures can close intake only after the fresh DB/queue authority
			// phase. They cannot discard an already denied prepared PDS decision.
			var reason ownedHistoryError
			if !errors.As(err, &reason) {
				reason = "owned_history_redis_read_failed"
			}
			evidence = &ownedHistoryRedisEvidence{requestDigest: request.digest, observedAt: request.observedAt, failure: string(reason)}
		}
	}
	return ErrHandlerSnapshotStale
}
