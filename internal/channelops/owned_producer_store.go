package channelops

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// Caller owns the native queue/leader/channel transaction. A Redis observation
// request aborts this phase; withOwnedTickQueuePhase observes outside row locks.
func (s *Store) lockOwnedProducer(ctx context.Context, task ProductionTaskRow, renderSHA256 string) (*ownedProducerAuthority, error) {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != task.ChannelProfileID {
		return nil, errOwnedInventory
	}
	db := s.db()
	var schedule string
	if err := db.QueryRow(ctx, `SELECT state FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE`).Scan(&schedule); err != nil {
		return nil, err
	}
	var protected bool
	if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM owned_seed_inventories WHERE approved_at IS NOT NULL)`).Scan(&protected); err != nil {
		return nil, err
	}
	if !protected {
		return nil, nil
	}
	var platform string
	if err := db.QueryRow(ctx, `SELECT a.platform_account_id FROM production_tasks t JOIN publishing_accounts a ON a.id=t.target_account_id WHERE t.id=$1::uuid AND t.channel_profile_id=$2::uuid`, task.ID, task.ChannelProfileID).Scan(&platform); err != nil {
		return nil, err
	}
	if !historyUC.MatchString(platform) {
		return nil, ownedHistoryError("owned_history_unclassified")
	}
	if _, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, ownedPlatformKey(platform)); err != nil {
		return nil, err
	}
	loadOne := func(query string, args ...any) (map[string]any, error) {
		var raw []byte
		if err := db.QueryRow(ctx, query, args...).Scan(&raw); err != nil {
			return nil, err
		}
		value, err := ownedDecode(raw)
		if err != nil || ownedMap(value) == nil {
			return nil, errOwnedInventory
		}
		return ownedMap(value), nil
	}
	var inventoryID *string
	err := db.QueryRow(ctx, `SELECT inventory_id::text FROM owned_seed_inventory_items WHERE production_task_id=$1::uuid`, task.ID).Scan(&inventoryID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	bindings := map[string]any{}
	var inventory map[string]any
	if inventoryID != nil {
		inventory, err = loadOne(`SELECT row_to_json(i) FROM owned_seed_inventories i WHERE id=$1::uuid FOR UPDATE`, *inventoryID)
		if err != nil {
			return nil, err
		}
		for _, binding := range []struct {
			name, query string
			id          any
		}{
			{"channel", `SELECT row_to_json(c) FROM channel_profiles c WHERE id=$1::uuid`, task.ChannelProfileID},
			{"account", `SELECT row_to_json(a) FROM publishing_accounts a WHERE id=$1::uuid FOR SHARE`, inventory["target_account_id"]},
			{"lane", `SELECT row_to_json(l) FROM topic_lanes l WHERE id=$1::uuid FOR SHARE`, inventory["topic_lane_id"]},
			{"format", `SELECT row_to_json(f) FROM lane_format_matrix f WHERE id=$1::uuid FOR SHARE`, inventory["lane_format_id"]},
		} {
			bindings[binding.name], err = loadOne(binding.query, binding.id)
			if err != nil {
				return nil, err
			}
		}
		locked, err := db.Query(ctx, `SELECT i.id FROM owned_seed_inventory_items i JOIN manual_seeds s ON s.id=i.manual_seed_id JOIN assets a ON a.id=i.asset_id WHERE i.inventory_id=$1::uuid ORDER BY i.ordinal FOR UPDATE OF i FOR SHARE OF s,a`, *inventoryID)
		if err != nil {
			return nil, err
		}
		count := 0
		for locked.Next() {
			count++
		}
		err = locked.Err()
		locked.Close()
		if err != nil {
			return nil, err
		}
		if count != 7 {
			return nil, ownedHistoryError("owned_inventory_cardinality")
		}
	}
	// Every potentially blocking row lock precedes the sole complete history read.
	snapshot, err := loadOwnedHistorySnapshot(ctx, db, platform)
	if err != nil {
		return nil, err
	}
	snapshot, err = ownedHistoryWithObservations(snapshot, s.ownedHistoryEvidence)
	if err != nil {
		return nil, err
	}
	var now time.Time
	if err := db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	identity, assessment, err := assessOwnedProducerSnapshot(snapshot, now.UTC(), task.ID, renderSHA256)
	if err != nil {
		return nil, err
	}
	var configurationSHA any
	if identity.InventoryID != "" {
		configuration, err := ownedConfiguration(bindings)
		if err != nil {
			return nil, err
		}
		configurationSHA, err = ownedHash(configuration)
		if err != nil {
			return nil, err
		}
		if configurationSHA != ownedMap(inventory["manifest_json"])["configuration_sha256"] {
			return nil, ownedHistoryError("owned_inventory_configuration_changed")
		}
		lane, format := ownedMap(bindings["lane"]), ownedMap(bindings["format"])
		if lane["enabled"] != true || lane["paused_until"] != nil || format["enabled"] != true || !ownedEqual(format["source_platforms_json"], []any{}) || format["default_publish_visibility"] != "unlisted" {
			return nil, ownedHistoryError("owned_inventory_producer_controls")
		}
	}
	current, err := s.GetProductionTask(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	prepared, err := newPreparedTaskSnapshot(task)
	if err != nil {
		return nil, err
	}
	if err = prepared.validate(current); err != nil {
		return nil, err
	}
	nullable := func(value string) any {
		if value == "" {
			return nil
		}
		return value
	}
	value := map[string]any{"identity": map[string]any{"task_id": identity.TaskID, "platform_channel_id": identity.PlatformChannelID, "inventory_id": nullable(identity.InventoryID), "item_id": nullable(identity.ItemID), "source_sha256": nullable(identity.SourceSHA256)}, "configuration": configurationSHA, "history": assessment.StableHistorySHA256, "authority": assessment.AuthoritySHA256, "retired_source": assessment.RetiredSourceSHA256, "retired_render": assessment.RetiredRenderSHA256}
	raw, err := ownedDecode([]byte(mustJSON(value)))
	if err != nil {
		return nil, err
	}
	digest, err := ownedHash(raw)
	if err != nil {
		return nil, err
	}
	return &ownedProducerAuthority{Identity: identity, Digest: digest}, nil
}

func (s *Store) persistOwnedTaskPolicy(ctx context.Context, task ProductionTaskRow, key string, evidence map[string]any) error {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != task.ChannelProfileID {
		return errOwnedInventory
	}
	_, err := s.db().Exec(ctx, `UPDATE production_tasks SET agent_approval_evidence_json=(COALESCE(agent_approval_evidence_json,'{}'::json)::jsonb || $2::jsonb)::json WHERE id=$1::uuid`, task.ID, mustJSON(map[string]any{key: evidence}))
	return err
}

func (s *Store) persistOwnedPendingPlan(ctx context.Context, task ProductionTaskRow, observation AutoFlowPlanObservation, evidence map[string]any) error {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != task.ChannelProfileID {
		return errOwnedInventory
	}
	tag, err := s.db().Exec(ctx, `UPDATE production_tasks SET autoflow_plan_id=$2::uuid,
		rationale_json=(COALESCE(rationale_json,'{}'::json)::jsonb || $3::jsonb)::json,
		agent_approval_evidence_json=(COALESCE(agent_approval_evidence_json,'{}'::json)::jsonb || $4::jsonb)::json
		WHERE id=$1::uuid AND state='selected' AND (autoflow_plan_id IS NULL OR autoflow_plan_id=$2::uuid)`, task.ID, observation.PlanID, mustJSON(map[string]any{"autoflow_plan_payload": observation.PlanPayload}), mustJSON(map[string]any{"plan_pds": evidence}))
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrHandlerSnapshotStale
	}
	return nil
}

func (s *Store) holdOwnedProducerPolicy(ctx context.Context, task ProductionTaskRow, planID string, authority *ownedProducerAuthority, key string, evidence map[string]any) error {
	if authority == nil || authority.Identity.InventoryID == "" || authority.Identity.TaskID != task.ID || !s.hasExecutionTransaction() {
		return errOwnedInventory
	}
	approval := map[string]any{}
	for k, value := range task.AgentApprovalEvidenceJSON {
		approval[k] = value
	}
	approval[key] = evidence
	if err := s.holdTask(ctx, task.ID, planID, "owned_inventory_pds_denied", "Owned inventory requires a real policy allow decision.", approval, key); err != nil {
		return err
	}
	if _, err := s.db().Exec(ctx, `UPDATE owned_seed_inventories SET state='held',hold_reason='owned_inventory_pds_denied',updated_at=clock_timestamp() WHERE id=$1::uuid AND channel_profile_id=$2::uuid AND state IN ('approved','exhausted')`, authority.Identity.InventoryID, task.ChannelProfileID); err != nil {
		return err
	}
	_, err := s.db().Exec(ctx, `UPDATE channel_profiles SET intake_paused_at=COALESCE(intake_paused_at,clock_timestamp()),intake_pause_reason='owned_inventory_pds_denied' WHERE id=$1::uuid AND owned_seed_inventory_id=$2::uuid`, task.ChannelProfileID, authority.Identity.InventoryID)
	return err
}

// The enclosing reconciliation transaction owns publication persistence and the
// real queue transition. Generic committed-claim cleanup remains unchanged.
func (s *Store) finishOwnedReconcile(ctx context.Context, item QueueItemRow, publication PublicationRow, task ProductionTaskRow, status YouTubePublicationStatus) error {
	if !s.hasExecutionTransaction() || s.executionChannelID == nil || *s.executionChannelID != task.ChannelProfileID || publication.ProductionTaskID != task.ID || item.Kind != QueueReconcilePublication {
		return errOwnedInventory
	}
	var owned bool
	if err := s.db().QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM owned_seed_inventory_items i JOIN owned_seed_inventories v ON v.id=i.inventory_id JOIN publication_records p ON p.production_task_id=i.production_task_id WHERE p.id=$1::uuid AND i.production_task_id=$2::uuid AND v.channel_profile_id=$3::uuid AND v.approved_at IS NOT NULL)`, publication.ID, task.ID, task.ChannelProfileID).Scan(&owned); err != nil {
		return err
	}
	if owned {
		var schedule string
		if err := s.db().QueryRow(ctx, `SELECT state FROM runtime_schedules WHERE service_name='videoprocess' FOR UPDATE`).Scan(&schedule); err != nil {
			return err
		}
	}
	if err := s.UpdatePublicationStatus(ctx, publication.ID, status); err != nil {
		return err
	}
	if !owned {
		return nil
	}
	if err := s.MarkQueueDone(ctx, item); err != nil {
		return err
	}
	return s.finalizeOwnedInventoryItems(ctx, task.ChannelProfileID, publication.ID)
}
