package channelops

import "time"

// The full history membership is wider than the set of eligible production targets.
func ownedHistoryProductionTarget(data ownedInventoryData, now time.Time) (err error) {
	defer historyRecover(&err, "")
	historyRequire(data.History != nil, "owned_history_snapshot_missing")
	historyRequire(data.History.platformChannelID == data.Inventory["platform_channel_id"], "owned_history_snapshot_scope")
	rows := data.History.rows()
	bindings, cert, _ := historyApprovedAuthority(rows, now)
	target, channel := data.Inventory["target_account_id"], data.Inventory["channel_profile_id"]
	for id, binding := range bindings {
		historyRequire(id != target && binding["legacy_channel_profile_id"] != channel, "owned_inventory_history_target")
	}
	if cert != nil {
		historyRequire(cert["legacy_account_id"] != target && cert["legacy_channel_profile_id"] != channel, "owned_inventory_history_target")
	}
	production := []string{}
	for _, account := range historyRows(rows["publishing_accounts"]) {
		id := historyString(account["id"])
		if historyPlatform(account) == "youtube" && account["platform_account_id"] == data.Inventory["platform_channel_id"] && bindings[id] == nil {
			production = append(production, id)
		}
	}
	historyRequire(len(production) == 1 && production[0] == target, "owned_inventory_account_alias")
	return nil
}

func ownedQueuesSafe(channelID string, data ownedInventoryData, now time.Time) (safe bool) {
	defer func() {
		if p := recover(); p != nil {
			if _, ok := p.(ownedHistoryError); !ok {
				panic(p)
			}
			safe = false
		}
	}()
	historyRequire(data.History != nil)
	rows := data.History.rows()
	assessment := assessOwnedHistorySnapshot(*data.History, now)
	historyRequire(assessment.BlockReason == nil)
	members := map[string]bool{}
	for _, id := range assessment.AccountIDs {
		members[id] = true
	}
	allowed := map[string]map[string]any{}
	for _, task := range historyRows(rows["production_tasks"]) {
		if !members[historyReference(task["target_account_id"])] {
			continue
		}
		h := historyTask(rows, task)
		pubs := historyArray(h["publications"])
		if len(pubs) != 1 {
			continue
		}
		pub := historyObject(pubs[0])
		if pub["scheduled_publish_at"] == nil {
			continue
		}
		historyMetricsReady(h, pub, historyTime(pub["scheduled_publish_at"]), now)
		for _, q := range historyRows(h["queues"]) {
			if q["kind"] == QueueCollectMetrics && historyObject(q["payload_json"])["metric_schedule_id"] != nil {
				allowed[historyString(q["id"])] = q
			}
		}
	}
	for _, q := range data.Queues {
		if q["kind"] == QueueAgentTick && q["channel_profile_id"] == channelID && ownedMap(q["payload_json"])["channel_id"] == channelID && historyQueueClean(q) {
			continue
		}
		if expected := allowed[ownedString(q["id"])]; expected != nil && ownedEqual(expected, q) {
			continue
		}
		return false
	}
	return true
}
