package channelops

import (
	"context"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func ownedB2DisposableRedis(raw, confirmation string) (*redis.Options, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "redis" || u.Hostname() != "127.0.0.1" || u.Path != "/15" || u.User == nil || u.RawQuery != "" || u.Fragment != "" || confirmation != u.Host+"/15" {
		return nil, errOwnedInventory
	}
	port, err := strconv.Atoi(u.Port())
	password, ok := u.User.Password()
	if err != nil || port < 1024 || port > 65535 || port == 6379 || u.User.Username() == "" || !ok || password == "" {
		return nil, errOwnedInventory
	}
	options, err := redis.ParseURL(raw)
	if err != nil {
		return nil, errOwnedInventory
	}
	options.MaxRetries, options.PoolSize = -1, 1
	options.DialTimeout, options.ReadTimeout, options.WriteTimeout = 3*time.Second, 3*time.Second, 3*time.Second
	options.ContextTimeoutEnabled, options.DisableIdentity = true, true
	options.Protocol = 2
	return options, nil
}

func TestOwnedB2RedisFixtureRejectsAmbientOrUnconfirmedTargets(t *testing.T) {
	good := "redis://native-reader:synthetic@127.0.0.1:55464/15"
	for _, raw := range []string{"", strings.Replace(good, "127.0.0.1", "remote", 1), strings.Replace(good, "/15", "/0", 1), strings.Replace(good, "55464", "6379", 1), good + "?db=0"} {
		if _, err := ownedB2DisposableRedis(raw, "127.0.0.1:55464/15"); err == nil {
			t.Fatal("unsafe Redis fixture accepted")
		}
	}
	if _, err := ownedB2DisposableRedis(good, ""); err == nil {
		t.Fatal("missing Redis confirmation accepted")
	}
	if _, err := ownedB2DisposableRedis(good, "127.0.0.1:55464/15"); err != nil {
		t.Fatal("explicit scratch target refused")
	}
}

// Parent-only: the setup credential owns a completely empty, isolated DB15.
// The tested production observer gets only the separately provisioned native ACL
// credential. No ACL/DCL, FLUSHDB, fixed-stream janitor or real platform call.
func TestOwnedB2RedisNativeRetirementObservation(t *testing.T) {
	raw := os.Getenv("OWNED_HISTORY_B2_REDIS_TEST_URL")
	setupRaw := os.Getenv("OWNED_HISTORY_B2_REDIS_SETUP_TEST_URL")
	if testing.Short() || raw == "" || setupRaw == "" {
		t.Skip("explicit empty disposable Redis DB15 and native reader credentials required")
	}
	confirmation := os.Getenv("OWNED_HISTORY_B2_REDIS_TEST_CONFIRM")
	readerOptions, err := ownedB2DisposableRedis(raw, confirmation)
	if err != nil || readerOptions.Username == "default" {
		t.Fatal("invalid explicit native Redis reader")
	}
	setupOptions, err := ownedB2DisposableRedis(setupRaw, confirmation)
	if err != nil || setupOptions.Addr != readerOptions.Addr {
		t.Fatal("invalid separate Redis setup credential")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	setup := redis.NewClient(setupOptions)
	defer setup.Close()
	if who, err := setup.ACLWhoAmI(ctx).Result(); err != nil || who != setupOptions.Username {
		t.Fatal("setup Redis identity mismatch")
	}
	if size, err := setup.DBSize(ctx).Result(); err != nil || size != 0 {
		t.Fatal("Redis fixture requires an empty dedicated DB15")
	}
	owned := []string{}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		client := redis.NewClient(setupOptions)
		defer client.Close()
		if len(owned) > 0 {
			if err := client.Del(cleanup, owned...).Err(); err != nil {
				t.Error("owned Redis fixture cleanup failed")
			}
		}
	})
	f := historyGolden(t, "retired_unassigned")
	snapshot, err := newOwnedHistorySnapshot(historyJSON(t, historyTestRows(f)), historyString(f["platform_channel_id"]), time.Now().UTC(), []byte("[]"))
	if err != nil {
		t.Fatal(err)
	}
	request, err := ownedHistoryRedisRequest(snapshot)
	if err != nil || request == nil {
		t.Fatal("native locators missing")
	}
	groups := map[string]string{}
	var source ownedHistoryRedisObservation
	for _, r := range request.sources() {
		groups[r.stream] = r.group
		if r.kind == "task" && r.message != nil && source.message == nil {
			source = r
		}
	}
	streams := make([]string, 0, len(groups))
	for stream := range groups {
		streams = append(streams, stream)
	}
	sort.Strings(streams)
	for _, stream := range streams {
		if err := setup.XGroupCreateMkStream(ctx, stream, groups[stream], "0-0").Err(); err != nil {
			t.Fatal("fixture group creation failed")
		}
		owned = append(owned, stream)
	}
	for _, r := range request.sources() {
		if r.kind != "task" || r.message == nil {
			continue
		}
		key := "vp:worker-task-dispatch:" + *r.key
		if ok, err := setup.SetNX(ctx, key, *r.message, 0).Result(); err != nil || !ok {
			t.Fatal("fixture marker was not newly owned")
		}
		owned = append(owned, key)
	}
	observe := func(wantBlock bool) {
		t.Helper()
		evidence, err := observeOwnedHistoryRedis(ctx, request, raw, nil)
		if err != nil {
			t.Fatalf("native credential observer failed: %T", err)
		}
		fresh, err := ownedHistoryWithObservations(snapshot, evidence)
		if err != nil {
			t.Fatal("native observation failed attachment")
		}
		got := assessOwnedHistorySnapshot(fresh, time.Now().UTC())
		if wantBlock {
			if got.BlockReason == nil || *got.BlockReason != "owned_history_retired_redis_changed" {
				t.Fatal("actual Redis drift accepted")
			}
		} else if got.BlockReason != nil || got.StableHistorySHA256 != historyObject(f["expected"])["stable_history_sha256"] || got.AuthoritySHA256 != historyObject(f["expected"])["authority_sha256"] {
			t.Fatal("native zero-PEL proof refused", got.BlockReason)
		}
	}
	observe(false)
	key := "vp:worker-task-dispatch:" + *source.key
	if err := setup.Set(ctx, key, "9999-0", 0).Err(); err != nil {
		t.Fatal("fixture marker drift failed")
	}
	observe(true)
	if err := setup.Set(ctx, key, *source.message, 0).Err(); err != nil {
		t.Fatal("fixture marker restoration failed")
	}
	if _, err := setup.XAdd(ctx, &redis.XAddArgs{Stream: source.stream, ID: *source.message, Values: map[string]any{"synthetic": "B2 observation"}}).Result(); err != nil {
		t.Fatal("fixture pending message insert failed")
	}
	if _, err := setup.XReadGroup(ctx, &redis.XReadGroupArgs{Group: source.group, Consumer: "b2-fixture-only", Streams: []string{source.stream, ">"}, Count: 1, Block: -1}).Result(); err != nil {
		t.Fatal("fixture native PEL creation failed")
	}
	observe(true)
	if n, err := setup.XAck(ctx, source.stream, source.group, *source.message).Result(); err != nil || n != 1 {
		t.Fatal("fixture own PEL settlement failed")
	}
	observe(false)
}
