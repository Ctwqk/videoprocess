package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
)

func TestOpenVideoScheduleForJobIsAtomicAndExclusive(t *testing.T) {
	if os.Getenv("CHANNELOPS_REQUIRE_DATABASE") != "1" {
		t.Skip("guarded schedule store integration requires CHANNELOPS_REQUIRE_DATABASE=1")
	}
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		t.Fatal("DATABASE_URL is required when CHANNELOPS_REQUIRE_DATABASE=1")
	}
	ctx := context.Background()
	st, err := Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration store: %v", err)
	}
	defer st.Close()

	pipelineID := uuid.NewString()
	if _, err := st.Pool.Exec(ctx, `
		INSERT INTO pipelines (id, name, description, definition, is_template, template_tags)
		VALUES ($1::uuid, 'guarded schedule test', '', '{}'::json, FALSE, '{}')
	`, pipelineID); err != nil {
		t.Fatalf("insert pipeline: %v", err)
	}
	defer st.Pool.Exec(ctx, `DELETE FROM pipelines WHERE id = $1::uuid`, pipelineID)

	reset := func(t *testing.T) {
		t.Helper()
		if _, err := st.Pool.Exec(ctx, `DELETE FROM jobs WHERE pipeline_id = $1::uuid`, pipelineID); err != nil {
			t.Fatalf("delete fixture jobs: %v", err)
		}
		if _, err := st.Pool.Exec(ctx, `
			UPDATE runtime_schedules
			SET state = 'CLOSED', updated_by = 'guarded_schedule_test'
			WHERE service_name = $1
		`, VideoScheduleServiceName); err != nil {
			t.Fatalf("close fixture schedule: %v", err)
		}
	}
	insertJob := func(t *testing.T, status string, owner string) string {
		t.Helper()
		jobID := uuid.NewString()
		if _, err := st.Pool.Exec(ctx, `
			INSERT INTO jobs (id, pipeline_id, pipeline_snapshot, status, orchestrator_owner)
			VALUES ($1::uuid, $2::uuid, '{}'::json, $3::job_status, $4)
		`, jobID, pipelineID, status, owner); err != nil {
			t.Fatalf("insert %s job: %v", status, err)
		}
		return jobID
	}
	readState := func(t *testing.T, jobID string) (string, string) {
		t.Helper()
		var scheduleState string
		var jobState string
		if err := st.Pool.QueryRow(ctx, `
			SELECT state FROM runtime_schedules WHERE service_name = $1
		`, VideoScheduleServiceName).Scan(&scheduleState); err != nil {
			t.Fatalf("read schedule state: %v", err)
		}
		if err := st.Pool.QueryRow(ctx, `
			SELECT status::text FROM jobs WHERE id = $1::uuid
		`, jobID).Scan(&jobState); err != nil {
			t.Fatalf("read job state: %v", err)
		}
		return scheduleState, jobState
	}

	t.Run("exact Go waiting job opens and releases one", func(t *testing.T) {
		reset(t)
		expectedJobID := insertJob(t, "WAITING_WINDOW", "go")

		row, err := st.OpenVideoScheduleForJob(ctx, expectedJobID)

		if err != nil {
			t.Fatal(err)
		}
		if row.State != "OPEN" || row.ReleasedJobs != 1 {
			t.Fatalf("row = %#v", row)
		}
		scheduleState, jobState := readState(t, expectedJobID)
		if scheduleState != "OPEN" || jobState != "PENDING" {
			t.Fatalf("schedule=%s job=%s", scheduleState, jobState)
		}
	})

	for _, blocker := range []string{"waiting_job", "active_job", "queued_node", "running_node"} {
		t.Run("rejects_"+blocker, func(t *testing.T) {
			reset(t)
			expectedJobID := insertJob(t, "WAITING_WINDOW", "go")
			switch blocker {
			case "waiting_job":
				insertJob(t, "WAITING_WINDOW", "go")
			case "active_job":
				insertJob(t, "RUNNING", "go")
			case "queued_node", "running_node":
				nodeStatus := "QUEUED"
				if blocker == "running_node" {
					nodeStatus = "RUNNING"
				}
				if _, err := st.Pool.Exec(ctx, `
					INSERT INTO node_executions (id, job_id, node_id, node_type, status)
					VALUES ($1::uuid, $2::uuid, 'guarded_node', 'source', $3::node_status)
				`, uuid.NewString(), expectedJobID, nodeStatus); err != nil {
					t.Fatalf("insert %s node: %v", nodeStatus, err)
				}
			}

			_, err := st.OpenVideoScheduleForJob(ctx, expectedJobID)

			if !errors.Is(err, ErrVideoScheduleGuardMismatch) {
				t.Fatalf("error = %v, want ErrVideoScheduleGuardMismatch", err)
			}
			scheduleState, jobState := readState(t, expectedJobID)
			if scheduleState != "CLOSED" || jobState != "WAITING_WINDOW" {
				t.Fatalf("schedule=%s job=%s", scheduleState, jobState)
			}
		})
	}
	reset(t)
}
