package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/store"
)

type ScheduleController interface {
	Status(ctx context.Context) (store.VideoScheduleStatusRow, error)
	SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error)
	OpenExpectedJob(ctx context.Context, expectedJobID string) (store.VideoScheduleStatusRow, error)
}

var (
	ErrScheduleGuardMismatch          = errors.New("schedule guard mismatch")
	errGuardedScheduleHandoff         = errors.New("guarded schedule handoff failed")
	errGuardedScheduleCloseIncomplete = errors.New("guarded schedule handoff failed; best-effort close incomplete")
	guardedScheduleCleanupTimeout     = 5 * time.Second
)

type storeScheduleController struct {
	store *store.Store
}

func NewStoreScheduleController(st *store.Store) ScheduleController {
	return &storeScheduleController{store: st}
}

func (c *storeScheduleController) Status(ctx context.Context) (store.VideoScheduleStatusRow, error) {
	return c.store.GetVideoScheduleStatus(ctx)
}

func (c *storeScheduleController) SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error) {
	return c.store.SetVideoScheduleState(ctx, state)
}

func (c *storeScheduleController) OpenExpectedJob(
	ctx context.Context,
	expectedJobID string,
) (store.VideoScheduleStatusRow, error) {
	row, err := c.store.OpenVideoScheduleForJob(ctx, expectedJobID)
	if errors.Is(err, store.ErrVideoScheduleGuardMismatch) {
		return store.VideoScheduleStatusRow{}, ErrScheduleGuardMismatch
	}
	return row, err
}

type httpScheduleController struct {
	baseURL string
	client  *http.Client
}

func NewHTTPScheduleController(baseURL string, client *http.Client) ScheduleController {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &httpScheduleController{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  client,
	}
}

func (c *httpScheduleController) Status(ctx context.Context) (store.VideoScheduleStatusRow, error) {
	return c.request(ctx, http.MethodGet, "status", "")
}

func (c *httpScheduleController) SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error) {
	return c.request(ctx, http.MethodPost, strings.ToLower(state), "")
}

func (c *httpScheduleController) OpenExpectedJob(
	ctx context.Context,
	expectedJobID string,
) (store.VideoScheduleStatusRow, error) {
	return c.request(ctx, http.MethodPost, "open", expectedJobID)
}

func (c *httpScheduleController) request(
	ctx context.Context,
	method string,
	action string,
	expectedJobID string,
) (store.VideoScheduleStatusRow, error) {
	requestURL := c.baseURL + "/internal/schedule/video/" + action
	if expectedJobID != "" {
		query := url.Values{"expected_job_id": []string{expectedJobID}}
		requestURL += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, nil)
	if err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		return store.VideoScheduleStatusRow{}, ErrScheduleGuardMismatch
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return store.VideoScheduleStatusRow{}, fmt.Errorf("schedule handoff returned HTTP %d", resp.StatusCode)
	}
	var row store.VideoScheduleStatusRow
	if err := json.NewDecoder(resp.Body).Decode(&row); err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	return row, nil
}

type coordinatedScheduleController struct {
	local  ScheduleController
	python ScheduleController
}

func NewCoordinatedScheduleController(local ScheduleController, python ScheduleController) ScheduleController {
	return &coordinatedScheduleController{local: local, python: python}
}

func (c *coordinatedScheduleController) Status(ctx context.Context) (store.VideoScheduleStatusRow, error) {
	return c.local.Status(ctx)
}

func (c *coordinatedScheduleController) SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error) {
	localRow, err := c.local.SetState(ctx, state)
	if err != nil || state != "OPEN" || c.python == nil {
		return localRow, err
	}

	pythonRow, err := c.python.SetState(ctx, state)
	if err == nil && pythonRow.State != state {
		err = fmt.Errorf("Python schedule handoff returned state %q", pythonRow.State)
	}
	if err != nil {
		_, closeErr := c.local.SetState(ctx, "CLOSED")
		if closeErr != nil {
			return store.VideoScheduleStatusRow{}, fmt.Errorf(
				"Python schedule handoff failed and local close failed: %w",
				err,
			)
		}
		return store.VideoScheduleStatusRow{}, fmt.Errorf("Python schedule handoff failed: %w", err)
	}

	row, err := c.local.Status(ctx)
	if err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	row.ReleasedJobs = localRow.ReleasedJobs + pythonRow.ReleasedJobs
	return row, nil
}

func (c *coordinatedScheduleController) OpenExpectedJob(
	ctx context.Context,
	expectedJobID string,
) (store.VideoScheduleStatusRow, error) {
	if c.python == nil {
		return c.local.OpenExpectedJob(ctx, expectedJobID)
	}
	pythonRow, err := c.python.OpenExpectedJob(ctx, expectedJobID)
	if err != nil {
		if errors.Is(err, ErrScheduleGuardMismatch) {
			return store.VideoScheduleStatusRow{}, guardedScheduleHandoffError(err, false)
		}
		closeIncomplete := c.closeAfterGuardedFailure(ctx, true)
		return store.VideoScheduleStatusRow{}, guardedScheduleHandoffError(err, closeIncomplete)
	}
	if !isExpectedGuardedOpen(pythonRow, expectedJobID) || pythonRow.ReleasedJobs != 1 {
		closeIncomplete := c.closeAfterGuardedFailure(ctx, true)
		return store.VideoScheduleStatusRow{}, guardedScheduleHandoffError(
			errGuardedScheduleHandoff,
			closeIncomplete,
		)
	}

	localRow, err := c.local.Status(ctx)
	if err == nil && isExpectedGuardedOpen(localRow, expectedJobID) {
		localRow.ReleasedJobs = 1
		return localRow, nil
	}
	closeIncomplete := c.closeAfterGuardedFailure(ctx, true)
	return store.VideoScheduleStatusRow{}, guardedScheduleHandoffError(
		errGuardedScheduleHandoff,
		closeIncomplete,
	)
}

func (c *coordinatedScheduleController) closeAfterGuardedFailure(
	ctx context.Context,
	closePython bool,
) bool {
	closeIncomplete := false
	if closePython && c.python != nil {
		if err := closeScheduleWithFreshContext(ctx, c.python); err != nil {
			closeIncomplete = true
		}
	}
	if err := closeScheduleWithFreshContext(ctx, c.local); err != nil {
		closeIncomplete = true
	}
	return closeIncomplete
}

func isExpectedGuardedOpen(row store.VideoScheduleStatusRow, expectedJobID string) bool {
	return row.State == "OPEN" && row.GuardedJobID != nil && *row.GuardedJobID == expectedJobID
}

func closeScheduleWithFreshContext(ctx context.Context, controller ScheduleController) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), guardedScheduleCleanupTimeout)
	defer cancel()
	_, err := controller.SetState(cleanupCtx, "CLOSED")
	return err
}

func guardedScheduleHandoffError(cause error, closeIncomplete bool) error {
	result := errGuardedScheduleHandoff
	if closeIncomplete {
		result = errGuardedScheduleCloseIncomplete
	}
	if errors.Is(cause, ErrScheduleGuardMismatch) {
		return fmt.Errorf("%w: %v", ErrScheduleGuardMismatch, result)
	}
	return result
}
