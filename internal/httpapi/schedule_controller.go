package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Ctwqk/videoprocess/internal/store"
)

type ScheduleController interface {
	Status(ctx context.Context) (store.VideoScheduleStatusRow, error)
	SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error)
}

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
	return c.request(ctx, http.MethodGet, "status")
}

func (c *httpScheduleController) SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error) {
	return c.request(ctx, http.MethodPost, strings.ToLower(state))
}

func (c *httpScheduleController) request(ctx context.Context, method string, action string) (store.VideoScheduleStatusRow, error) {
	url := c.baseURL + "/internal/schedule/video/" + action
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return store.VideoScheduleStatusRow{}, err
	}
	defer resp.Body.Close()
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
