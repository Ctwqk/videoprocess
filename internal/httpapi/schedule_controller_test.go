package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Ctwqk/videoprocess/internal/store"
)

func TestHTTPScheduleControllerPostsRequestedState(t *testing.T) {
	var method string
	var path string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		path = r.URL.Path
		_ = json.NewEncoder(w).Encode(store.VideoScheduleStatusRow{State: "OPEN", ReleasedJobs: 2})
	}))
	defer upstream.Close()
	controller := NewHTTPScheduleController(upstream.URL, upstream.Client())

	row, err := controller.SetState(context.Background(), "OPEN")

	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/internal/schedule/video/open" {
		t.Fatalf("request = %s %s", method, path)
	}
	if row.State != "OPEN" || row.ReleasedJobs != 2 {
		t.Fatalf("row = %#v", row)
	}
}

func TestScheduleRouteUsesConfiguredController(t *testing.T) {
	controller := &fakeScheduleController{
		setRows: []store.VideoScheduleStatusRow{{State: "OPEN", ReleasedJobs: 1}},
	}
	server := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Schedule:       controller,
	})
	req := httptest.NewRequest(http.MethodPost, "/internal/schedule/video/open", nil)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !reflect.DeepEqual(controller.setStates, []string{"OPEN"}) {
		t.Fatalf("states = %#v", controller.setStates)
	}
}

func TestCoordinatedScheduleOpenAggregatesGoAndPythonReleases(t *testing.T) {
	local := &fakeScheduleController{
		setRows:   []store.VideoScheduleStatusRow{{State: "OPEN", ReleasedJobs: 1}},
		statusRow: store.VideoScheduleStatusRow{State: "OPEN", ActiveJobs: 3},
	}
	python := &fakeScheduleController{
		setRows: []store.VideoScheduleStatusRow{{State: "OPEN", ReleasedJobs: 2}},
	}
	controller := NewCoordinatedScheduleController(local, python)

	row, err := controller.SetState(context.Background(), "OPEN")

	if err != nil {
		t.Fatal(err)
	}
	if row.State != "OPEN" || row.ActiveJobs != 3 || row.ReleasedJobs != 3 {
		t.Fatalf("row = %#v", row)
	}
	if !reflect.DeepEqual(local.setStates, []string{"OPEN"}) {
		t.Fatalf("local states = %#v", local.setStates)
	}
	if !reflect.DeepEqual(python.setStates, []string{"OPEN"}) {
		t.Fatalf("python states = %#v", python.setStates)
	}
}

func TestCoordinatedScheduleOpenFailsClosedWhenPythonHandoffFails(t *testing.T) {
	local := &fakeScheduleController{
		setRows: []store.VideoScheduleStatusRow{
			{State: "OPEN", ReleasedJobs: 1},
			{State: "CLOSED"},
		},
	}
	python := &fakeScheduleController{setErr: errors.New("python unavailable")}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.SetState(context.Background(), "OPEN")

	if err == nil {
		t.Fatal("expected Python handoff failure")
	}
	if !reflect.DeepEqual(local.setStates, []string{"OPEN", "CLOSED"}) {
		t.Fatalf("local states = %#v", local.setStates)
	}
}

func TestCoordinatedScheduleOpenFailsClosedWhenPythonDoesNotOpen(t *testing.T) {
	local := &fakeScheduleController{
		setRows: []store.VideoScheduleStatusRow{
			{State: "OPEN", ReleasedJobs: 1},
			{State: "CLOSED"},
		},
	}
	python := &fakeScheduleController{
		setRows: []store.VideoScheduleStatusRow{{State: "CLOSED"}},
	}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.SetState(context.Background(), "OPEN")

	if err == nil {
		t.Fatal("expected mismatched Python schedule state to fail")
	}
	if !reflect.DeepEqual(local.setStates, []string{"OPEN", "CLOSED"}) {
		t.Fatalf("local states = %#v", local.setStates)
	}
}

type fakeScheduleController struct {
	setStates []string
	setRows   []store.VideoScheduleStatusRow
	setErr    error
	statusRow store.VideoScheduleStatusRow
	statusErr error
}

func (f *fakeScheduleController) Status(context.Context) (store.VideoScheduleStatusRow, error) {
	return f.statusRow, f.statusErr
}

func (f *fakeScheduleController) SetState(_ context.Context, state string) (store.VideoScheduleStatusRow, error) {
	f.setStates = append(f.setStates, state)
	if f.setErr != nil {
		return store.VideoScheduleStatusRow{}, f.setErr
	}
	if len(f.setRows) == 0 {
		return store.VideoScheduleStatusRow{State: state}, nil
	}
	row := f.setRows[0]
	f.setRows = f.setRows[1:]
	return row, nil
}
