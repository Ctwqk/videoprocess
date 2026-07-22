package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

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

func TestHTTPScheduleControllerGuardedOpenSendsExpectedJobID(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	var method string
	var path string
	var query string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		method = r.Method
		path = r.URL.Path
		query = r.URL.Query().Get("expected_job_id")
		_ = json.NewEncoder(w).Encode(store.VideoScheduleStatusRow{State: "OPEN", ReleasedJobs: 1})
	}))
	defer upstream.Close()
	controller := NewHTTPScheduleController(upstream.URL, upstream.Client())

	row, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

	if err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPost || path != "/internal/schedule/video/open" || query != expectedJobID {
		t.Fatalf("request = %s %s expected_job_id=%q", method, path, query)
	}
	if row.State != "OPEN" || row.ReleasedJobs != 1 {
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

func TestGuardedScheduleRouteRejectsMalformedJobIDWithoutControllerCall(t *testing.T) {
	controller := &fakeScheduleController{}
	server := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Schedule:       controller,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/schedule/video/open?expected_job_id=not-a-uuid",
		nil,
	)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if len(controller.setStates) != 0 || len(controller.guardedJobIDs) != 0 {
		t.Fatalf("controller calls = states %#v guarded %#v", controller.setStates, controller.guardedJobIDs)
	}
}

func TestGuardedScheduleRouteUsesExactJobAndMapsConflict(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	controller := &fakeScheduleController{
		guardedErr: ErrScheduleGuardMismatch,
	}
	server := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Schedule:       controller,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/schedule/video/open?expected_job_id="+expectedJobID,
		nil,
	)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if !reflect.DeepEqual(controller.guardedJobIDs, []string{expectedJobID}) {
		t.Fatalf("guarded job IDs = %#v", controller.guardedJobIDs)
	}
	if len(controller.setStates) != 0 {
		t.Fatalf("legacy state calls = %#v", controller.setStates)
	}
}

func TestGuardedScheduleRouteReturnsExactOpenResult(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	controller := &fakeScheduleController{
		guardedRows: []store.VideoScheduleStatusRow{{State: "OPEN", ReleasedJobs: 1}},
	}
	server := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Schedule:       controller,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/schedule/video/open?expected_job_id="+expectedJobID,
		nil,
	)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	var row store.VideoScheduleStatusRow
	if err := json.NewDecoder(rec.Body).Decode(&row); err != nil {
		t.Fatal(err)
	}
	if row.State != "OPEN" || row.ReleasedJobs != 1 {
		t.Fatalf("row = %#v", row)
	}
	if !reflect.DeepEqual(controller.guardedJobIDs, []string{expectedJobID}) {
		t.Fatalf("guarded job IDs = %#v", controller.guardedJobIDs)
	}
	if len(controller.setStates) != 0 {
		t.Fatalf("legacy state calls = %#v", controller.setStates)
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

func TestCoordinatedGuardedOpenRequiresMatchingPythonAndLocalGuard(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	otherJobID := "22222222-2222-4222-8222-222222222222"
	guard := func(value string) *string { return &value }

	t.Run("matching guards", func(t *testing.T) {
		events := []string{}
		local := &fakeScheduleController{
			name:      "local",
			events:    &events,
			statusRow: store.VideoScheduleStatusRow{State: "OPEN", GuardedJobID: guard(expectedJobID), ActiveJobs: 1},
		}
		python := &fakeScheduleController{
			name: "python", events: &events,
			guardedRows: []store.VideoScheduleStatusRow{{
				State: "OPEN", GuardedJobID: guard(expectedJobID), ReleasedJobs: 1,
			}},
		}
		controller := NewCoordinatedScheduleController(local, python)

		row, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(events, []string{"python.guarded", "local.status"}) {
			t.Fatalf("events = %#v", events)
		}
		if len(local.setStates) != 0 {
			t.Fatalf("local legacy state calls = %#v", local.setStates)
		}
		if row.State != "OPEN" || row.GuardedJobID == nil || *row.GuardedJobID != expectedJobID || row.ReleasedJobs != 1 {
			t.Fatalf("row = %#v", row)
		}
	})

	for _, tc := range []struct {
		name        string
		pythonGuard string
		localGuard  string
	}{
		{name: "Python mismatch", pythonGuard: otherJobID, localGuard: expectedJobID},
		{name: "local mismatch", pythonGuard: expectedJobID, localGuard: otherJobID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			local := &fakeScheduleController{
				statusRow: store.VideoScheduleStatusRow{State: "OPEN", GuardedJobID: guard(tc.localGuard)},
			}
			python := &fakeScheduleController{
				guardedRows: []store.VideoScheduleStatusRow{{
					State: "OPEN", GuardedJobID: guard(tc.pythonGuard), ReleasedJobs: 1,
				}},
			}
			controller := NewCoordinatedScheduleController(local, python)

			if _, err := controller.OpenExpectedJob(context.Background(), expectedJobID); err == nil {
				t.Fatal("expected guarded authority mismatch")
			}
			if !reflect.DeepEqual(python.setStates, []string{"CLOSED"}) ||
				!reflect.DeepEqual(local.setStates, []string{"CLOSED"}) {
				t.Fatalf("python states = %#v local states = %#v", python.setStates, local.setStates)
			}
		})
	}
}

func TestCoordinatedGuardedOpenClosesWhenHandoffVerificationFails(t *testing.T) {
	events := []string{}
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	local := &fakeScheduleController{
		name:      "local",
		events:    &events,
		setRows:   []store.VideoScheduleStatusRow{{State: "CLOSED"}},
		statusRow: store.VideoScheduleStatusRow{State: "CLOSED"},
	}
	python := &fakeScheduleController{
		name:        "python",
		events:      &events,
		guardedRows: []store.VideoScheduleStatusRow{{State: "OPEN", GuardedJobID: &expectedJobID, ReleasedJobs: 1}},
		setRows:     []store.VideoScheduleStatusRow{{State: "CLOSED"}},
	}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

	if err == nil {
		t.Fatal("expected local shared-state verification failure")
	}
	if !reflect.DeepEqual(events, []string{
		"python.guarded",
		"local.status",
		"python.CLOSED",
		"local.CLOSED",
	}) {
		t.Fatalf("events = %#v", events)
	}
	if !reflect.DeepEqual(python.setStates, []string{"CLOSED"}) ||
		!reflect.DeepEqual(local.setStates, []string{"CLOSED"}) {
		t.Fatalf("python states = %#v local states = %#v", python.setStates, local.setStates)
	}
}

func TestCoordinatedGuardedOpenBestEffortClosesBothWithoutLeakingResponses(t *testing.T) {
	events := []string{}
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	local := &fakeScheduleController{
		name:      "local",
		events:    &events,
		statusErr: errors.New("raw-local-status-response-secret"),
		setErr:    errors.New("raw-local-close-response-secret"),
	}
	python := &fakeScheduleController{
		name:        "python",
		events:      &events,
		guardedRows: []store.VideoScheduleStatusRow{{State: "OPEN", GuardedJobID: &expectedJobID, ReleasedJobs: 1}},
		setErr:      errors.New("raw-python-close-response-secret"),
	}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

	if err == nil {
		t.Fatal("expected local shared-state verification failure")
	}
	if !reflect.DeepEqual(events, []string{
		"python.guarded",
		"local.status",
		"python.CLOSED",
		"local.CLOSED",
	}) {
		t.Fatalf("events = %#v", events)
	}
	for _, secret := range []string{
		"raw-local-status-response-secret",
		"raw-python-close-response-secret",
		"raw-local-close-response-secret",
	} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error leaked raw response %q: %v", secret, err)
		}
	}
}

func TestCoordinatedGuardedOpenUncertainErrorClosesPythonAndLocalAfterRequestCancellation(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	local := &fakeScheduleController{}
	python := &fakeScheduleController{guardedErr: context.Canceled}
	controller := NewCoordinatedScheduleController(local, python)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := controller.OpenExpectedJob(ctx, expectedJobID)

	if err == nil {
		t.Fatal("expected uncertain guarded-open failure")
	}
	if !reflect.DeepEqual(python.setStates, []string{"CLOSED"}) ||
		!reflect.DeepEqual(local.setStates, []string{"CLOSED"}) {
		t.Fatalf("python states = %#v local states = %#v", python.setStates, local.setStates)
	}
	if !reflect.DeepEqual(python.setContextErrs, []error{nil}) {
		t.Fatalf("python close context errors = %#v; want [nil]", python.setContextErrs)
	}
	if !reflect.DeepEqual(local.setContextErrs, []error{nil}) {
		t.Fatalf("local close context errors = %#v; want [nil]", local.setContextErrs)
	}
}

func TestCoordinatedGuardedOpenLocalCloseGetsFreshContextAfterPythonCloseTimeout(t *testing.T) {
	previousTimeout := guardedScheduleCleanupTimeout
	guardedScheduleCleanupTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		guardedScheduleCleanupTimeout = previousTimeout
	})
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	local := &fakeScheduleController{}
	python := &fakeScheduleController{
		guardedErr:         errors.New("ambiguous Python response"),
		waitForSetDeadline: true,
	}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

	if err == nil {
		t.Fatal("expected guarded-open failure")
	}
	if !reflect.DeepEqual(python.setStates, []string{"CLOSED"}) ||
		!reflect.DeepEqual(local.setStates, []string{"CLOSED"}) {
		t.Fatalf("python states = %#v local states = %#v", python.setStates, local.setStates)
	}
	if !reflect.DeepEqual(python.setContextErrs, []error{nil}) {
		t.Fatalf("python close context errors = %#v; want [nil]", python.setContextErrs)
	}
	if !reflect.DeepEqual(python.setDeadlineErrs, []error{context.DeadlineExceeded}) {
		t.Fatalf("python close deadline errors = %#v; want deadline exceeded", python.setDeadlineErrs)
	}
	if !reflect.DeepEqual(local.setContextErrs, []error{nil}) {
		t.Fatalf("local close context errors = %#v; want [nil]", local.setContextErrs)
	}
}

func TestCoordinatedGuardedOpenKnownConflictDoesNotCloseSharedSchedule(t *testing.T) {
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	local := &fakeScheduleController{}
	python := &fakeScheduleController{guardedErr: ErrScheduleGuardMismatch}
	controller := NewCoordinatedScheduleController(local, python)

	_, err := controller.OpenExpectedJob(context.Background(), expectedJobID)

	if !errors.Is(err, ErrScheduleGuardMismatch) {
		t.Fatalf("error = %v; want ErrScheduleGuardMismatch", err)
	}
	if len(python.setStates) != 0 {
		t.Fatalf("python states = %#v; want no close", python.setStates)
	}
	if len(local.setStates) != 0 {
		t.Fatalf("local states = %#v; want no close", local.setStates)
	}
}

func TestGuardedScheduleRouteSanitizesInfrastructureError(t *testing.T) {
	const secret = "postgres://admin:password@database.internal:5432/videoprocess"
	expectedJobID := "11111111-1111-4111-8111-111111111111"
	controller := &fakeScheduleController{guardedErr: errors.New("dial " + secret)}
	server := NewServerWithOptions(nil, ServerOptions{
		AllowStubStore: true,
		Schedule:       controller,
	})
	req := httptest.NewRequest(
		http.MethodPost,
		"/internal/schedule/video/open?expected_job_id="+expectedJobID,
		nil,
	)
	rec := httptest.NewRecorder()

	server.Router().ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d body=%s", rec.Code, rec.Body.String())
	}
	if body := strings.TrimSpace(rec.Body.String()); body != `{"detail":"guarded_schedule_open_failed"}` {
		t.Fatalf("body = %q", body)
	}
	if strings.Contains(rec.Body.String(), secret) {
		t.Fatalf("response leaked infrastructure error: %s", rec.Body.String())
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
	name               string
	events             *[]string
	setStates          []string
	setRows            []store.VideoScheduleStatusRow
	setErr             error
	setContextErrs     []error
	setDeadlineErrs    []error
	waitForSetDeadline bool
	guardedJobIDs      []string
	guardedRows        []store.VideoScheduleStatusRow
	guardedErr         error
	statusRow          store.VideoScheduleStatusRow
	statusErr          error
}

func (f *fakeScheduleController) Status(context.Context) (store.VideoScheduleStatusRow, error) {
	if f.events != nil {
		*f.events = append(*f.events, f.name+".status")
	}
	return f.statusRow, f.statusErr
}

func (f *fakeScheduleController) SetState(ctx context.Context, state string) (store.VideoScheduleStatusRow, error) {
	f.setStates = append(f.setStates, state)
	f.setContextErrs = append(f.setContextErrs, ctx.Err())
	if f.events != nil {
		*f.events = append(*f.events, f.name+"."+state)
	}
	if f.waitForSetDeadline {
		<-ctx.Done()
		f.setDeadlineErrs = append(f.setDeadlineErrs, ctx.Err())
		return store.VideoScheduleStatusRow{}, ctx.Err()
	}
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

func (f *fakeScheduleController) OpenExpectedJob(
	_ context.Context,
	expectedJobID string,
) (store.VideoScheduleStatusRow, error) {
	f.guardedJobIDs = append(f.guardedJobIDs, expectedJobID)
	if f.events != nil {
		*f.events = append(*f.events, f.name+".guarded")
	}
	if f.guardedErr != nil {
		return store.VideoScheduleStatusRow{}, f.guardedErr
	}
	if len(f.guardedRows) == 0 {
		return store.VideoScheduleStatusRow{State: "OPEN", ReleasedJobs: 1}, nil
	}
	row := f.guardedRows[0]
	f.guardedRows = f.guardedRows[1:]
	return row, nil
}
