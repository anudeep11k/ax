// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/server"
	"github.com/google/ax/internal/store/memory"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// fakeClock is a manually advanced clock for active-time accounting.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// recordingReconciler keeps the status it is given, as the real reconciler
// does, and records the phase of every Reconcile call.
type recordingReconciler struct {
	mu     sync.Mutex
	phases []string
	err    error
}

func (r *recordingReconciler) Reconcile(ctx context.Context, task *v1alpha1.Task, workspaces ...*v1alpha1.Workspace) (*v1alpha1.Task, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.phases = append(r.phases, task.GetStatus().GetPhase())
	if r.err != nil {
		return task, r.err
	}
	return task, nil
}

func (r *recordingReconciler) ReconcileDelete(ctx context.Context, atespace, taskName string) error {
	return nil
}

func (r *recordingReconciler) lastPhase() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.phases) == 0 {
		return ""
	}
	return r.phases[len(r.phases)-1]
}

func (r *recordingReconciler) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func newBudgetServer(t *testing.T) (*server.Server, *fakeClock, *recordingReconciler) {
	t.Helper()
	clock := newFakeClock()
	rec := &recordingReconciler{}
	srv := server.NewServer(memory.NewStore(), server.Options{Reconciler: rec, Now: clock.Now})
	return srv, clock, rec
}

func createBudgetTask(t *testing.T, srv *server.Server, name, deadline string) {
	t.Helper()
	spec := &v1alpha1.TaskSpec{Image: "alpine"}
	if deadline != "" {
		spec.Budget = &v1alpha1.TaskBudget{ActiveDeadline: deadline}
	}
	if _, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: name},
		Spec:     spec,
	}}); err != nil {
		t.Fatalf("CreateTask(%s): %v", name, err)
	}
}

func getTask(t *testing.T, srv *server.Server, name string) *v1alpha1.Task {
	t.Helper()
	task, err := srv.GetTask(context.Background(), &v1alpha1.GetTaskRequest{Name: name})
	if err != nil {
		t.Fatalf("GetTask(%s): %v", name, err)
	}
	return task
}

func resume(t *testing.T, srv *server.Server, name string) {
	t.Helper()
	if _, err := srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: name}); err != nil {
		t.Fatalf("ResumeTask(%s): %v", name, err)
	}
}

func suspend(t *testing.T, srv *server.Server, name string) {
	t.Helper()
	if _, err := srv.SuspendTask(context.Background(), &v1alpha1.SuspendTaskRequest{Name: name}); err != nil {
		t.Fatalf("SuspendTask(%s): %v", name, err)
	}
}

func readyCondition(task *v1alpha1.Task) *v1alpha1.Condition {
	for _, c := range task.GetStatus().GetConditions() {
		if c.GetType() == "Ready" {
			return c
		}
	}
	return nil
}

func TestActiveTime_CountsOnlyRunningTime(t *testing.T) {
	srv, clock, _ := newBudgetServer(t)
	createBudgetTask(t, srv, "worker", "1h")

	// Time before the first resume does not count.
	clock.Advance(20 * time.Minute)
	resume(t, srv, "worker")
	if got := getTask(t, srv, "worker").GetStatus().GetRunningSince().AsTime(); !got.Equal(clock.Now()) {
		t.Fatalf("runningSince = %v, want %v", got, clock.Now())
	}

	// A first run of 10 minutes is banked on suspend.
	clock.Advance(10 * time.Minute)
	suspend(t, srv, "worker")
	st := getTask(t, srv, "worker").GetStatus()
	if st.GetRunningSince() != nil {
		t.Errorf("runningSince should be cleared while suspended, got %v", st.GetRunningSince())
	}
	if got := st.GetActiveDuration().AsDuration(); got != 10*time.Minute {
		t.Errorf("activeDuration after first run = %v, want 10m", got)
	}

	// Suspended time does not count.
	clock.Advance(3 * time.Hour)
	resume(t, srv, "worker")

	// Resuming a task that is already running keeps the current run's start.
	clock.Advance(2 * time.Minute)
	resume(t, srv, "worker")
	clock.Advance(3 * time.Minute)

	st = getTask(t, srv, "worker").GetStatus()
	if got := st.ActiveTime(clock.Now()); got != 15*time.Minute {
		t.Errorf("ActiveTime = %v, want 15m (10m banked + 5m current run)", got)
	}
}

func TestCreateTask_IgnoresClientSuppliedActiveTime(t *testing.T) {
	srv, clock, _ := newBudgetServer(t)
	_, err := srv.CreateTask(context.Background(), &v1alpha1.CreateTaskRequest{Task: &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "forged"},
		Spec:     &v1alpha1.TaskSpec{Budget: &v1alpha1.TaskBudget{ActiveDeadline: "1h"}},
		Status: &v1alpha1.TaskStatus{
			ActiveDuration: durationpb.New(-10 * time.Hour),
			RunningSince:   timestamppb.New(clock.Now().Add(time.Hour)),
		},
	}})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	st := getTask(t, srv, "forged").GetStatus()
	if st.GetActiveDuration() != nil || st.GetRunningSince() != nil {
		t.Errorf("expected client-supplied active time to be dropped, got activeDuration=%v runningSince=%v",
			st.GetActiveDuration(), st.GetRunningSince())
	}
}

func TestEnforceBudgets_StopsTaskPastItsDeadline(t *testing.T) {
	srv, clock, rec := newBudgetServer(t)
	ctx := context.Background()
	createBudgetTask(t, srv, "runaway", "30m")
	resume(t, srv, "runaway")

	clock.Advance(29 * time.Minute)
	if n, err := srv.EnforceBudgets(ctx); err != nil || n != 0 {
		t.Fatalf("EnforceBudgets before the deadline = %d, %v; want 0, nil", n, err)
	}

	clock.Advance(2 * time.Minute)
	n, err := srv.EnforceBudgets(ctx)
	if err != nil || n != 1 {
		t.Fatalf("EnforceBudgets after the deadline = %d, %v; want 1, nil", n, err)
	}
	if got := rec.lastPhase(); got != "Suspended" {
		t.Errorf("expected the actor to be suspended, last reconcile phase was %q", got)
	}

	task := getTask(t, srv, "runaway")
	st := task.GetStatus()
	if st.GetPhase() != "Failed" {
		t.Errorf("phase = %q, want Failed", st.GetPhase())
	}
	cond := readyCondition(task)
	if cond == nil || cond.GetStatus() != "False" || cond.GetReason() != v1alpha1.ReasonDeadlineExceeded {
		t.Errorf("Ready condition = %v, want False/%s", cond, v1alpha1.ReasonDeadlineExceeded)
	}
	if got := st.GetActiveDuration().AsDuration(); got != 31*time.Minute {
		t.Errorf("activeDuration = %v, want 31m", got)
	}
	if st.GetRunningSince() != nil {
		t.Errorf("runningSince should be cleared once stopped, got %v", st.GetRunningSince())
	}

	// A second sweep finds nothing left to stop.
	if n, err := srv.EnforceBudgets(ctx); err != nil || n != 0 {
		t.Errorf("second EnforceBudgets = %d, %v; want 0, nil", n, err)
	}
}

func TestEnforceBudgets_DeadlineSpansSuspendResume(t *testing.T) {
	srv, clock, _ := newBudgetServer(t)
	ctx := context.Background()
	createBudgetTask(t, srv, "episodic", "30m")

	resume(t, srv, "episodic")
	clock.Advance(20 * time.Minute)
	suspend(t, srv, "episodic")
	clock.Advance(24 * time.Hour)
	resume(t, srv, "episodic")

	clock.Advance(9 * time.Minute)
	if n, _ := srv.EnforceBudgets(ctx); n != 0 {
		t.Fatalf("stopped a task at 29m of a 30m budget")
	}
	clock.Advance(time.Minute)
	if n, _ := srv.EnforceBudgets(ctx); n != 1 {
		t.Fatalf("did not stop a task at 30m of a 30m budget")
	}
}

func TestEnforceBudgets_LeavesOtherTasksAlone(t *testing.T) {
	srv, clock, _ := newBudgetServer(t)
	ctx := context.Background()

	createBudgetTask(t, srv, "unbounded", "")
	resume(t, srv, "unbounded")
	createBudgetTask(t, srv, "within", "2h")
	resume(t, srv, "within")
	createBudgetTask(t, srv, "parked", "1m")
	resume(t, srv, "parked")
	clock.Advance(30 * time.Second)
	suspend(t, srv, "parked")

	clock.Advance(90 * time.Minute)
	if n, err := srv.EnforceBudgets(ctx); err != nil || n != 0 {
		t.Fatalf("EnforceBudgets = %d, %v; want 0, nil", n, err)
	}
	for name, want := range map[string]string{"unbounded": "Running", "within": "Running", "parked": "Suspended"} {
		if got := getTask(t, srv, name).GetStatus().GetPhase(); got != want {
			t.Errorf("%s: phase = %q, want %q", name, got, want)
		}
	}
}

func TestEnforceBudgets_RetriesWhenSuspendFails(t *testing.T) {
	srv, clock, rec := newBudgetServer(t)
	ctx := context.Background()
	createBudgetTask(t, srv, "stuck", "10m")
	resume(t, srv, "stuck")
	clock.Advance(11 * time.Minute)

	rec.setErr(errors.New("substrate unavailable"))
	if n, err := srv.EnforceBudgets(ctx); err != nil || n != 0 {
		t.Fatalf("EnforceBudgets with failing suspend = %d, %v; want 0, nil", n, err)
	}
	st := getTask(t, srv, "stuck").GetStatus()
	if st.GetPhase() != "Running" || st.GetRunningSince() == nil {
		t.Fatalf("a failed suspend must leave the task Running for the next sweep, got phase=%q runningSince=%v",
			st.GetPhase(), st.GetRunningSince())
	}

	rec.setErr(nil)
	if n, err := srv.EnforceBudgets(ctx); err != nil || n != 1 {
		t.Fatalf("EnforceBudgets after recovery = %d, %v; want 1, nil", n, err)
	}
}

func TestResumeTask_RefusesExhaustedBudget(t *testing.T) {
	srv, clock, _ := newBudgetServer(t)
	createBudgetTask(t, srv, "spent", "5m")
	resume(t, srv, "spent")
	clock.Advance(6 * time.Minute)
	if n, _ := srv.EnforceBudgets(context.Background()); n != 1 {
		t.Fatal("expected the task to be stopped")
	}

	_, err := srv.ResumeTask(context.Background(), &v1alpha1.ResumeTaskRequest{Name: "spent"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ResumeTask on an exhausted budget = %v, want FailedPrecondition", err)
	}
	if got := getTask(t, srv, "spent").GetStatus().GetPhase(); got != "Failed" {
		t.Errorf("a refused resume must not change the phase, got %q", got)
	}
}

func TestEnforceBudgets_WithoutReconciler(t *testing.T) {
	clock := newFakeClock()
	srv := server.NewServer(memory.NewStore(), server.Options{Now: clock.Now})
	createBudgetTask(t, srv, "local", "1m")
	resume(t, srv, "local")
	clock.Advance(2 * time.Minute)

	if n, err := srv.EnforceBudgets(context.Background()); err != nil || n != 1 {
		t.Fatalf("EnforceBudgets = %d, %v; want 1, nil", n, err)
	}
	if got := getTask(t, srv, "local").GetStatus().GetPhase(); got != "Failed" {
		t.Errorf("phase = %q, want Failed", got)
	}
}

func TestRunBudgetEnforcer_StopsOnContextCancel(t *testing.T) {
	srv, _, _ := newBudgetServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		srv.RunBudgetEnforcer(ctx, time.Millisecond)
		close(done)
	}()
	time.Sleep(5 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunBudgetEnforcer did not return after its context was cancelled")
	}

	// A zero interval disables enforcement and returns immediately.
	srv.RunBudgetEnforcer(context.Background(), 0)
}
