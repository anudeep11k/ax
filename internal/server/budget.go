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

package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/ax/internal/store"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// budgetListPageSize is how many tasks the enforcer reads per store call.
	budgetListPageSize = 500
	// budgetLockTimeout bounds how long the enforcer waits for a task's lock.
	// A task whose lock is busy (a resume or suspend in flight) is retried on
	// the next sweep instead of stalling the rest of the sweep.
	budgetLockTimeout = 5 * time.Second
)

// settleActiveClock carries the active-time accounting from prev into next and
// advances it for next's phase. A reconcile may rebuild the status, so the
// accounting is always settled after the new phase is known:
//
//   - entering Running starts the clock, unless a run is already in progress;
//   - leaving Running adds the finished run to active_duration and stops the clock.
func settleActiveClock(prev, next *v1alpha1.TaskStatus, now time.Time) {
	if next == nil {
		return
	}
	next.ActiveDuration = prev.GetActiveDuration()
	next.RunningSince = prev.GetRunningSince()

	if next.GetPhase() == "Running" {
		if next.RunningSince == nil {
			next.RunningSince = timestamppb.New(now)
		}
		return
	}
	if since := next.GetRunningSince(); since != nil {
		run := now.Sub(since.AsTime())
		if run < 0 {
			run = 0
		}
		next.ActiveDuration = durationpb.New(next.GetActiveDuration().AsDuration() + run)
		next.RunningSince = nil
	}
}

// budgetExhausted reports whether the task has used its whole active deadline
// as of now. Tasks without a deadline never exhaust it.
func budgetExhausted(task *v1alpha1.Task, now time.Time) (bool, time.Duration) {
	deadline, err := task.GetSpec().ActiveDeadline()
	if err != nil || deadline <= 0 {
		return false, 0
	}
	return task.GetStatus().ActiveTime(now) >= deadline, deadline
}

// RunBudgetEnforcer sweeps every interval until ctx is done, stopping tasks
// that have used their active deadline. It is a no-op when interval <= 0.
func (s *Server) RunBudgetEnforcer(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := s.EnforceBudgets(ctx); err != nil && ctx.Err() == nil {
				slog.Error("budget enforcement sweep failed", "error", err)
			}
		}
	}
}

// EnforceBudgets runs one sweep over all tasks and stops every Running task
// whose active time has reached spec.budget.activeDeadline. It returns how
// many tasks it stopped. A task that cannot be stopped this sweep is logged
// and retried on the next one.
func (s *Server) EnforceBudgets(ctx context.Context) (int, error) {
	var expired []*v1alpha1.Task
	now := s.now()
	for offset := int64(0); ; offset += budgetListPageSize {
		page, err := s.store.ListTasks(ctx, "", budgetListPageSize, offset)
		if err != nil {
			return 0, fmt.Errorf("listing tasks: %w", err)
		}
		for _, t := range page {
			if t.GetStatus().GetPhase() != "Running" {
				continue
			}
			if over, _ := budgetExhausted(t, now); over {
				expired = append(expired, t)
			}
		}
		if len(page) < budgetListPageSize {
			break
		}
	}

	stopped := 0
	for _, t := range expired {
		if ctx.Err() != nil {
			return stopped, ctx.Err()
		}
		atespace, name := t.GetMetadata().GetAtespace(), t.GetMetadata().GetName()
		ok, err := s.expireTask(ctx, atespace, name)
		if err != nil {
			slog.Warn("could not stop task that exceeded its active deadline; retrying next sweep",
				"atespace", atespace, "task", name, "error", err)
			continue
		}
		if ok {
			stopped++
		}
	}
	return stopped, nil
}

// expireTask suspends a task that has used its active deadline and records it
// as Failed with reason DeadlineExceeded. Suspending rather than deleting keeps
// the workspace for inspection. It re-checks the task under its lock and
// returns false if the task no longer needs stopping.
func (s *Server) expireTask(ctx context.Context, atespace, name string) (bool, error) {
	lockCtx, cancel := context.WithTimeout(ctx, budgetLockTimeout)
	unlock, err := s.locker.Lock(lockCtx, "task", atespace, name)
	cancel()
	if err != nil {
		return false, fmt.Errorf("locking task: %w", err)
	}
	defer unlock()

	task, err := s.store.GetTask(ctx, atespace, name)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("getting task: %w", err)
	}
	now := s.now()
	over, deadline := budgetExhausted(task, now)
	if task.GetStatus().GetPhase() != "Running" || !over {
		return false, nil
	}
	prev := task.GetStatus()
	used := prev.ActiveTime(now)

	task.Status = cloneStatus(prev)
	task.Status.Phase = "Suspended"
	if s.reconciler != nil {
		// A suspend that is cancelled partway can lose the workspace snapshot
		// (see google/ax#453), so server shutdown must not interrupt it.
		reconciled, err := s.reconciler.Reconcile(context.WithoutCancel(ctx), task, s.fetchWorkspaces(ctx, atespace, task)...)
		if err != nil {
			return false, fmt.Errorf("suspending task on substrate: %w", err)
		}
		task.Status = reconciled.Status
	}
	if task.Status == nil {
		task.Status = &v1alpha1.TaskStatus{}
	}
	task.Status.Phase = "Failed"
	setStatusCondition(task.Status, "Ready", "False", v1alpha1.ReasonDeadlineExceeded,
		fmt.Sprintf("Task ran for %s, reaching its active deadline of %s; it was suspended and its workspace kept", used.Round(time.Second), deadline), now)
	settleActiveClock(prev, task.Status, now)

	if err := s.store.UpdateTaskStatus(ctx, atespace, name, task.Status); err != nil {
		return false, fmt.Errorf("updating task status: %w", err)
	}
	slog.Info("stopped task that exceeded its active deadline",
		"atespace", atespace, "task", name, "deadline", deadline, "activeTime", used)
	return true, nil
}

// cloneStatus returns a deep copy of st, or an empty status when st is nil.
func cloneStatus(st *v1alpha1.TaskStatus) *v1alpha1.TaskStatus {
	if st == nil {
		return &v1alpha1.TaskStatus{}
	}
	return proto.Clone(st).(*v1alpha1.TaskStatus)
}

// setStatusCondition sets or adds the condition of the given type.
func setStatusCondition(st *v1alpha1.TaskStatus, condType, status, reason, message string, t time.Time) {
	ts := timestamppb.New(t)
	for _, c := range st.Conditions {
		if c.GetType() == condType {
			c.Status = status
			c.Reason = reason
			c.Message = message
			c.UpdateTime = ts
			return
		}
	}
	st.Conditions = append(st.Conditions, &v1alpha1.Condition{
		Type:       condType,
		Status:     status,
		Reason:     reason,
		Message:    message,
		UpdateTime: ts,
	})
}
