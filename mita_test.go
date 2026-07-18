package mita

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestNew verifies TaskManager creation with various options
func TestNew(t *testing.T) {
	tests := []struct {
		name string
		opts []Option
		want func(*TaskManager) bool
	}{
		{
			name: "default configuration",
			opts: nil,
			want: func(tm *TaskManager) bool {
				return tm != nil && !tm.IsStarted() && tm.maxConcurrent == 0 &&
					!tm.allowOverlapping && tm.shutdownTimeout == DefaultShutdownTimeout
			},
		},
		{
			name: "with max concurrent",
			opts: []Option{WithMaxConcurrent(5)},
			want: func(tm *TaskManager) bool {
				return tm.maxConcurrent == 5 && tm.semaphore != nil
			},
		},
		{
			name: "with negative max concurrent",
			opts: []Option{WithMaxConcurrent(-1)},
			want: func(tm *TaskManager) bool {
				return tm.maxConcurrent == 0 && tm.semaphore == nil
			},
		},
		{
			name: "with allow overlapping",
			opts: []Option{WithAllowOverlapping(true)},
			want: func(tm *TaskManager) bool {
				return tm.allowOverlapping
			},
		},
		{
			name: "with shutdown timeout",
			opts: []Option{WithShutdownTimeout(5 * time.Second)},
			want: func(tm *TaskManager) bool {
				return tm.shutdownTimeout == 5*time.Second
			},
		},
		{
			name: "with non-positive shutdown timeout ignored",
			opts: []Option{WithShutdownTimeout(0)},
			want: func(tm *TaskManager) bool {
				return tm.shutdownTimeout == DefaultShutdownTimeout
			},
		},
		{
			name: "with context value",
			opts: []Option{WithContextValue("key1", "value1")},
			want: func(tm *TaskManager) bool {
				return tm.GetContextValue("key1") == "value1"
			},
		},
		{
			name: "with empty context key",
			opts: []Option{WithContextValue("", "value1")},
			want: func(tm *TaskManager) bool {
				return tm.GetContextValue("") == nil
			},
		},
		{
			name: "with custom logger",
			opts: []Option{WithLogger(log.New(os.Stdout, "[TEST] ", log.LstdFlags))},
			want: func(tm *TaskManager) bool {
				return tm.logger != nil
			},
		},
		{
			name: "with nil logger",
			opts: []Option{WithLogger(nil)},
			want: func(tm *TaskManager) bool {
				return tm.logger == log.Default()
			},
		},
		{
			name: "with location",
			opts: []Option{WithLocation(time.UTC)},
			want: func(tm *TaskManager) bool {
				return tm.cron != nil && tm.location == time.UTC
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := New(tt.opts...)
			defer tm.Stop()

			if !tt.want(tm) {
				t.Errorf("TaskManager validation failed for test: %s", tt.name)
			}
		})
	}
}

// TestAddTask verifies task addition with various scenarios
func TestAddTask(t *testing.T) {
	tests := []struct {
		name      string
		taskName  string
		schedule  Schedule
		task      Task
		wantError bool
	}{
		{
			name:      "valid task",
			taskName:  "test-task",
			schedule:  Every().Minute(),
			task:      func(ctx context.Context) error { return nil },
			wantError: false,
		},
		{
			name:      "empty task name",
			taskName:  "",
			schedule:  Every().Minute(),
			task:      func(ctx context.Context) error { return nil },
			wantError: true,
		},
		{
			name:      "nil schedule",
			taskName:  "test-task",
			schedule:  nil,
			task:      func(ctx context.Context) error { return nil },
			wantError: true,
		},
		{
			name:      "nil task function",
			taskName:  "test-task",
			schedule:  Every().Minute(),
			task:      nil,
			wantError: true,
		},
		{
			name:      "invalid cron expression",
			taskName:  "test-task",
			schedule:  Cron("invalid cron"),
			task:      func(ctx context.Context) error { return nil },
			wantError: true,
		},
		{
			name:      "standard 5-field cron expression",
			taskName:  "test-task",
			schedule:  Cron("*/5 * * * *"),
			task:      func(ctx context.Context) error { return nil },
			wantError: false,
		},
		{
			name:      "descriptor expression",
			taskName:  "test-task",
			schedule:  Cron("@every 90s"),
			task:      func(ctx context.Context) error { return nil },
			wantError: false,
		},
		{
			name:      "builder with invalid interval",
			taskName:  "test-task",
			schedule:  Every().Seconds(90),
			task:      func(ctx context.Context) error { return nil },
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := New()
			defer tm.Stop()

			err := tm.AddTask(tt.taskName, tt.schedule, tt.task)
			if (err != nil) != tt.wantError {
				t.Errorf("AddTask() error = %v, wantError %v", err, tt.wantError)
			}

			if !tt.wantError {
				info, err := tm.GetTask(tt.taskName)
				if err != nil {
					t.Errorf("GetTask() error = %v", err)
				}
				if info.Name != tt.taskName {
					t.Errorf("Task name = %v, want %v", info.Name, tt.taskName)
				}
			}
		})
	}
}

// TestFiveFieldCronNormalization verifies 5-field expressions gain a seconds column.
func TestFiveFieldCronNormalization(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("five-field", Cron("*/5 * * * *"), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	info, err := tm.GetTask("five-field")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.Schedule != "0 */5 * * * *" {
		t.Fatalf("Schedule = %q, want %q", info.Schedule, "0 */5 * * * *")
	}
}

// TestAddDuplicateTask verifies duplicate task prevention
func TestAddDuplicateTask(t *testing.T) {
	tm := New()
	defer tm.Stop()

	task := func(ctx context.Context) error { return nil }
	schedule := Every().Minute()

	err := tm.AddTask("duplicate", schedule, task)
	if err != nil {
		t.Fatalf("First AddTask() failed: %v", err)
	}

	err = tm.AddTask("duplicate", schedule, task)
	if !errors.Is(err, ErrTaskExists) {
		t.Errorf("AddTask() error = %v, want ErrTaskExists", err)
	}
}

// TestTaskExecution verifies tasks execute correctly
func TestTaskExecution(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	// Task should run every second
	err := tm.AddTask("counter", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for at least 2 executions
	time.Sleep(2500 * time.Millisecond)

	count := counter.Load()
	if count < 2 {
		t.Errorf("Task executed %d times, expected at least 2", count)
	}
}

// TestTaskExecutionWithError verifies error handling
func TestTaskExecutionWithError(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	expectedErr := errors.New("task failed")
	task := func(ctx context.Context) error {
		return expectedErr
	}

	err := tm.AddTask("error-task", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for execution
	time.Sleep(1500 * time.Millisecond)

	info, err := tm.GetTask("error-task")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}

	if info.ErrorCount == 0 {
		t.Error("Expected ErrorCount > 0, got 0")
	}

	if info.LastError != expectedErr.Error() {
		t.Errorf("LastError = %v, want %v", info.LastError, expectedErr.Error())
	}
}

// TestRunTaskNow verifies manual task execution
func TestRunTaskNow(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var executed atomic.Bool
	task := func(ctx context.Context) error {
		executed.Store(true)
		return nil
	}

	// Add task with a schedule that won't trigger during test
	err := tm.AddTask("manual", Cron("0 0 0 1 1 *"), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	done, err := tm.RunTaskNow("manual")
	if err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("task returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("task did not complete")
	}

	if !executed.Load() {
		t.Error("Task was not executed")
	}
}

// TestRunTaskNowDoneChannel verifies the result channel reports task errors.
func TestRunTaskNowDoneChannel(t *testing.T) {
	tm := New()
	defer tm.Stop()

	expectedErr := errors.New("task failed")
	if err := tm.AddTask("channel-err", Every().Minute(), func(ctx context.Context) error {
		return expectedErr
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	done, err := tm.RunTaskNow("channel-err")
	if err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, expectedErr) {
			t.Fatalf("done channel error = %v, want %v", err, expectedErr)
		}
	case <-time.After(time.Second):
		t.Fatal("done channel did not receive a result")
	}
}

func TestRunTaskNowAndWait(t *testing.T) {
	tm := New()
	defer tm.Stop()

	var executed atomic.Bool
	if err := tm.AddTask("manual-wait", Every().Minute(), func(ctx context.Context) error {
		executed.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if err := tm.RunTaskNowAndWait(context.Background(), "manual-wait"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}
	if !executed.Load() {
		t.Fatal("task did not execute")
	}

	info, err := tm.GetTask("manual-wait")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.RunCount != 1 || info.Running() || info.RunningCount != 0 {
		t.Fatalf("task state = RunCount=%d Running=%v RunningCount=%d, want 1/false/0", info.RunCount, info.Running(), info.RunningCount)
	}
}

func TestRunTaskNowAndWaitWorksBeforeStart(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("pre-start", Every().Minute(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if err := tm.RunTaskNowAndWait(context.Background(), "pre-start"); err != nil {
		t.Fatalf("RunTaskNowAndWait() before Start failed: %v", err)
	}
}

func TestRunTaskNowAndWaitReturnsTaskError(t *testing.T) {
	expectedErr := errors.New("task failed")
	completed := make(chan TaskCompleteEvent, 1)
	tm := New(WithOnTaskComplete(func(event TaskCompleteEvent) {
		completed <- event
	}))
	defer tm.Stop()

	if err := tm.AddTask("error-wait", Every().Minute(), func(ctx context.Context) error {
		return expectedErr
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	err := tm.RunTaskNowAndWait(context.Background(), "error-wait")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("RunTaskNowAndWait() error = %v, want %v", err, expectedErr)
	}

	select {
	case event := <-completed:
		if !errors.Is(event.Error, expectedErr) {
			t.Fatalf("complete event error = %v, want %v", event.Error, expectedErr)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskComplete was not called")
	}
}

func TestRunTaskNowAndWaitTaskPanic(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("panic-wait", Every().Minute(), func(ctx context.Context) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	err := tm.RunTaskNowAndWait(context.Background(), "panic-wait")
	if err == nil || !strings.Contains(err.Error(), "task panic: boom") {
		t.Fatalf("RunTaskNowAndWait() error = %v, want task panic", err)
	}

	info, err := tm.GetTask("panic-wait")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.ErrorCount != 1 || !strings.Contains(info.LastError, "task panic: boom") {
		t.Fatalf("panic failure not recorded: ErrorCount=%d LastError=%q", info.ErrorCount, info.LastError)
	}
}

func TestRunTaskNowAndWaitContextAlreadyCanceled(t *testing.T) {
	tm := New()
	defer tm.Stop()

	var executed atomic.Bool
	if err := tm.AddTask("canceled", Every().Minute(), func(ctx context.Context) error {
		executed.Store(true)
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tm.RunTaskNowAndWait(ctx, "canceled")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunTaskNowAndWait() error = %v, want context.Canceled", err)
	}
	if executed.Load() {
		t.Fatal("task should not execute when context is already canceled")
	}
	info, err := tm.GetTask("canceled")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.RunCount != 0 {
		t.Fatalf("RunCount = %d, want 0", info.RunCount)
	}
}

func TestRunTaskNowAndWaitContextCancelsTask(t *testing.T) {
	tm := New()
	defer tm.Stop()

	started := make(chan struct{})
	if err := tm.AddTask("cancel-during", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- tm.RunTaskNowAndWait(ctx, "cancel-during")
	}()
	<-started
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunTaskNowAndWait() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunTaskNowAndWait() did not return after task observed cancellation")
	}
}

func TestRunTaskNowAndWaitAdmissionErrors(t *testing.T) {
	t.Run("nil context", func(t *testing.T) {
		tm := New()
		defer tm.Stop()
		err := tm.RunTaskNowAndWait(nil, "task")
		if !errors.Is(err, ErrNilContext) {
			t.Fatalf("RunTaskNowAndWait() error = %v, want ErrNilContext", err)
		}
	})

	t.Run("already running", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		tm := New()
		defer tm.Stop()
		if err := tm.AddTask("running", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}
		if _, err := tm.RunTaskNow("running"); err != nil {
			t.Fatalf("RunTaskNow() failed: %v", err)
		}
		<-started

		err := tm.RunTaskNowAndWait(context.Background(), "running")
		if !errors.Is(err, ErrTaskRunning) {
			t.Fatalf("RunTaskNowAndWait() error = %v, want ErrTaskRunning", err)
		}
		close(release)
	})

	t.Run("max concurrency reached", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		tm := New(WithMaxConcurrent(1))
		defer tm.Stop()
		if err := tm.AddTask("first", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("AddTask(first) failed: %v", err)
		}
		if err := tm.AddTask("second", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("AddTask(second) failed: %v", err)
		}
		if _, err := tm.RunTaskNow("first"); err != nil {
			t.Fatalf("RunTaskNow(first) failed: %v", err)
		}
		<-started

		err := tm.RunTaskNowAndWait(context.Background(), "second")
		if !errors.Is(err, ErrMaxConcurrencyReached) {
			t.Fatalf("RunTaskNowAndWait(second) error = %v, want ErrMaxConcurrencyReached", err)
		}
		close(release)
	})
}

// TestManualRunAllowedOnDisabledTask verifies that disabling a task pauses its
// schedule but keeps explicit manual triggers working.
func TestManualRunAllowedOnDisabledTask(t *testing.T) {
	tm := New()
	defer tm.Stop()

	var executed atomic.Int32
	if err := tm.AddTask("disabled", Every().Minute(), func(ctx context.Context) error {
		executed.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if err := tm.DisableTask("disabled"); err != nil {
		t.Fatalf("DisableTask() failed: %v", err)
	}

	if err := tm.RunTaskNowAndWait(context.Background(), "disabled"); err != nil {
		t.Fatalf("RunTaskNowAndWait() on disabled task failed: %v", err)
	}

	done, err := tm.RunTaskNow("disabled")
	if err != nil {
		t.Fatalf("RunTaskNow() on disabled task failed: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("manual run returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("manual run did not complete")
	}

	if executed.Load() != 2 {
		t.Fatalf("executed %d times, want 2", executed.Load())
	}
}

func TestRunTaskNowAndWaitHookPanicsDoNotPropagate(t *testing.T) {
	tm := New(
		WithOnTaskStart(func(event TaskStartEvent) {
			panic("start hook failed")
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			panic("complete hook failed")
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("hook-panic-wait", Every().Minute(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if err := tm.RunTaskNowAndWait(context.Background(), "hook-panic-wait"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}
}

func TestRunTaskNowAndWaitStopCancelsTask(t *testing.T) {
	tm := New(WithShutdownTimeout(100 * time.Millisecond))
	started := make(chan struct{})
	errCh := make(chan error, 1)

	if err := tm.AddTask("stop-wait", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	go func() {
		errCh <- tm.RunTaskNowAndWait(context.Background(), "stop-wait")
	}()
	<-started
	if err := tm.Stop(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want context.DeadlineExceeded", err)
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunTaskNowAndWait() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("RunTaskNowAndWait() did not return after Stop")
	}
}

// TestRunTaskNowErrors verifies RunTaskNow error cases
func TestRunTaskNowErrors(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*TaskManager)
		taskName  string
		wantError bool
	}{
		{
			name:      "empty task name",
			setup:     func(tm *TaskManager) {},
			taskName:  "",
			wantError: true,
		},
		{
			name:      "non-existent task",
			setup:     func(tm *TaskManager) {},
			taskName:  "non-existent",
			wantError: true,
		},
		{
			name: "disabled task is allowed",
			setup: func(tm *TaskManager) {
				tm.AddTask("disabled", Every().Minute(), func(ctx context.Context) error { return nil })
				tm.DisableTask("disabled")
			},
			taskName:  "disabled",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := New()
			defer tm.Stop()
			tt.setup(tm)

			_, err := tm.RunTaskNow(tt.taskName)
			if (err != nil) != tt.wantError {
				t.Errorf("RunTaskNow() error = %v, wantError %v", err, tt.wantError)
			}
		})
	}
}

func TestRunTaskNowReturnsSentinelErrors(t *testing.T) {
	t.Run("already running", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		tm := New()
		defer tm.Stop()
		if err := tm.AddTask("running", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}
		if _, err := tm.RunTaskNow("running"); err != nil {
			t.Fatalf("RunTaskNow() failed: %v", err)
		}
		<-started

		_, err := tm.RunTaskNow("running")
		if !errors.Is(err, ErrTaskRunning) {
			t.Fatalf("RunTaskNow() error = %v, want ErrTaskRunning", err)
		}
		close(release)
	})

	t.Run("max concurrency reached", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		tm := New(WithMaxConcurrent(1))
		defer tm.Stop()
		if err := tm.AddTask("first", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("AddTask(first) failed: %v", err)
		}
		if err := tm.AddTask("second", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("AddTask(second) failed: %v", err)
		}
		if _, err := tm.RunTaskNow("first"); err != nil {
			t.Fatalf("RunTaskNow(first) failed: %v", err)
		}
		<-started

		_, err := tm.RunTaskNow("second")
		if !errors.Is(err, ErrMaxConcurrencyReached) {
			t.Fatalf("RunTaskNow(second) error = %v, want ErrMaxConcurrencyReached", err)
		}
		info, err := tm.GetTask("second")
		if err != nil {
			t.Fatalf("GetTask(second) failed: %v", err)
		}
		if info.RunCount != 0 {
			t.Fatalf("second RunCount = %d, want 0", info.RunCount)
		}
		close(release)
	})
}

func TestRunTaskNowSkipDoesNotFireStartCompleteHooks(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var skippedStarts atomic.Int32
	var skippedCompletes atomic.Int32

	tm := New(
		WithMaxConcurrent(1),
		WithOnTaskStart(func(event TaskStartEvent) {
			if event.TaskName == "skipped" {
				skippedStarts.Add(1)
			}
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			if event.TaskName == "skipped" {
				skippedCompletes.Add(1)
			}
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("holder", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("AddTask(holder) failed: %v", err)
	}
	if err := tm.AddTask("skipped", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask(skipped) failed: %v", err)
	}
	if _, err := tm.RunTaskNow("holder"); err != nil {
		t.Fatalf("RunTaskNow(holder) failed: %v", err)
	}
	<-started

	_, err := tm.RunTaskNow("skipped")
	if !errors.Is(err, ErrMaxConcurrencyReached) {
		t.Fatalf("RunTaskNow(skipped) error = %v, want ErrMaxConcurrencyReached", err)
	}
	close(release)

	if skippedStarts.Load() != 0 || skippedCompletes.Load() != 0 {
		t.Fatalf("skipped task hooks fired: starts=%d completes=%d", skippedStarts.Load(), skippedCompletes.Load())
	}
	info, err := tm.GetTask("skipped")
	if err != nil {
		t.Fatalf("GetTask(skipped) failed: %v", err)
	}
	if info.RunCount != 0 || info.Running() || info.RunningCount != 0 {
		t.Fatalf("skipped task state = RunCount=%d Running=%v RunningCount=%d", info.RunCount, info.Running(), info.RunningCount)
	}
}

// TestOnTaskSkipHook verifies the skip hook fires with the skip reason.
func TestOnTaskSkipHook(t *testing.T) {
	skips := make(chan TaskSkipEvent, 4)
	started := make(chan struct{})
	release := make(chan struct{})

	tm := New(WithOnTaskSkip(func(event TaskSkipEvent) {
		skips <- event
	}))
	defer tm.Stop()

	if err := tm.AddTask("busy", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("busy"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}
	<-started

	if _, err := tm.RunTaskNow("busy"); !errors.Is(err, ErrTaskRunning) {
		t.Fatalf("RunTaskNow() error = %v, want ErrTaskRunning", err)
	}

	select {
	case event := <-skips:
		if event.TaskName != "busy" || event.Trigger != TriggerManual || !errors.Is(event.Reason, ErrTaskRunning) {
			t.Fatalf("unexpected skip event: %+v", event)
		}
		if event.SkippedAt.IsZero() {
			t.Fatal("skip event has zero SkippedAt")
		}
	default:
		t.Fatal("OnTaskSkip was not called")
	}
	close(release)
}

// TestDisableEnableTask verifies task enable/disable functionality
func TestDisableEnableTask(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	err := tm.AddTask("toggle", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Let it run once
	time.Sleep(1200 * time.Millisecond)
	firstCount := counter.Load()

	// Disable task
	err = tm.DisableTask("toggle")
	if err != nil {
		t.Fatalf("DisableTask() failed: %v", err)
	}

	// Wait and verify it doesn't run
	time.Sleep(1500 * time.Millisecond)
	if counter.Load() != firstCount {
		t.Error("Task executed while disabled")
	}

	// Re-enable task
	err = tm.EnableTask("toggle")
	if err != nil {
		t.Fatalf("EnableTask() failed: %v", err)
	}

	// Verify it runs again
	time.Sleep(1200 * time.Millisecond)
	if counter.Load() == firstCount {
		t.Error("Task did not execute after being re-enabled")
	}
}

// TestRemoveTask verifies task removal
func TestRemoveTask(t *testing.T) {
	tm := New()
	defer tm.Stop()

	task := func(ctx context.Context) error { return nil }
	err := tm.AddTask("removable", Every().Minute(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	err = tm.RemoveTask("removable")
	if err != nil {
		t.Fatalf("RemoveTask() failed: %v", err)
	}

	_, err = tm.GetTask("removable")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("GetTask() error = %v, want ErrTaskNotFound", err)
	}

	// Test removing non-existent task
	err = tm.RemoveTask("non-existent")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("RemoveTask() error = %v, want ErrTaskNotFound", err)
	}

	// Test removing with empty name
	err = tm.RemoveTask("")
	if err == nil {
		t.Error("Expected error removing task with empty name, got nil")
	}
}

// TestUpdateSchedule verifies schedule updates preserve task state.
func TestUpdateSchedule(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("reschedule", Cron("0 0 0 1 1 *"), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if err := tm.RunTaskNowAndWait(context.Background(), "reschedule"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}

	if err := tm.UpdateSchedule("reschedule", Every().Seconds(5)); err != nil {
		t.Fatalf("UpdateSchedule() failed: %v", err)
	}

	info, err := tm.GetTask("reschedule")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.Schedule != "*/5 * * * * *" {
		t.Fatalf("Schedule = %q, want %q", info.Schedule, "*/5 * * * * *")
	}
	if info.RunCount != 1 {
		t.Fatalf("RunCount = %d, want 1 (statistics must be preserved)", info.RunCount)
	}

	t.Run("invalid schedule leaves task unchanged", func(t *testing.T) {
		if err := tm.UpdateSchedule("reschedule", Cron("garbage")); err == nil {
			t.Fatal("UpdateSchedule() accepted an invalid schedule")
		}
		if err := tm.UpdateSchedule("reschedule", Every().Seconds(90)); err == nil {
			t.Fatal("UpdateSchedule() accepted an invalid builder")
		}
		info, err := tm.GetTask("reschedule")
		if err != nil {
			t.Fatalf("GetTask() failed: %v", err)
		}
		if info.Schedule != "*/5 * * * * *" {
			t.Fatalf("Schedule = %q, want unchanged %q", info.Schedule, "*/5 * * * * *")
		}
	})

	t.Run("unknown task", func(t *testing.T) {
		if err := tm.UpdateSchedule("nope", Every().Minute()); !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("UpdateSchedule() error = %v, want ErrTaskNotFound", err)
		}
	})

	t.Run("nil schedule", func(t *testing.T) {
		if err := tm.UpdateSchedule("reschedule", nil); err == nil {
			t.Fatal("UpdateSchedule() accepted a nil schedule")
		}
	})
}

// TestUpdateScheduleTakesEffect verifies the new schedule actually drives execution.
func TestUpdateScheduleTakesEffect(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var counter atomic.Int32
	// Far-future schedule: never fires during the test.
	if err := tm.AddTask("dormant", Cron("0 0 0 1 1 *"), func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if err := tm.UpdateSchedule("dormant", Every().Second()); err != nil {
		t.Fatalf("UpdateSchedule() failed: %v", err)
	}

	time.Sleep(2500 * time.Millisecond)
	if counter.Load() < 2 {
		t.Fatalf("task executed %d times after reschedule, expected at least 2", counter.Load())
	}
}

// TestListTasks verifies task listing
func TestListTasks(t *testing.T) {
	tm := New()
	defer tm.Stop()

	task := func(ctx context.Context) error { return nil }

	// Added out of order to verify sorting.
	names := []string{"task3", "task1", "task2"}
	for _, name := range names {
		err := tm.AddTask(name, Every().Minute(), task)
		if err != nil {
			t.Fatalf("AddTask() failed for %s: %v", name, err)
		}
	}

	tasks := tm.ListTasks()
	if len(tasks) != len(names) {
		t.Fatalf("ListTasks() returned %d tasks, want %d", len(tasks), len(names))
	}

	// ListTasks returns tasks sorted by name.
	for i, want := range []string{"task1", "task2", "task3"} {
		if tasks[i].Name != want {
			t.Errorf("tasks[%d].Name = %s, want %s", i, tasks[i].Name, want)
		}
	}
}

// TestMaxConcurrent verifies concurrency limiting
func TestMaxConcurrent(t *testing.T) {
	maxConcurrent := 2
	tm := New(WithMaxConcurrent(maxConcurrent))
	tm.Start()
	defer tm.Stop()

	var concurrentCount atomic.Int32
	var maxObserved atomic.Int32

	task := func(ctx context.Context) error {
		current := concurrentCount.Add(1)

		// Track maximum concurrent executions
		for {
			max := maxObserved.Load()
			if current <= max || maxObserved.CompareAndSwap(max, current) {
				break
			}
		}

		time.Sleep(500 * time.Millisecond)
		concurrentCount.Add(-1)
		return nil
	}

	// Add multiple tasks that will trigger simultaneously
	for i := 0; i < 5; i++ {
		err := tm.AddTask(fmt.Sprintf("concurrent-%d", i), Every().Second(), task)
		if err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}
	}

	// Wait for executions
	time.Sleep(2 * time.Second)

	max := maxObserved.Load()
	if max > int32(maxConcurrent) {
		t.Errorf("Max concurrent tasks = %d, want <= %d", max, maxConcurrent)
	}
}

func TestScheduledTaskSkipsWhenMaxConcurrentReached(t *testing.T) {
	holderStarted := make(chan struct{})
	releaseHolder := make(chan struct{})
	var scheduledStarts atomic.Int32
	var scheduledCompletes atomic.Int32
	var scheduledSkips atomic.Int32

	tm := New(
		WithMaxConcurrent(1),
		WithOnTaskStart(func(event TaskStartEvent) {
			if event.TaskName == "scheduled-skip" {
				scheduledStarts.Add(1)
			}
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			if event.TaskName == "scheduled-skip" {
				scheduledCompletes.Add(1)
			}
		}),
		WithOnTaskSkip(func(event TaskSkipEvent) {
			if event.TaskName == "scheduled-skip" && errors.Is(event.Reason, ErrMaxConcurrencyReached) {
				scheduledSkips.Add(1)
			}
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("holder", Cron("0 0 0 1 1 *"), func(ctx context.Context) error {
		close(holderStarted)
		<-releaseHolder
		return nil
	}); err != nil {
		t.Fatalf("AddTask(holder) failed: %v", err)
	}
	if err := tm.AddTask("scheduled-skip", Every().Second(), func(ctx context.Context) error {
		return nil
	}); err != nil {
		t.Fatalf("AddTask(scheduled-skip) failed: %v", err)
	}
	if _, err := tm.RunTaskNow("holder"); err != nil {
		t.Fatalf("RunTaskNow(holder) failed: %v", err)
	}
	<-holderStarted

	tm.Start()
	time.Sleep(1200 * time.Millisecond)

	info, err := tm.GetTask("scheduled-skip")
	if err != nil {
		t.Fatalf("GetTask(scheduled-skip) failed: %v", err)
	}
	if info.RunCount != 0 || info.Running() || info.RunningCount != 0 {
		t.Fatalf("scheduled skipped task state = RunCount=%d Running=%v RunningCount=%d", info.RunCount, info.Running(), info.RunningCount)
	}
	if scheduledStarts.Load() != 0 || scheduledCompletes.Load() != 0 {
		t.Fatalf("scheduled skipped hooks fired: starts=%d completes=%d", scheduledStarts.Load(), scheduledCompletes.Load())
	}
	if scheduledSkips.Load() == 0 {
		t.Fatal("OnTaskSkip was not called for scheduled skip")
	}
	close(releaseHolder)
}

// TestOverlapPrevention verifies overlap prevention
func TestOverlapPrevention(t *testing.T) {
	tm := New(WithAllowOverlapping(false))
	tm.Start()
	defer tm.Stop()

	var executing atomic.Bool
	var overlapDetected atomic.Bool

	task := func(ctx context.Context) error {
		if !executing.CompareAndSwap(false, true) {
			overlapDetected.Store(true)
			return nil
		}
		time.Sleep(1500 * time.Millisecond)
		executing.Store(false)
		return nil
	}

	err := tm.AddTask("no-overlap", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for multiple potential executions
	time.Sleep(3 * time.Second)

	if overlapDetected.Load() {
		t.Error("Task overlap detected when overlapping is disabled")
	}
}

// TestAllowOverlapping verifies overlapping execution when enabled
func TestAllowOverlapping(t *testing.T) {
	tm := New(WithAllowOverlapping(true))
	tm.Start()
	defer tm.Stop()

	var concurrentCount atomic.Int32
	var hadConcurrent atomic.Bool

	task := func(ctx context.Context) error {
		count := concurrentCount.Add(1)
		if count > 1 {
			hadConcurrent.Store(true)
		}
		time.Sleep(1500 * time.Millisecond)
		concurrentCount.Add(-1)
		return nil
	}

	err := tm.AddTask("allow-overlap", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for multiple executions
	time.Sleep(3 * time.Second)

	if !hadConcurrent.Load() {
		t.Error("Expected concurrent execution when overlapping is allowed")
	}
}

// TestPerTaskOverlappingOverride verifies WithTaskOverlapping wins over the manager default.
func TestPerTaskOverlappingOverride(t *testing.T) {
	t.Run("task allows overlap despite manager default", func(t *testing.T) {
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		tm := New(WithAllowOverlapping(false))
		defer tm.Stop()

		if err := tm.AddTask("overlap-ok", Every().Minute(), func(ctx context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}, WithTaskOverlapping(true)); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}

		if _, err := tm.RunTaskNow("overlap-ok"); err != nil {
			t.Fatalf("first RunTaskNow() failed: %v", err)
		}
		if _, err := tm.RunTaskNow("overlap-ok"); err != nil {
			t.Fatalf("second RunTaskNow() failed: %v", err)
		}
		for i := 0; i < 2; i++ {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("overlapping instance did not start")
			}
		}
		close(release)
	})

	t.Run("task forbids overlap despite manager default", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		tm := New(WithAllowOverlapping(true))
		defer tm.Stop()

		if err := tm.AddTask("no-overlap", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}, WithTaskOverlapping(false)); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}

		if _, err := tm.RunTaskNow("no-overlap"); err != nil {
			t.Fatalf("first RunTaskNow() failed: %v", err)
		}
		<-started
		if _, err := tm.RunTaskNow("no-overlap"); !errors.Is(err, ErrTaskRunning) {
			t.Fatalf("second RunTaskNow() error = %v, want ErrTaskRunning", err)
		}
		close(release)
	})
}

// TestPerTaskTimeout verifies WithTaskTimeout cancels the task context.
func TestPerTaskTimeout(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("slow", Every().Minute(), func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return nil
		}
	}, WithTaskTimeout(100*time.Millisecond)); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	start := time.Now()
	err := tm.RunTaskNowAndWait(context.Background(), "slow")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("RunTaskNowAndWait() error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("task took %v, timeout did not kick in", elapsed)
	}

	info, err := tm.GetTask("slow")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.ErrorCount != 1 {
		t.Fatalf("ErrorCount = %d, want 1", info.ErrorCount)
	}
}

func TestRunningCountWithOverlappingExecutions(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	completed := make(chan struct{}, 2)

	tm := New(WithAllowOverlapping(true), WithOnTaskComplete(func(event TaskCompleteEvent) {
		completed <- struct{}{}
	}))
	defer tm.Stop()

	if err := tm.AddTask("overlap-count", Every().Minute(), func(ctx context.Context) error {
		started <- struct{}{}
		<-release
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("overlap-count"); err != nil {
		t.Fatalf("first RunTaskNow() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("overlap-count"); err != nil {
		t.Fatalf("second RunTaskNow() failed: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("overlapping task did not start")
		}
	}

	info, err := tm.GetTask("overlap-count")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if !info.Running() || info.RunningCount != 2 {
		t.Fatalf("Running=%v RunningCount=%d, want true/2", info.Running(), info.RunningCount)
	}

	close(release)
	for i := 0; i < 2; i++ {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("overlapping task did not complete")
		}
	}

	info, err = tm.GetTask("overlap-count")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.Running() || info.RunningCount != 0 {
		t.Fatalf("Running=%v RunningCount=%d, want false/0", info.Running(), info.RunningCount)
	}
}

// TestContextInjection verifies context value injection
func TestContextInjection(t *testing.T) {
	staticValue := "static-value"
	dynamicValue := "dynamic-value"

	type dynamicKey string

	tm := New(
		WithContextValue("static-key", staticValue),
		WithContextInjector(func(ctx context.Context, taskName string) context.Context {
			return context.WithValue(ctx, dynamicKey("dynamic-key"), dynamicValue)
		}),
	)
	tm.Start()
	defer tm.Stop()

	var receivedStatic, receivedDynamic, receivedTaskName string
	var done atomic.Bool

	task := func(ctx context.Context) error {
		if v, ok := ContextValue(ctx, "static-key").(string); ok {
			receivedStatic = v
		}
		if v, ok := ctx.Value(dynamicKey("dynamic-key")).(string); ok {
			receivedDynamic = v
		}
		receivedTaskName = GetTaskName(ctx)
		done.Store(true)
		return nil
	}

	err := tm.AddTask("context-test", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for execution
	for i := 0; i < 20 && !done.Load(); i++ {
		time.Sleep(100 * time.Millisecond)
	}

	if receivedStatic != staticValue {
		t.Errorf("Static context value = %v, want %v", receivedStatic, staticValue)
	}

	if receivedDynamic != dynamicValue {
		t.Errorf("Dynamic context value = %v, want %v", receivedDynamic, dynamicValue)
	}

	if receivedTaskName != "context-test" {
		t.Errorf("Task name = %v, want %v", receivedTaskName, "context-test")
	}
}

// TestTaskNameImmuneToUserContextValues verifies user values cannot shadow the
// internal task name key.
func TestTaskNameImmuneToUserContextValues(t *testing.T) {
	tm := New(WithContextValue("taskName", "user-value"))
	defer tm.Stop()

	got := make(chan [2]any, 1)
	if err := tm.AddTask("real-task", Every().Minute(), func(ctx context.Context) error {
		got <- [2]any{GetTaskName(ctx), ContextValue(ctx, "taskName")}
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if err := tm.RunTaskNowAndWait(context.Background(), "real-task"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}

	values := <-got
	if values[0] != "real-task" {
		t.Errorf("GetTaskName() = %v, want real-task (must not be shadowed by user value)", values[0])
	}
	if values[1] != "user-value" {
		t.Errorf("ContextValue(taskName) = %v, want user-value", values[1])
	}
}

func TestContextValueHelper(t *testing.T) {
	tm := New(WithContextValue("app", "demo"))
	defer tm.Stop()

	got := make(chan any, 1)
	if err := tm.AddTask("context-helper", Every().Minute(), func(ctx context.Context) error {
		got <- ContextValue(ctx, "app")
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if _, err := tm.RunTaskNow("context-helper"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case value := <-got:
		if value != "demo" {
			t.Fatalf("ContextValue() = %v, want demo", value)
		}
	case <-time.After(time.Second):
		t.Fatal("task did not run")
	}
}

// TestSetGetContextValue verifies runtime context value management
func TestSetGetContextValue(t *testing.T) {
	tm := New()
	defer tm.Stop()

	// Test setting and getting
	tm.SetContextValue("key1", "value1")
	if v := tm.GetContextValue("key1"); v != "value1" {
		t.Errorf("GetContextValue() = %v, want value1", v)
	}

	// Test updating
	tm.SetContextValue("key1", "value2")
	if v := tm.GetContextValue("key1"); v != "value2" {
		t.Errorf("GetContextValue() = %v, want value2", v)
	}

	// Test non-existent key
	if v := tm.GetContextValue("non-existent"); v != nil {
		t.Errorf("GetContextValue() = %v, want nil", v)
	}

	// Test empty key
	tm.SetContextValue("", "value")
	if v := tm.GetContextValue(""); v != nil {
		t.Errorf("GetContextValue() with empty key = %v, want nil", v)
	}
}

func TestLifecycleHooks(t *testing.T) {
	started := make(chan TaskStartEvent, 1)
	completed := make(chan TaskCompleteEvent, 1)

	var tm *TaskManager
	tm = New(
		WithOnTaskStart(func(event TaskStartEvent) {
			if _, err := tm.GetTask(event.TaskName); err != nil {
				t.Errorf("GetTask() in OnTaskStart failed: %v", err)
			}
			started <- event
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			info, err := tm.GetTask(event.TaskName)
			if err != nil {
				t.Errorf("GetTask() in OnTaskComplete failed: %v", err)
			} else if info.Running() || info.RunningCount != 0 {
				t.Errorf("OnTaskComplete observed running task: Running=%v RunningCount=%d", info.Running(), info.RunningCount)
			}
			completed <- event
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("hooked", Every().Minute(), func(ctx context.Context) error {
		if GetTaskName(ctx) != "hooked" {
			t.Errorf("GetTaskName() = %q, want hooked", GetTaskName(ctx))
		}
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("hooked"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case event := <-started:
		if event.TaskName != "hooked" || event.Trigger != TriggerManual || event.RunCount != 1 || event.RunningCount != 1 {
			t.Fatalf("unexpected start event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskStart was not called")
	}

	select {
	case event := <-completed:
		if event.TaskName != "hooked" || event.Trigger != TriggerManual || event.Error != nil || event.RunCount != 1 || event.RunningCount != 0 {
			t.Fatalf("unexpected complete event: %+v", event)
		}
		if event.Duration < 0 {
			t.Fatalf("complete duration = %v, want non-negative", event.Duration)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskComplete was not called")
	}
}

// TestCompleteEventRunCountMatchesStartEvent verifies that with overlapping
// executions, each completion reports its own execution's RunCount rather
// than the latest global counter.
func TestCompleteEventRunCountMatchesStartEvent(t *testing.T) {
	release := make(chan struct{})
	started := make(chan int64, 2)
	completed := make(chan int64, 2)

	tm := New(
		WithAllowOverlapping(true),
		WithOnTaskStart(func(event TaskStartEvent) {
			started <- event.RunCount
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			completed <- event.RunCount
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("overlap-count", Every().Minute(), func(ctx context.Context) error {
		<-release
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	if _, err := tm.RunTaskNow("overlap-count"); err != nil {
		t.Fatalf("first RunTaskNow() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("overlap-count"); err != nil {
		t.Fatalf("second RunTaskNow() failed: %v", err)
	}

	startCounts := map[int64]bool{}
	for i := 0; i < 2; i++ {
		select {
		case c := <-started:
			startCounts[c] = true
		case <-time.After(time.Second):
			t.Fatal("start events not received")
		}
	}
	close(release)

	for i := 0; i < 2; i++ {
		select {
		case c := <-completed:
			if !startCounts[c] {
				t.Fatalf("complete event RunCount %d does not match any start event", c)
			}
			delete(startCounts, c)
		case <-time.After(time.Second):
			t.Fatal("complete events not received")
		}
	}
}

func TestLifecycleHookPanicsDoNotAffectTask(t *testing.T) {
	completed := make(chan TaskCompleteEvent, 1)
	taskRan := make(chan struct{}, 1)
	tm := New(
		WithOnTaskStart(func(event TaskStartEvent) {
			panic("start hook failed")
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			completed <- event
			panic("complete hook failed")
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("hook-panic", Every().Minute(), func(ctx context.Context) error {
		taskRan <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("hook-panic"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case <-taskRan:
	case <-time.After(time.Second):
		t.Fatal("task did not run after OnTaskStart panic")
	}
	select {
	case event := <-completed:
		if event.Error != nil {
			t.Fatalf("complete event error = %v, want nil", event.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskComplete was not called")
	}
}

func TestTaskPanicIsRecordedAsFailure(t *testing.T) {
	completed := make(chan TaskCompleteEvent, 1)
	tm := New(WithOnTaskComplete(func(event TaskCompleteEvent) {
		completed <- event
	}))
	defer tm.Stop()

	if err := tm.AddTask("panic-task", Every().Minute(), func(ctx context.Context) error {
		panic("boom")
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("panic-task"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case event := <-completed:
		if event.Error == nil || !strings.Contains(event.Error.Error(), "task panic: boom") {
			t.Fatalf("complete error = %v, want task panic", event.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskComplete was not called")
	}

	info, err := tm.GetTask("panic-task")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.ErrorCount != 1 || !strings.Contains(info.LastError, "task panic: boom") {
		t.Fatalf("panic failure not recorded: ErrorCount=%d LastError=%q", info.ErrorCount, info.LastError)
	}
}

func TestContextInjectorPanicIsRecordedAsFailure(t *testing.T) {
	started := make(chan TaskStartEvent, 1)
	completed := make(chan TaskCompleteEvent, 1)
	taskRan := make(chan struct{}, 1)

	tm := New(
		WithContextInjector(func(ctx context.Context, taskName string) context.Context {
			panic("injector boom")
		}),
		WithOnTaskStart(func(event TaskStartEvent) {
			started <- event
		}),
		WithOnTaskComplete(func(event TaskCompleteEvent) {
			completed <- event
		}),
	)
	defer tm.Stop()

	if err := tm.AddTask("injector-panic", Every().Minute(), func(ctx context.Context) error {
		taskRan <- struct{}{}
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("injector-panic"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}

	select {
	case event := <-completed:
		if event.Error == nil || !strings.Contains(event.Error.Error(), "task panic: injector boom") {
			t.Fatalf("complete error = %v, want injector panic", event.Error)
		}
	case <-time.After(time.Second):
		t.Fatal("OnTaskComplete was not called")
	}

	select {
	case <-started:
		t.Fatal("OnTaskStart should not run when context injection fails")
	default:
	}
	select {
	case <-taskRan:
		t.Fatal("task should not run when context injection fails")
	default:
	}

	info, err := tm.GetTask("injector-panic")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.Running() || info.RunningCount != 0 || info.ErrorCount != 1 {
		t.Fatalf("task state after injector panic = Running=%v RunningCount=%d ErrorCount=%d", info.Running(), info.RunningCount, info.ErrorCount)
	}
}

// TestStats verifies statistics collection
func TestStats(t *testing.T) {
	tm := New(WithMaxConcurrent(5), WithAllowOverlapping(true))
	tm.Start()
	defer tm.Stop()

	var counter atomic.Int32
	successTask := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	errorTask := func(ctx context.Context) error {
		return errors.New("task error")
	}

	err := tm.AddTask("success-task", Every().Second(), successTask)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	err = tm.AddTask("error-task", Every().Second(), errorTask)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for some executions
	time.Sleep(2500 * time.Millisecond)

	stats := tm.Stats()

	if stats.TotalTasks != 2 {
		t.Errorf("TotalTasks = %v, want 2", stats.TotalTasks)
	}
	if stats.EnabledTasks != 2 {
		t.Errorf("EnabledTasks = %v, want 2", stats.EnabledTasks)
	}
	if stats.MaxConcurrent != 5 {
		t.Errorf("MaxConcurrent = %v, want 5", stats.MaxConcurrent)
	}
	if !stats.AllowOverlapping {
		t.Errorf("AllowOverlapping = %v, want true", stats.AllowOverlapping)
	}
	if stats.TotalRuns < 2 {
		t.Errorf("TotalRuns = %v, want >= 2", stats.TotalRuns)
	}
	if stats.TotalErrors == 0 {
		t.Error("expected some errors, got 0")
	}
}

// TestGracefulShutdown verifies Stop waits for running tasks without
// canceling their contexts when they finish within the timeout.
func TestGracefulShutdown(t *testing.T) {
	tm := New()
	tm.Start()

	var started atomic.Bool
	var completed atomic.Bool
	var canceled atomic.Bool

	task := func(ctx context.Context) error {
		started.Store(true)
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
			completed.Store(true)
			return nil
		}
	}

	err := tm.AddTask("long-task", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for task to start
	time.Sleep(1200 * time.Millisecond)

	if !started.Load() {
		t.Fatal("Task did not start")
	}

	// Stop should wait for the task to complete without canceling it.
	if err := tm.Stop(); err != nil {
		t.Errorf("Stop() error = %v, want nil", err)
	}

	if !completed.Load() {
		t.Error("Task was not allowed to complete during shutdown")
	}
	if canceled.Load() {
		t.Error("Task context was canceled even though it finished within the timeout")
	}

	if tm.IsStarted() {
		t.Error("TaskManager is still started after Stop()")
	}
}

// TestStopCancelsTasksAfterTimeout verifies the second phase of shutdown:
// tasks still running when the deadline expires get their contexts canceled.
func TestStopCancelsTasksAfterTimeout(t *testing.T) {
	tm := New(WithShutdownTimeout(200 * time.Millisecond))

	started := make(chan struct{})
	var sawCancel atomic.Bool

	if err := tm.AddTask("stubborn", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		sawCancel.Store(true)
		return ctx.Err()
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("stubborn"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}
	<-started

	err := tm.Stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want context.DeadlineExceeded", err)
	}
	if !sawCancel.Load() {
		t.Fatal("task context was not canceled after the shutdown deadline")
	}
}

func TestStopContext(t *testing.T) {
	tm := New()
	tm.Start()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tm.StopContext(ctx); err != nil {
		t.Fatalf("StopContext() error = %v, want nil", err)
	}

	if err := tm.StopContext(context.Background()); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("second StopContext() error = %v, want ErrTaskManagerStopped", err)
	}
}

func TestTaskManagerLifecycleIsOneWay(t *testing.T) {
	tm := New()
	if tm.IsStarted() {
		t.Fatal("new TaskManager should not be started before Start")
	}
	if err := tm.Start(); err != nil {
		t.Fatalf("Start() error = %v, want nil", err)
	}
	if !tm.IsStarted() {
		t.Fatal("TaskManager should be started after Start")
	}
	if err := tm.Start(); !errors.Is(err, ErrTaskManagerStarted) {
		t.Fatalf("second Start() error = %v, want ErrTaskManagerStarted", err)
	}
	if err := tm.Stop(); err != nil {
		t.Fatalf("Stop() error = %v, want nil", err)
	}
	if tm.IsStarted() {
		t.Fatal("TaskManager should not be started after Stop")
	}
	if err := tm.Stop(); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("second Stop() error = %v, want ErrTaskManagerStopped", err)
	}

	if err := tm.AddTask("after-stop", Every().Minute(), func(ctx context.Context) error { return nil }); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("AddTask() after Stop error = %v, want ErrTaskManagerStopped", err)
	}

	if err := tm.Start(); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("Start() after Stop error = %v, want ErrTaskManagerStopped", err)
	}
	if tm.IsStarted() {
		t.Fatal("Start after Stop should not restart the manager")
	}
}

func TestStopBeforeStartIsTerminal(t *testing.T) {
	tm := New()
	if err := tm.Stop(); err != nil {
		t.Fatalf("Stop() before Start error = %v, want nil", err)
	}
	if err := tm.Stop(); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("second Stop() error = %v, want ErrTaskManagerStopped", err)
	}

	if tm.IsStarted() {
		t.Fatal("TaskManager should not be started after Stop before Start")
	}
	if err := tm.AddTask("after-stop", Every().Minute(), func(ctx context.Context) error { return nil }); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("AddTask() after Stop error = %v, want ErrTaskManagerStopped", err)
	}
	if _, err := tm.RunTaskNow("after-stop"); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("RunTaskNow() after Stop error = %v, want ErrTaskManagerStopped", err)
	}
	if err := tm.Start(); !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("Start() after Stop error = %v, want ErrTaskManagerStopped", err)
	}
}

func TestRunTaskNowAfterStop(t *testing.T) {
	tm := New()
	if err := tm.AddTask("manual", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	tm.Stop()

	_, err := tm.RunTaskNow("manual")
	if !errors.Is(err, ErrTaskManagerStopped) {
		t.Fatalf("RunTaskNow() after Stop error = %v, want ErrTaskManagerStopped", err)
	}
}

// TestConcurrentOperations verifies thread-safety
func TestConcurrentOperations(t *testing.T) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var wg sync.WaitGroup
	numGoroutines := 10

	// Concurrent task additions
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			task := func(ctx context.Context) error { return nil }
			tm.AddTask(fmt.Sprintf("task-%d", id), Every().Minute(), task)
		}(i)
	}

	wg.Wait()

	// Concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tm.ListTasks()
			tm.Stats()
		}()
	}

	wg.Wait()

	// Concurrent modifications
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			taskName := fmt.Sprintf("task-%d", id)
			tm.DisableTask(taskName)
			tm.EnableTask(taskName)
			tm.GetTask(taskName)
			tm.UpdateSchedule(taskName, Every().Hour())
		}(i)
	}

	wg.Wait()
}

// TestTaskInfoCopy verifies that GetTask returns an independent snapshot
func TestTaskInfoCopy(t *testing.T) {
	tm := New()
	defer tm.Stop()

	task := func(ctx context.Context) error { return nil }
	err := tm.AddTask("copy-test", Every().Minute(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	info1, _ := tm.GetTask("copy-test")
	info2, _ := tm.GetTask("copy-test")

	// Modify the copy
	info1.RunCount = 999

	// Original should be unchanged
	if info2.RunCount == 999 {
		t.Error("GetTask() did not return a copy, modifications affected other references")
	}
}

// TestContextCancellation verifies tasks are canceled once the shutdown
// deadline passes.
func TestContextCancellation(t *testing.T) {
	tm := New(WithShutdownTimeout(200 * time.Millisecond))
	tm.Start()

	var taskStarted atomic.Bool
	var taskCompleted atomic.Bool

	task := func(ctx context.Context) error {
		taskStarted.Store(true)
		select {
		case <-time.After(5 * time.Second):
			taskCompleted.Store(true)
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	err := tm.AddTask("cancel-test", Every().Second(), task)
	if err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}

	// Wait for task to start
	time.Sleep(1200 * time.Millisecond)

	if !taskStarted.Load() {
		t.Fatal("Task did not start")
	}

	// Stop cancels the task after the (short) shutdown timeout
	tm.Stop()

	// Task should not complete normally
	if taskCompleted.Load() {
		t.Error("Task completed normally despite context cancellation")
	}
}

// BenchmarkTaskExecution benchmarks task execution overhead
func BenchmarkTaskExecution(b *testing.B) {
	tm := New()
	tm.Start()
	defer tm.Stop()

	var counter atomic.Int32
	task := func(ctx context.Context) error {
		counter.Add(1)
		return nil
	}

	err := tm.AddTask("bench", Every().Second(), task)
	if err != nil {
		b.Fatalf("AddTask() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.RunTaskNow("bench")
	}
	b.StopTimer()

	// Wait for all tasks to complete
	time.Sleep(500 * time.Millisecond)
}

// BenchmarkConcurrentTasks benchmarks concurrent task execution
func BenchmarkConcurrentTasks(b *testing.B) {
	tm := New(WithMaxConcurrent(10))
	tm.Start()
	defer tm.Stop()

	task := func(ctx context.Context) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}

	for i := 0; i < 10; i++ {
		err := tm.AddTask(fmt.Sprintf("bench-%d", i), Every().Second(), task)
		if err != nil {
			b.Fatalf("AddTask() failed: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.RunTaskNow(fmt.Sprintf("bench-%d", i%10))
	}
	b.StopTimer()

	time.Sleep(500 * time.Millisecond)
}

// BenchmarkListTasks measures snapshot cost with many registered tasks.
func BenchmarkListTasks(b *testing.B) {
	tm := New()
	defer tm.Stop()

	for i := 0; i < 200; i++ {
		if err := tm.AddTask(fmt.Sprintf("task-%d", i), Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
			b.Fatalf("AddTask() failed: %v", err)
		}
	}
	tm.Start()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tm.ListTasks()
	}
}

// TestRecentEvents verifies the event ring buffer records and orders events.
func TestRecentEvents(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("evt", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if err := tm.RunTaskNowAndWait(context.Background(), "evt"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}

	events := tm.RecentEvents(0)
	if len(events) < 3 {
		t.Fatalf("expected at least 3 events, got %d", len(events))
	}
	// newest first: completed, manual-trigger, task-added
	if events[0].Kind != EventCompleted || events[0].TaskName != "evt" {
		t.Fatalf("events[0] = %+v, want completed for evt", events[0])
	}
	if events[1].Kind != EventLifecycle || events[1].Message != "manual run triggered" {
		t.Fatalf("events[1] = %+v, want manual trigger lifecycle", events[1])
	}
	if events[0].Duration < 0 {
		t.Fatalf("completed event duration = %v, want >= 0", events[0].Duration)
	}

	if got := tm.RecentEvents(2); len(got) != 2 {
		t.Fatalf("RecentEvents(2) returned %d events", len(got))
	}
}

// TestSkipCountAndSkipEvents verifies skips are counted and recorded.
func TestSkipCountAndSkipEvents(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("skippy", Every().Minute(), func(ctx context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if _, err := tm.RunTaskNow("skippy"); err != nil {
		t.Fatalf("RunTaskNow() failed: %v", err)
	}
	<-started
	if _, err := tm.RunTaskNow("skippy"); !errors.Is(err, ErrTaskRunning) {
		t.Fatalf("second RunTaskNow() error = %v, want ErrTaskRunning", err)
	}
	close(release)

	info, err := tm.GetTask("skippy")
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.SkipCount != 1 {
		t.Fatalf("SkipCount = %d, want 1", info.SkipCount)
	}
	if tm.Stats().TotalSkips != 1 {
		t.Fatalf("Stats().TotalSkips = %d, want 1", tm.Stats().TotalSkips)
	}

	foundSkip := false
	for _, ev := range tm.RecentEvents(0) {
		if ev.Kind == EventSkipped && ev.TaskName == "skippy" {
			foundSkip = true
		}
	}
	if !foundSkip {
		t.Fatal("skip event not recorded")
	}
}

// TestUpcomingRuns verifies bulk next-fire computation.
func TestUpcomingRuns(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if err := tm.AddTask("fast", Every().Seconds(10), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask(fast) failed: %v", err)
	}
	if err := tm.AddTask("slow", Every().Day().At(2, 0), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask(slow) failed: %v", err)
	}
	if err := tm.AddTask("off", Every().Seconds(10), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask(off) failed: %v", err)
	}
	if err := tm.DisableTask("off"); err != nil {
		t.Fatalf("DisableTask() failed: %v", err)
	}

	runs := tm.UpcomingRuns(time.Minute, 0)
	if n := len(runs["fast"]); n < 5 || n > 7 {
		t.Fatalf("fast upcoming = %d fires in 1m, want ~6", n)
	}
	for i := 1; i < len(runs["fast"]); i++ {
		if !runs["fast"][i].After(runs["fast"][i-1]) {
			t.Fatal("upcoming fires not strictly increasing")
		}
	}
	if len(runs["slow"]) != 0 {
		t.Fatalf("slow upcoming = %d fires in 1m, want 0", len(runs["slow"]))
	}
	if runs["off"] != nil {
		t.Fatalf("disabled task upcoming = %v, want nil", runs["off"])
	}

	capped := tm.UpcomingRuns(time.Hour, 3)
	if len(capped["fast"]) != 3 {
		t.Fatalf("perTaskLimit not honored: %d fires", len(capped["fast"]))
	}
}

// TestPreviewSchedule verifies validation and next-fire preview.
func TestPreviewSchedule(t *testing.T) {
	tm := New()
	defer tm.Stop()

	expr, fires, err := tm.PreviewSchedule(Cron("*/5 * * * *"), 3)
	if err != nil {
		t.Fatalf("PreviewSchedule() failed: %v", err)
	}
	if expr != "0 */5 * * * *" {
		t.Fatalf("normalized expr = %q, want %q", expr, "0 */5 * * * *")
	}
	if len(fires) != 3 {
		t.Fatalf("fires = %d, want 3", len(fires))
	}
	if gap := fires[1].Sub(fires[0]); gap != 5*time.Minute {
		t.Fatalf("fire gap = %v, want 5m", gap)
	}

	if _, _, err := tm.PreviewSchedule(Cron("*/90 * * * * *"), 3); err == nil {
		t.Fatal("PreviewSchedule() accepted an out-of-range step")
	}
	if _, _, err := tm.PreviewSchedule(Cron("garbage"), 3); err == nil {
		t.Fatal("PreviewSchedule() accepted garbage")
	}
	if _, _, err := tm.PreviewSchedule(nil, 3); err == nil {
		t.Fatal("PreviewSchedule() accepted nil schedule")
	}
}

// TestRawCronStepValidation verifies AddTask rejects out-of-range raw-cron steps.
func TestRawCronStepValidation(t *testing.T) {
	tm := New()
	defer tm.Stop()

	task := func(ctx context.Context) error { return nil }
	if err := tm.AddTask("bad-sec", Cron("*/90 * * * * *"), task); err == nil {
		t.Fatal("AddTask() accepted */90 seconds step")
	}
	if err := tm.AddTask("bad-min", Cron("0 */75 * * * *"), task); err == nil {
		t.Fatal("AddTask() accepted */75 minutes step")
	}
	if err := tm.AddTask("bad-hour", Cron("0 0 */30 * * *"), task); err == nil {
		t.Fatal("AddTask() accepted */30 hours step")
	}
	if err := tm.AddTask("good", Cron("*/30 * * * * *"), task); err != nil {
		t.Fatalf("AddTask() rejected a valid step: %v", err)
	}
}

// TestStatsStartedAt verifies the started timestamp is exposed.
func TestStatsStartedAt(t *testing.T) {
	tm := New()
	defer tm.Stop()

	if !tm.Stats().StartedAt.IsZero() {
		t.Fatal("StartedAt should be zero before Start")
	}
	if err := tm.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	if tm.Stats().StartedAt.IsZero() {
		t.Fatal("StartedAt should be set after Start")
	}
}
