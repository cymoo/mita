package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/cymoo/mita"
)

func main() {
	// Create custom logger
	logger := log.New(os.Stdout, "[mita] ", log.LstdFlags)

	// Load timezone
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		log.Fatalf("Failed to load timezone: %v", err)
	}

	// Create task manager with various options
	tm := mita.New(
		mita.WithLogger(logger),
		mita.WithLocation(location),
		mita.WithMaxConcurrent(3),        // Max 3 concurrent running tasks
		mita.WithAllowOverlapping(false), // Prevent overlapping executions
		mita.WithShutdownTimeout(10*time.Second),
		mita.WithContextValue("app", "demo"),
		mita.WithContextInjector(func(ctx context.Context, taskName string) context.Context {
			// Dynamically inject a request ID under your own key type
			return context.WithValue(ctx, requestIDKey{}, fmt.Sprintf("%s-%d", taskName, time.Now().Unix()))
		}),
		mita.WithOnTaskSkip(func(e mita.TaskSkipEvent) {
			fmt.Printf("⏭  %s skipped (%s): %v\n", e.TaskName, e.Trigger, e.Reason)
		}),
	)

	// Example 1: Data cleanup task - runs every 5 seconds
	err = tm.AddTask("cleanup", mita.Every().Seconds(5), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		app := mita.ContextValue(ctx, "app")
		requestID := ctx.Value(requestIDKey{})

		fmt.Printf("[%s] Starting data cleanup... (app=%v, request_id=%v)\n", taskName, app, requestID)
		time.Sleep(2 * time.Second) // Simulate work
		fmt.Printf("[%s] Cleanup completed\n", taskName)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to add cleanup task: %v", err)
	}

	// Example 2: Data sync task - fixed 10-second interval (may fail)
	err = tm.AddTask("sync", mita.Every().Interval(10*time.Second), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		fmt.Printf("[%s] Starting data synchronization...\n", taskName)

		// Simulate 30% failure rate
		if time.Now().Unix()%3 == 0 {
			return fmt.Errorf("network connection timeout")
		}

		time.Sleep(3 * time.Second)
		fmt.Printf("[%s] Sync completed successfully\n", taskName)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to add sync task: %v", err)
	}

	// Example 3: Report generation - runs every minute
	err = tm.AddTask("report", mita.Every().Minute(), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		fmt.Printf("[%s] Generating minute report...\n", taskName)

		// Check if context is cancelled
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
			fmt.Printf("[%s] Report generation completed\n", taskName)
			return nil
		}
	})
	if err != nil {
		log.Fatalf("Failed to add report task: %v", err)
	}

	// Example 4: Daily backup at 2:00 AM
	err = tm.AddTask("backup", mita.Every().Day().At(2, 0), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		fmt.Printf("[%s] Starting daily backup...\n", taskName)
		time.Sleep(1 * time.Second)
		fmt.Printf("[%s] Backup completed\n", taskName)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to add backup task: %v", err)
	}

	// Example 5: Health check using a standard 5-field cron expression - every 15 minutes
	err = tm.AddTask("health-check", mita.Cron("*/15 * * * *"), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		fmt.Printf("[%s] Performing health check...\n", taskName)
		time.Sleep(500 * time.Millisecond)
		fmt.Printf("[%s] System healthy\n", taskName)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to add health check task: %v", err)
	}

	// Example 6: Long-running task with a per-task timeout
	err = tm.AddTask("long-task", mita.Every().Minutes(2), func(ctx context.Context) error {
		taskName := mita.GetTaskName(ctx)
		fmt.Printf("[%s] Starting long-running task...\n", taskName)

		// Simulate long processing with proper cancellation handling
		for i := 0; i < 10; i++ {
			select {
			case <-ctx.Done():
				fmt.Printf("[%s] Task cancelled, cleaning up...\n", taskName)
				return ctx.Err()
			default:
				fmt.Printf("[%s] Processing step %d/10...\n", taskName, i+1)
				time.Sleep(500 * time.Millisecond)
			}
		}

		fmt.Printf("[%s] Long task completed\n", taskName)
		return nil
	}, mita.WithTaskTimeout(30*time.Second))
	if err != nil {
		log.Fatalf("Failed to add long-running task: %v", err)
	}

	// Start the task manager
	if err := tm.Start(); err != nil {
		log.Fatalf("Failed to start task manager: %v", err)
	}
	fmt.Println("\n✓ Task manager started. Press Ctrl+C to stop...")
	fmt.Println(strings.Repeat("=", 70))

	// Create web handler mounted at /tasks
	mux := tm.WebHandler("/tasks")

	// Manually trigger sync task after 3 seconds and wait for its result
	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("\n>>> Manually triggering sync task <<<")
		done, err := tm.RunTaskNow("sync")
		if err != nil {
			log.Printf("Manual trigger failed: %v", err)
			return
		}
		if err := <-done; err != nil {
			log.Printf("Manual sync run finished with error: %v", err)
		}
	}()

	// Demonstrate task management operations
	go func() {
		time.Sleep(15 * time.Second)

		// Disable cleanup task
		fmt.Println("\n>>> Disabling cleanup task <<<")
		if err := tm.DisableTask("cleanup"); err != nil {
			log.Printf("Failed to disable task: %v", err)
		}

		time.Sleep(10 * time.Second)

		// Re-enable cleanup task
		fmt.Println("\n>>> Re-enabling cleanup task <<<")
		if err := tm.EnableTask("cleanup"); err != nil {
			log.Printf("Failed to enable task: %v", err)
		}

		// Reschedule the report task without losing its statistics
		fmt.Println("\n>>> Rescheduling report task to every 30 seconds <<<")
		if err := tm.UpdateSchedule("report", mita.Every().Seconds(30)); err != nil {
			log.Printf("Failed to update schedule: %v", err)
		}
	}()

	// Demonstrate dynamic context value updates
	go func() {
		ticker := time.NewTicker(25 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			tm.SetContextValue("last_update", time.Now().Format(time.RFC3339))
			fmt.Println("\n>>> Updated context value: last_update <<<")
		}
	}()

	// Display statistics periodically
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			displayStats(tm)
		}
	}()

	// Start HTTP server
	server := &http.Server{Addr: "localhost:8080", Handler: mux}
	go func() {
		log.Printf("server starting on http://localhost:8080/tasks")
		fmt.Println(strings.Repeat("=", 70))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}()

	// Graceful shutdown on Ctrl+C: stop the HTTP server, then drain tasks.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Println("\n>>> Shutting down <<<")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	if err := tm.StopContext(shutdownCtx); err != nil {
		log.Printf("Task manager stopped with error: %v", err)
	}
}

// requestIDKey is a private context key type for the injected request ID.
type requestIDKey struct{}

// displayStats shows current task manager statistics
func displayStats(tm *mita.TaskManager) {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("TASK MANAGER STATISTICS")
	fmt.Println(strings.Repeat("=", 70))

	stats := tm.Stats()
	fmt.Printf("Total Tasks:       %d\n", stats.TotalTasks)
	fmt.Printf("Enabled Tasks:     %d\n", stats.EnabledTasks)
	fmt.Printf("Running Tasks:     %d\n", stats.RunningTasks)
	fmt.Printf("Total Executions:  %d\n", stats.TotalRuns)
	fmt.Printf("Total Errors:      %d\n", stats.TotalErrors)
	fmt.Printf("Max Concurrent:    %d\n", stats.MaxConcurrent)
	fmt.Printf("Allow Overlapping: %v\n", stats.AllowOverlapping)

	fmt.Println("\nPER-TASK DETAILS")
	fmt.Println(strings.Repeat("=", 70))

	tasks := tm.ListTasks()
	for _, task := range tasks {
		fmt.Printf("\n📌 Task: %s\n", task.Name)
		fmt.Printf("   Schedule:     %s\n", task.Schedule)
		fmt.Printf("   Status:       Enabled=%v, Running=%v\n", task.Enabled, task.Running())
		fmt.Printf("   Executions:   %d (Errors: %d)\n", task.RunCount, task.ErrorCount)
		fmt.Printf("   Added At:     %s\n", task.AddedAt.Format("2006-01-02 15:04:05"))

		if !task.LastRun.IsZero() {
			fmt.Printf("   Last Run:     %s\n", task.LastRun.Format("2006-01-02 15:04:05"))
		}

		if !task.NextRun.IsZero() {
			fmt.Printf("   Next Run:     %s\n", task.NextRun.Format("2006-01-02 15:04:05"))
		}

		if task.LastError != "" {
			fmt.Printf("   ⚠️  Last Error: %s\n", task.LastError)
		}

		// Calculate success rate
		if task.RunCount > 0 {
			successRate := float64(task.RunCount-task.ErrorCount) / float64(task.RunCount) * 100
			fmt.Printf("   Success Rate: %.1f%%\n", successRate)
		}
	}

	fmt.Println("\n" + strings.Repeat("=", 70) + "\n")
}
