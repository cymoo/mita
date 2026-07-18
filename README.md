# Mita

A minimum task scheduling library for Go, built on top of cron expressions with advanced features including concurrency control, error tracking, and flexible configuration options.

## Features

- 🕐 **Flexible Scheduling** - 5- or 6-field cron expressions, `@every` intervals, and a fluent builder API
- 🔒 **Concurrency Control** - Configurable max concurrent tasks and overlap prevention (global and per task)
- 📊 **Execution Tracking** - Automatic statistics for runs, errors, and execution status
- 🎯 **Context Injection** - Support for both static and dynamic context value injection
- 🔄 **Graceful Shutdown** - Waits for running tasks to finish, then cancels stragglers after a configurable timeout
- 🚀 **Manual Triggers** - Execute tasks on-demand outside their regular schedule
- 🎛️ **Task Management** - Full CRUD operations: enable, disable, remove, reschedule tasks
- ⏱️ **Per-Task Options** - Execution timeout and overlap policy per task
- 📝 **Logging** - Customizable logging output with detailed execution info
- 🪝 **Lifecycle Hooks** - Observe task start, completion, and skip events
- 🌍 **Timezone Support** - Configure task execution timezone
- ⚡ **Second Precision** - Support for second-level scheduling granularity
- 🛡️ **Thread-Safe** - Safe for concurrent use across multiple goroutines
- 🔍 **Rich Metadata** - Track added time, last run, next run, and more
- 🌐 **Web Interface** - Built-in web UI for monitoring and managing tasks


## Installation

```bash
go get github.com/cymoo/mita
```

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/cymoo/mita"
)

func main() {
    // Create task manager
    tm := mita.New()
    
    // Add a task that runs every 5 seconds
    tm.AddTask("hello", mita.Every().Seconds(5), func(ctx context.Context) error {
        fmt.Println("Hello, World!")
        return nil
    })
    
    // Start the manager
    if err := tm.Start(); err != nil {
        panic(err)
    }
    
    // Run for a while
    time.Sleep(30 * time.Second)
    
    // Graceful shutdown
    if err := tm.Stop(); err != nil {
        fmt.Println("shutdown:", err)
    }
}
```

### Advanced Configuration

```go
// Create task manager with options
tm := mita.New(
    mita.WithLogger(customLogger),           // Custom logger
    mita.WithLocation(location),             // Set timezone
    mita.WithMaxConcurrent(5),               // Max concurrent running tasks
    mita.WithAllowOverlapping(false),        // Prevent overlapping
    mita.WithShutdownTimeout(10*time.Second), // How long Stop waits before canceling tasks
    mita.WithContextValue("env", "prod"),    // Inject context values
    mita.WithOnTaskStart(func(e mita.TaskStartEvent) {
        log.Printf("starting %s", e.TaskName)
    }),
    mita.WithOnTaskComplete(func(e mita.TaskCompleteEvent) {
        if e.Error != nil {
            log.Printf("%s failed: %v", e.TaskName, e.Error)
        }
    }),
    mita.WithOnTaskSkip(func(e mita.TaskSkipEvent) {
        log.Printf("%s skipped: %v", e.TaskName, e.Reason)
    }),
)
```

## Schedule Expressions

### Using Builder API (Recommended)

```go
// Every second
mita.Every().Second()

// Every minute
mita.Every().Minute()

// Every hour
mita.Every().Hour()

// Every day
mita.Every().Day()

// Every N seconds (1-59)
mita.Every().Seconds(30)

// Every N minutes (1-59)
mita.Every().Minutes(15)

// Every N hours (1-23)
mita.Every().Hours(6)

// Every N days (1-31)
mita.Every().Days(2)

// Exact fixed-length interval (any duration >= 1s), using "@every"
mita.Every().Interval(90 * time.Second)
mita.Every().Interval(4 * time.Hour)

// Daily at specific time
mita.Every().Day().At(14, 30)  // 2:30 PM daily

// Specific weekday
mita.Every().Day().At(9, 0).OnWeekday(time.Monday)  // Monday 9:00 AM

// Specific day of month
mita.Every().Day().At(0, 0).OnDay(1)  // 1st of every month at midnight
```

Two things to know about intervals:

- `Seconds`/`Minutes`/`Hours` map to cron step expressions, which **reset at unit
  boundaries**: `Seconds(45)` fires at `:00` and `:45` of every minute (a 15s gap).
  Use `Interval` when you need exact fixed-length periods.
- Out-of-range steps are rejected: cron would silently misinterpret them
  (`*/90` in the seconds field fires every 60 seconds, not 90). Invalid builder
  arguments are recorded and returned as an error from `AddTask` — builder
  methods never panic. You can also check eagerly with `builder.Err()`.

### Using Raw Cron Expressions

```go
// 6-field format: second minute hour day month weekday
mita.Cron("0 30 * * * *")     // Every hour at 30 minutes
mita.Cron("0 0 2 * * *")      // Daily at 2:00 AM
mita.Cron("0 */15 * * * *")   // Every 15 minutes

// Standard 5-field expressions also work (run at second 0)
mita.Cron("*/15 * * * *")     // Every 15 minutes
mita.Cron("0 9 * * 1")        // Every Monday at 9:00 AM

// Descriptors
mita.Cron("@hourly")
mita.Cron("@every 90s")
```

Expressions are validated when the schedule is registered with `AddTask` or
`UpdateSchedule`.

## Configuration Options

### WithLogger

Set a custom logger:

```go
logger := log.New(os.Stdout, "[TASK] ", log.LstdFlags)
tm := mita.New(mita.WithLogger(logger))
```

### WithLocation

Set timezone for task execution:

```go
location, _ := time.LoadLocation("America/New_York")
tm := mita.New(mita.WithLocation(location))
```

### WithMaxConcurrent

Limit maximum concurrent running tasks (0 = unlimited). When the limit is reached,
scheduled executions are skipped and manual executions return
`ErrMaxConcurrencyReached`.

```go
tm := mita.New(mita.WithMaxConcurrent(3))
```

### WithAllowOverlapping

Control whether the same task can run concurrently (individual tasks can
override this with the `WithTaskOverlapping` task option):

```go
// Prevent same task from running multiple instances
tm := mita.New(mita.WithAllowOverlapping(false))

// Allow same task to run concurrently
tm := mita.New(mita.WithAllowOverlapping(true))
```

### WithShutdownTimeout

Control how long `Stop()` waits for running tasks to finish before canceling
their contexts (default: 30 seconds):

```go
tm := mita.New(mita.WithShutdownTimeout(10 * time.Second))
```

### WithContextValue

Inject static context values available to all tasks:

```go
tm := mita.New(
    mita.WithContextValue("database", dbConnection),
    mita.WithContextValue("cache", redisClient),
    mita.WithContextValue("env", "production"),
)
```

### WithContextInjector

Dynamically inject context values per execution:

```go
tm := mita.New(
    mita.WithContextInjector(func(ctx context.Context, taskName string) context.Context {
        // Generate unique ID for each execution
        ctx = context.WithValue(ctx, "request_id", uuid.New().String())
        ctx = context.WithValue(ctx, "timestamp", time.Now())
        return ctx
    }),
)
```

### WithOnTaskStart / WithOnTaskComplete / WithOnTaskSkip

Observe task lifecycle events:

```go
tm := mita.New(
    mita.WithOnTaskStart(func(e mita.TaskStartEvent) {
        log.Printf("START %s trigger=%s", e.TaskName, e.Trigger)
    }),
    mita.WithOnTaskComplete(func(e mita.TaskCompleteEvent) {
        if e.Error != nil {
            log.Printf("FAIL %s after %s: %v", e.TaskName, e.Duration, e.Error)
            return
        }
        log.Printf("DONE %s after %s", e.TaskName, e.Duration)
    }),
    mita.WithOnTaskSkip(func(e mita.TaskSkipEvent) {
        // Reason is ErrTaskRunning or ErrMaxConcurrencyReached
        log.Printf("SKIP %s: %v", e.TaskName, e.Reason)
    }),
)
```

Hooks are synchronous and observability-only. A hook panic is recovered and logged
so it does not affect task execution. Task panics are recovered, recorded as
failures, and surfaced through `TaskCompleteEvent.Error`.

`OnTaskSkip` fires when an execution is rejected by overlap prevention or the
concurrency limit — often exactly the situations you want to alert on.
Executions of disabled tasks are considered paused, not skipped, and do not
fire this hook.

## Task Management

### Adding Tasks

```go
err := tm.AddTask("backup", mita.Every().Day().At(2, 0), func(ctx context.Context) error {
    // Perform backup logic
    return nil
})
if err != nil {
    log.Fatal(err)
}
```

Adding a task with an existing name returns `ErrTaskExists`.

Per-task options can be appended:

```go
err := tm.AddTask("sync", mita.Every().Minutes(5), syncTask,
    mita.WithTaskTimeout(2*time.Minute),   // cancel the task context after 2 minutes
    mita.WithTaskOverlapping(true),        // override the manager-level overlap setting
)
```

`WithTaskTimeout` cancels the task's context when the timeout elapses; the task
must honor `ctx.Done()` for the timeout to take effect.

### Updating Schedules

Change a task's schedule in place — statistics and settings are preserved and
the new schedule takes effect immediately:

```go
err := tm.UpdateSchedule("backup", mita.Every().Day().At(3, 30))
```

An invalid schedule leaves the existing one untouched.

### Manual Execution

Trigger a task immediately outside its schedule:

```go
done, err := tm.RunTaskNow("backup")
if err != nil {
    log.Printf("Manual trigger failed: %v", err)
}
```

`RunTaskNow` returns after the execution is admitted, before the task finishes.
It reports submission errors such as `ErrTaskRunning`,
`ErrMaxConcurrencyReached`, and `ErrTaskManagerStopped`; use `errors.Is` to
check them. Disabled tasks **can** be triggered manually — disabling only
pauses the schedule.

The returned channel receives the task's result exactly once and can be ignored
for fire-and-forget triggers, or consumed to join the execution later:

```go
done, err := tm.RunTaskNow("backup")
if err != nil {
    return err
}
// ... do other work ...
if err := <-done; err != nil {
    log.Printf("backup failed: %v", err)
}
```

Use `RunTaskNowAndWait` when the caller should block until the task completes:

```go
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

if err := tm.RunTaskNowAndWait(ctx, "backup"); err != nil {
    log.Printf("Manual run failed: %v", err)
}
```

`RunTaskNowAndWait` uses the provided context for cancellation/deadline only; use
`WithContextValue` or `WithContextInjector` for values available inside tasks.
If the task is admitted and then the context is canceled, that execution counts
toward `RunCount` and, when the task returns the context error, `ErrorCount`.

### Disabling Tasks

Temporarily pause a task's schedule without removing it (manual triggers still
work while a task is disabled):

```go
err := tm.DisableTask("backup")
```

### Enabling Tasks

Re-enable a previously disabled task:

```go
err := tm.EnableTask("backup")
```

### Removing Tasks

Permanently remove a task:

```go
err := tm.RemoveTask("backup")
```

### Query Task Information

Get information about a specific task:

`GetTask` returns an immutable `TaskInfo` snapshot:

```go
taskInfo, err := tm.GetTask("backup")
if err == nil {
    fmt.Printf("Task: %s\n", taskInfo.Name)
    fmt.Printf("Schedule: %s\n", taskInfo.Schedule)
    fmt.Printf("Run Count: %d\n", taskInfo.RunCount)
    fmt.Printf("Error Count: %d\n", taskInfo.ErrorCount)
    fmt.Printf("Last Run: %s\n", taskInfo.LastRun)
    fmt.Printf("Next Run: %s\n", taskInfo.NextRun)
    fmt.Printf("Enabled: %v\n", taskInfo.Enabled)
    fmt.Printf("Running: %v\n", taskInfo.Running())
    fmt.Printf("Running Count: %d\n", taskInfo.RunningCount)
}
```

List all tasks (sorted by name):

```go
tasks := tm.ListTasks()
for _, task := range tasks {
    fmt.Printf("%s - Next: %s, Runs: %d, Errors: %d\n", 
        task.Name, task.NextRun, task.RunCount, task.ErrorCount)
}
```

### Statistics

Get aggregated statistics as a typed struct:

```go
stats := tm.Stats()
fmt.Printf("Total Tasks: %d\n", stats.TotalTasks)
fmt.Printf("Enabled Tasks: %d\n", stats.EnabledTasks)
fmt.Printf("Running Tasks: %d\n", stats.RunningTasks)
fmt.Printf("Total Runs: %d\n", stats.TotalRuns)
fmt.Printf("Total Errors: %d\n", stats.TotalErrors)
fmt.Printf("Max Concurrent: %d\n", stats.MaxConcurrent)
fmt.Printf("Allow Overlapping: %v\n", stats.AllowOverlapping)
```

## Working with Context

### Retrieving Task Name

```go
tm.AddTask("example", mita.Every().Minute(), func(ctx context.Context) error {
    taskName := mita.GetTaskName(ctx)
    fmt.Printf("Current task: %s\n", taskName)
    return nil
})
```

### Accessing Injected Values

```go
tm.AddTask("example", mita.Every().Minute(), func(ctx context.Context) error {
    // Get injected static context values
    db := mita.ContextValue(ctx, "database").(*sql.DB)
    env := mita.ContextValue(ctx, "env").(string)
    
    // Values added by your own WithContextInjector use your own key type
    requestID := ctx.Value(requestIDKey{}).(string)
    
    // Use injected values
    log.Printf("[%s] Processing in %s environment", requestID, env)
    rows, err := db.Query("SELECT * FROM users")
    // ...
    return nil
})
```

Values injected with `WithContextValue`/`SetContextValue` live under a private
key type, so they can never collide with the internal task name key or with
keys used by other libraries — always read them back with `mita.ContextValue`.

### Handling Context Cancellation

Always check for context cancellation in long-running tasks:

```go
tm.AddTask("long-running", mita.Every().Hour(), func(ctx context.Context) error {
    for i := 0; i < 100; i++ {
        select {
        case <-ctx.Done():
            // Task manager is shutting down
            log.Println("Task cancelled, cleaning up...")
            return ctx.Err()
        default:
            // Continue work
            time.Sleep(time.Second)
            // Process item i
        }
    }
    return nil
})
```

### Dynamic Context Updates

Update context values at runtime:

```go
// Set or update a context value
tm.SetContextValue("feature_flag", true)

// Retrieve a context value
value := tm.GetContextValue("feature_flag")
if enabled, ok := value.(bool); ok && enabled {
    // Feature is enabled
}
```

## Error Handling

Errors returned from task functions are automatically logged and tracked:

```go
tm.AddTask("api-call", mita.Every().Minutes(5), func(ctx context.Context) error {
    resp, err := http.Get("https://api.example.com/data")
    if err != nil {
        return fmt.Errorf("API call failed: %w", err)
    }
    defer resp.Body.Close()
    
    if resp.StatusCode != 200 {
        return fmt.Errorf("API returned error status: %d", resp.StatusCode)
    }
    
    // Process response
    return nil
})

// Later, check for errors
taskInfo, _ := tm.GetTask("api-call")
if taskInfo.LastError != "" {
    log.Printf("Task last failed with: %s", taskInfo.LastError)
    log.Printf("Error rate: %d/%d (%.1f%%)", 
        taskInfo.ErrorCount, 
        taskInfo.RunCount,
        float64(taskInfo.ErrorCount)/float64(taskInfo.RunCount)*100)
}
```

## Graceful Shutdown

The task manager shuts down in two phases: first it waits for running tasks to
finish naturally, and only when the deadline expires does it cancel their
contexts. Tasks that respect `ctx.Done()` are therefore never interrupted as
long as they finish within the timeout.

```go
func main() {
    tm := mita.New(mita.WithShutdownTimeout(15 * time.Second))
    // ... add tasks
    if err := tm.Start(); err != nil {
        log.Fatal(err)
    }
    
    // Listen for system signals
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
    <-sigChan
    
    // Gracefully shutdown
    if err := tm.Stop(); err != nil {
        log.Printf("shutdown: %v", err)
    }
}
```

The `Stop()` method:
1. Stops the cron scheduler and rejects new executions
2. Waits for all running tasks to complete, up to the shutdown timeout
   (`WithShutdownTimeout`, default 30 seconds)
3. If the deadline expires, cancels the task contexts and waits a short grace
   period for tasks to exit
4. Returns `nil` on a clean shutdown, or an error wrapping
   `context.DeadlineExceeded` if tasks had to be canceled

Use `StopContext(ctx)` to control the deadline with your own context — handy
when coordinating with an HTTP server shutdown:

```go
shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
defer cancel()
_ = server.Shutdown(shutdownCtx)
_ = tm.StopContext(shutdownCtx)
```

`Stop()` is terminal: after it is called, the manager cannot be restarted and
new tasks or manual executions are rejected with `ErrTaskManagerStopped`.
Calling `Start()` twice returns `ErrTaskManagerStarted`; calling `Stop()` twice
returns `ErrTaskManagerStopped`. Use `IsStarted()` to query the state.

## Web Management Interface

The task manager includes a built-in web interface for monitoring and managing tasks through your browser.

### Starting the Web Server

```go
package main

import (
    "net/http"
    "github.com/cymoo/mita"
)

func main() {
    tm := mita.New()
    
    // Add your tasks
    tm.AddTask("example", mita.Every().Minute(), func(ctx context.Context) error {
        // Task logic
        return nil
    })
    
    tm.Start()
    
    // Create web handler mounted at /tasks
    mux := tm.WebHandler("/tasks")
    
    // Start HTTP server
    http.ListenAndServe(":8080", mux)
}
```

### Web Interface Features

The web interface is a single live dashboard (light & dark themes, follows the
system preference) with:

- **Overview** — big-number stats: tasks, running now, executions, errors, success rate
- **Schedule board** — one row per task with a live *horizon timeline*: upcoming
  fires drift toward the NOW line in real time (5m / 15m / 1h window)
- **Activity feed** — live stream of completions, failures, **skips**
  (overlap/concurrency rejections), and lifecycle events
- **Task drawer** — click a task for details: schedule editing with live
  validation and next-fire preview (`UpdateSchedule`), execution policy
  (per-task timeout, overlap), statistics, recent events, and removal
- **Actions** — run now, pause/resume (manual runs still work while paused), remove

The JSON API behind it:

| Route | Description |
|---|---|
| `GET  {base}/api/state?window=SECONDS` | Full snapshot: stats, tasks, upcoming runs, events |
| `GET  {base}/api/schedule/preview?expr=...` | Validate an expression, get next fires |
| `POST {base}/api/tasks/{name}/{action}` | `run` \| `pause` \| `resume` \| `remove` \| `schedule` (JSON body `{"expr":"..."}`) |

Errors use meaningful status codes: 404 unknown task, 409 overlap/concurrency
conflict, 400 invalid input, plus a JSON body `{"error": "..."}`.

### Mounting at Custom Paths

```go
// Mount at root
mux := tm.WebHandler("/")

// Mount at custom path
mux := tm.WebHandler("/admin/tasks")

// Integrate with existing HTTP server
existingMux := http.NewServeMux()
existingMux.Handle("/api/", apiHandler)
existingMux.Handle("/tasks/", tm.WebHandler("/tasks"))
```

### Example with Authentication

```go
func authMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // Check authentication
        if !isAuthenticated(r) {
            http.Error(w, "Unauthorized", http.StatusUnauthorized)
            return
        }
        next.ServeHTTP(w, r)
    })
}

func main() {
    tm := mita.New()
    tm.Start()
    
    mux := tm.WebHandler("/tasks")
    
    // Wrap with authentication
    http.ListenAndServe(":8080", authMiddleware(mux))
}
```

### Integrating with Existing Applications

```go
// Chi router
r := chi.NewRouter()
r.Mount("/", tm.WebHandler("/tasks"))

// Gin
router := gin.Default()
router.Any("/*any", gin.WrapH(tm.WebHandler("/tasks")))

// Echo
e := echo.New()
e.Any("/*", echo.WrapHandler(tm.WebHandler("/tasks")))
```

## Complete Example

See `_examples` for a comprehensive, runnable example that demonstrates:

- Multiple scheduling strategies
- Concurrency control and overlap prevention
- Error handling with simulated failures
- Context injection (static and dynamic)
- Task management operations (enable/disable)
- Statistics and monitoring
- Long-running tasks with cancellation
- Graceful shutdown handling
- Web interface integration

Run the example:

```bash
go run _examples/main.go
```

## Best Practices

### 1. Set Appropriate Concurrency Limits

```go
// For CPU-intensive tasks
tm := mita.New(mita.WithMaxConcurrent(runtime.NumCPU()))

// For I/O-bound tasks
tm := mita.New(mita.WithMaxConcurrent(20))
```

### 2. Handle Context Cancellation

Always respect context cancellation in long-running tasks:

```go
func(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            // Do work
        }
    }
}
```

### 3. Return Meaningful Errors

```go
func(ctx context.Context) error {
    if err := doWork(); err != nil {
        return fmt.Errorf("failed to process batch %d: %w", batchID, err)
    }
    return nil
}
```

### 4. Prevent Overlapping for Critical Tasks

```go
tm := mita.New(mita.WithAllowOverlapping(false))
```

### 5. Monitor Task Health

```go
// Periodically check task statistics
ticker := time.NewTicker(5 * time.Minute)
go func() {
    for range ticker.C {
        stats := tm.Stats()
        if stats.TotalRuns == 0 {
            continue
        }
        errorRate := float64(stats.TotalErrors) / float64(stats.TotalRuns)
        if errorRate > 0.1 { // More than 10% errors
            alert("High task error rate detected")
        }
    }
}()
```

Or push-based, using the hooks: `WithOnTaskComplete` for failures and
`WithOnTaskSkip` for executions rejected by overlap/concurrency rules.

### 6. Use Timeouts for External Calls

Prefer the per-task option so the timeout also shows up in error statistics:

```go
tm.AddTask("api-call", schedule, func(ctx context.Context) error {
    req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
    resp, err := client.Do(req)
    // ...
}, mita.WithTaskTimeout(30*time.Second))
```

### 7. Clean Up Resources

```go
tm.AddTask("db-task", schedule, func(ctx context.Context) error {
    conn := pool.Get()
    defer conn.Close()
    
    // Use connection
    return nil
})
```

## Performance Considerations

- **Memory Usage**: Each task stores minimal metadata (~200 bytes)
- **Goroutines**: One goroutine per concurrent task execution
- **Lock Contention**: Read-write locks minimize contention on task metadata
- **Cron Performance**: Uses the highly optimized `robfig/cron` library

## Thread Safety

All mita methods are thread-safe and can be called concurrently:

```go
// Safe to call from multiple goroutines
go tm.AddTask(name1, schedule1, task1)
go tm.AddTask(name2, schedule2, task2)
go tm.RunTaskNow(name1)
go tm.Stats()
```

## Scope & Limitations

mita is deliberately a *minimum* scheduling library. Out of scope (by design):

- **Retry/backoff policies** — wrap your task function if you need retries
- **Persistence** — schedules and statistics live in memory only
- **Distributed coordination** — for multi-instance deployments, add your own
  distributed lock inside the task

Other limitations:

- Task names must be unique (`ErrTaskExists` otherwise)
- A stopped manager cannot be restarted; create a new one
- Context values are copied, not referenced (use pointers for shared state)

## FAQ

**Q: Can I update a task's schedule without removing it?**  
A: Yes — `tm.UpdateSchedule(name, schedule)` swaps the schedule in place and preserves statistics.

**Q: What happens if a task is already running when triggered manually?**  
A: If overlapping is not allowed (manager default, or per-task `WithTaskOverlapping(false)`), you'll get `ErrTaskRunning`. If allowed, both instances run.

**Q: Can I manually run a disabled task?**  
A: Yes. Disabling only pauses the schedule; `RunTaskNow` and `RunTaskNowAndWait` are explicit requests and always work.

**Q: How do I handle tasks that might run longer than their interval?**  
A: Keep overlapping disabled to skip executions while the previous one is still running, and consider `WithTaskTimeout` to bound each execution. Use `WithOnTaskSkip` to observe the skips.

**Q: Can I pause the entire task manager?**  
A: Not directly. You can disable all tasks individually. Note a stopped manager cannot be restarted.

**Q: Is it safe to modify context values during execution?**  
A: Use `SetContextValue()` to update values. Changes apply to new executions, not running ones.

**Q: Can I customize the web interface?**
A: The web interface is embedded in the library. For customization, you can build your own interface using its API methods.

**Q: Is the web interface secure?**
A: The web interface has no built-in authentication. Always add authentication middleware when exposing it publicly (see examples above).

## Testing

To test your tasks deterministically, trigger them manually instead of waiting
for the schedule:

```go
func TestMyTask(t *testing.T) {
    tm := mita.New()
    defer tm.Stop()

    var executed atomic.Bool
    if err := tm.AddTask("test", mita.Every().Minute(), func(ctx context.Context) error {
        executed.Store(true)
        return nil
    }); err != nil {
        t.Fatal(err)
    }

    if err := tm.RunTaskNowAndWait(context.Background(), "test"); err != nil {
        t.Fatal(err)
    }
    if !executed.Load() {
        t.Error("Task was not executed")
    }
}
```

## Migrating from v0.x

Breaking changes in this version:

| Before | After |
|---|---|
| `tm.GetStats()` returns `map[string]interface{}` | `tm.Stats()` returns a typed `Stats` struct |
| `tm.RunTaskNow(name) error` | `tm.RunTaskNow(name) (<-chan error, error)` — the channel carries the task result and can be ignored |
| `tm.GetTask` returns `*TaskInfo` | returns a `TaskInfo` value snapshot |
| `TaskInfo.Running` field | `TaskInfo.Running()` method; `Task` and `EntryID` fields removed |
| `tm.IsRunning()` | `tm.IsStarted()` |
| `tm.Start()` / `tm.Stop()` return nothing | both return `error`; `StopContext(ctx)` added |
| `Stop()` cancels task contexts immediately | two-phase: waits first, cancels after the shutdown timeout |
| `ScheduleBuilder` panics on invalid input | records the error; surfaced by `builder.Err()` and `AddTask` |
| `Every().Seconds(90)` accepted (misfired every 60s) | rejected; use `Every().Interval(90 * time.Second)` |
| `CtxtKey` exported key type | removed; use `mita.ContextValue(ctx, key)` to read injected values |
| `RunTaskNow` on a disabled task returns `ErrTaskDisabled` | manual triggers are allowed on disabled tasks |
| duplicate `AddTask` returns a plain error | returns `ErrTaskExists` (checkable with `errors.Is`) |

## License

MIT License - see [LICENSE](LICENSE) file for details.
