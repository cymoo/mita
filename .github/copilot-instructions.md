# Copilot Instructions

## Build, Test & Format

```bash
# Build the library
go build ./...

# Run all tests
go test ./...

# Run a single test
go test -run '^TestTaskExecution$' .

# Format modified Go files
gofmt -w <files>

# Run the example app (web UI at http://localhost:8080/tasks)
go run _examples/main.go
```

## Architecture

`mita` is a single-package Go library (`package mita`) for scheduled task management. All public API lives in two files:

- **`mita.go`** — Core: `TaskManager`, `TaskInfo`, `Schedule` interface, `ScheduleBuilder` (fluent API), `CronSchedule` (raw expressions), all lifecycle and concurrency logic.
- **`web.go` + `ui/`** — HTTP web UI mounted via `tm.WebHandler(baseURL)`. `web.go` owns routing/data/actions; `ui/*.html` and `ui/*.css` are embedded with `go:embed`.

The only external dependency is `github.com/robfig/cron/v3`, used as the underlying cron engine.

**Lifecycle:** `New(opts...)` creates the manager → `AddTask(...)` registers tasks → `Start()` begins scheduling → `Stop()` gracefully shuts down (30 s timeout).

**Concurrency model:**
- A `sync.RWMutex` guards all reads/writes to `tasks` map and `TaskInfo` fields. Read operations use `RLock`; mutations use `Lock`.
- Optional global concurrency cap uses a channel-based semaphore (`chan struct{}`).
- Per-task overlap prevention is checked via `TaskInfo.Running` under the write lock.

## Key Conventions

**Cron expression format is 6 fields (seconds included):**
```
second minute hour day month weekday
```
This differs from standard 5-field cron. `Every()` and `Cron()` both produce this format.

**`ScheduleBuilder` panics (not errors) on invalid builder inputs** (e.g., non-positive interval, hour out of 0–23). Raw `Cron(...)` expressions are validated when `AddTask` calls into `robfig/cron`.

**Context value keys use `CtxtKey` (a typed `string`):**
```go
// Injecting
mita.WithContextValue("db", dbConn)

// Retrieving inside a task
db := ctx.Value(mita.CtxtKey("db")).(*sql.DB)
```
The task name is stored under a private `taskNameKey`; retrieve it with `mita.GetTaskName(ctx)`.

**`contextInjector` runs after static values are applied** and must not hold any locks (it is called outside the mutex). Static values are snapshot-copied before injection to avoid holding the lock during context building.

**Test pattern:** table-driven tests, always `defer tm.Stop()` for cleanup. Tests that exercise execution call `tm.Start()` explicitly; configuration-only tests do not.

**`GetTask` and `ListTasks` return copies** of `TaskInfo` structs (value copy via `infoCopy := *info`) so callers cannot mutate internal state.
