package mita

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// taskNameKey is the context key type for the task name. It is a private
// struct type so user-provided context values can never collide with it.
type taskNameKey struct{}

// valueKey is the context key type for values injected with WithContextValue
// and SetContextValue. Retrieve them with ContextValue.
type valueKey string

// Task represents a function that performs work within a given context.
// It should return an error if the task execution fails.
type Task func(ctx context.Context) error

// Public task manager errors. Use errors.Is to check wrapped errors returned by APIs.
var (
	ErrTaskNotFound          = errors.New("task not found")
	ErrTaskExists            = errors.New("task already exists")
	ErrTaskDisabled          = errors.New("task disabled")
	ErrTaskRunning           = errors.New("task already running")
	ErrMaxConcurrencyReached = errors.New("max concurrency reached")
	ErrTaskManagerStarted    = errors.New("task manager already started")
	ErrTaskManagerStopped    = errors.New("task manager stopped")
	ErrNilContext            = errors.New("nil context")
)

// DefaultShutdownTimeout is how long Stop waits for running tasks to finish
// before canceling their contexts. Override with WithShutdownTimeout.
const DefaultShutdownTimeout = 30 * time.Second

// forceStopGrace is how long StopContext waits for tasks to exit after their
// contexts have been canceled.
const forceStopGrace = 5 * time.Second

// TaskTrigger identifies how a task execution was triggered.
type TaskTrigger string

const (
	TriggerScheduled TaskTrigger = "scheduled"
	TriggerManual    TaskTrigger = "manual"
)

type managerState int

const (
	managerCreated managerState = iota
	managerStarted
	managerStopped
)

// TaskStartEvent is emitted immediately before a task function starts.
type TaskStartEvent struct {
	TaskName     string
	Trigger      TaskTrigger
	StartedAt    time.Time
	Context      context.Context
	RunCount     int64
	RunningCount int
}

// TaskCompleteEvent is emitted after a task function finishes and internal state is updated.
type TaskCompleteEvent struct {
	TaskName     string
	Trigger      TaskTrigger
	StartedAt    time.Time
	FinishedAt   time.Time
	Duration     time.Duration
	Error        error
	RunCount     int64
	ErrorCount   int64
	RunningCount int
}

// TaskSkipEvent is emitted when an execution is skipped because the task is
// already running (overlap prevention) or the concurrency limit is reached.
// Executions of disabled tasks are considered paused, not skipped, and do not
// emit this event.
type TaskSkipEvent struct {
	TaskName  string
	Trigger   TaskTrigger
	SkippedAt time.Time
	Reason    error // ErrTaskRunning or ErrMaxConcurrencyReached
}

// TaskInfo is an immutable snapshot of a task's metadata and statistics.
type TaskInfo struct {
	Name         string    // Unique identifier for the task
	Schedule     string    // Cron expression for the task schedule
	AddedAt      time.Time // When the task was added to the manager
	LastRun      time.Time // Start time of the most recent execution
	NextRun      time.Time // Next scheduled execution time
	RunCount     int64     // Total number of executions
	ErrorCount   int64     // Total number of failed executions
	LastError    string    // Most recent error message (empty if last run succeeded)
	Enabled      bool      // Whether the task is enabled for scheduled execution
	RunningCount int       // Number of currently executing instances
}

// Running reports whether the task had at least one executing instance when
// the snapshot was taken.
func (t TaskInfo) Running() bool {
	return t.RunningCount > 0
}

// Stats holds aggregated statistics about the task manager and all tasks.
type Stats struct {
	TotalTasks       int
	EnabledTasks     int
	RunningTasks     int
	TotalRuns        int64
	TotalErrors      int64
	MaxConcurrent    int
	AllowOverlapping bool
}

// taskEntry is the internal, mutable state of a registered task.
type taskEntry struct {
	name         string
	schedule     string
	task         Task
	entryID      cron.EntryID
	addedAt      time.Time
	lastRun      time.Time
	runCount     int64
	errorCount   int64
	lastError    string
	enabled      bool
	runningCount int
	settings     taskSettings
}

// snapshot returns a copy of the entry's public state.
func (e *taskEntry) snapshot(nextRun time.Time) TaskInfo {
	return TaskInfo{
		Name:         e.name,
		Schedule:     e.schedule,
		AddedAt:      e.addedAt,
		LastRun:      e.lastRun,
		NextRun:      nextRun,
		RunCount:     e.runCount,
		ErrorCount:   e.errorCount,
		LastError:    e.lastError,
		Enabled:      e.enabled,
		RunningCount: e.runningCount,
	}
}

// TaskManager orchestrates scheduled task execution with concurrency control,
// error tracking, and flexible configuration options.
type TaskManager struct {
	cron             *cron.Cron                                                 // Underlying cron scheduler
	tasks            map[string]*taskEntry                                      // Map of task name to task state
	mu               sync.RWMutex                                               // Protects tasks map and task state
	ctx              context.Context                                            // Manager lifecycle context
	cancel           context.CancelFunc                                         // Function to cancel the manager context
	logger           *log.Logger                                                // Logger for task execution events
	wg               sync.WaitGroup                                             // Tracks running tasks for graceful shutdown
	maxConcurrent    int                                                        // Maximum concurrent tasks (0 = unlimited)
	semaphore        chan struct{}                                              // Channel-based semaphore for concurrency control
	allowOverlapping bool                                                       // Whether same task can run concurrently
	shutdownTimeout  time.Duration                                              // How long Stop waits before canceling tasks
	location         *time.Location                                             // Timezone for cron schedule interpretation
	contextValues    map[string]any                                             // Static values to inject into task contexts
	contextInjector  func(ctx context.Context, taskName string) context.Context // Dynamic context injection
	onTaskStart      func(TaskStartEvent)                                       // Called before a task starts
	onTaskComplete   func(TaskCompleteEvent)                                    // Called after a task completes
	onTaskSkip       func(TaskSkipEvent)                                        // Called when an execution is skipped
	state            managerState                                               // Manager lifecycle state
}

// taskSettings holds per-task configuration applied via TaskOption.
type taskSettings struct {
	timeout time.Duration // 0 = no timeout
	overlap *bool         // nil = inherit the manager-level setting
}

type taskExecution struct {
	name         string
	trigger      TaskTrigger
	task         Task
	timeout      time.Duration
	startedAt    time.Time
	runCount     int64
	runningCount int
	acquiredSlot bool
}

// cronParser parses 6-field (with seconds) cron expressions and descriptors.
var cronParser = cron.NewParser(
	cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// Option is a functional option for configuring TaskManager.
type Option func(*TaskManager)

// WithLogger sets a custom logger for the task manager.
// If not provided, log.Default() will be used.
func WithLogger(logger *log.Logger) Option {
	return func(tm *TaskManager) {
		if logger != nil {
			tm.logger = logger
		}
	}
}

// WithLocation sets the timezone for cron schedule interpretation.
// If not provided, the local timezone is used.
func WithLocation(loc *time.Location) Option {
	return func(tm *TaskManager) {
		if loc != nil {
			tm.location = loc
		}
	}
}

// WithMaxConcurrent sets the maximum number of tasks that can run concurrently.
// A value of 0 means unlimited concurrency. Negative values are treated as 0.
// When the limit is reached, scheduled executions are skipped and manual runs return ErrMaxConcurrencyReached.
func WithMaxConcurrent(max int) Option {
	return func(tm *TaskManager) {
		if max < 0 {
			max = 0
		}
		tm.maxConcurrent = max
		if max > 0 {
			tm.semaphore = make(chan struct{}, max)
		} else {
			tm.semaphore = nil
		}
	}
}

// WithAllowOverlapping controls whether the same task can run multiple instances concurrently.
// By default, overlapping is not allowed (false). Individual tasks can override
// this with WithTaskOverlapping.
func WithAllowOverlapping(allow bool) Option {
	return func(tm *TaskManager) {
		tm.allowOverlapping = allow
	}
}

// WithShutdownTimeout sets how long Stop waits for running tasks to finish
// before canceling their contexts. The default is DefaultShutdownTimeout.
// Non-positive values are ignored.
func WithShutdownTimeout(d time.Duration) Option {
	return func(tm *TaskManager) {
		if d > 0 {
			tm.shutdownTimeout = d
		}
	}
}

// WithContextValue adds a static key-value pair that will be injected into all
// task contexts. Retrieve it inside tasks with ContextValue.
func WithContextValue(key string, value any) Option {
	return func(tm *TaskManager) {
		if key == "" {
			return
		}
		if tm.contextValues == nil {
			tm.contextValues = make(map[string]any)
		}
		tm.contextValues[key] = value
	}
}

// WithContextInjector sets a custom function to dynamically inject values into task contexts.
// The injector is called for each task execution and receives the base context and task name.
func WithContextInjector(injector func(ctx context.Context, taskName string) context.Context) Option {
	return func(tm *TaskManager) {
		tm.contextInjector = injector
	}
}

// WithOnTaskStart sets a hook called immediately before each task execution starts.
func WithOnTaskStart(hook func(TaskStartEvent)) Option {
	return func(tm *TaskManager) {
		tm.onTaskStart = hook
	}
}

// WithOnTaskComplete sets a hook called after each task execution finishes.
func WithOnTaskComplete(hook func(TaskCompleteEvent)) Option {
	return func(tm *TaskManager) {
		tm.onTaskComplete = hook
	}
}

// WithOnTaskSkip sets a hook called when an execution is skipped due to
// overlap prevention or the concurrency limit.
func WithOnTaskSkip(hook func(TaskSkipEvent)) Option {
	return func(tm *TaskManager) {
		tm.onTaskSkip = hook
	}
}

// TaskOption is a functional option for configuring an individual task in AddTask.
type TaskOption func(*taskSettings)

// WithTaskTimeout sets a per-execution timeout for the task. The task context
// is canceled when the timeout elapses; tasks must honor ctx.Done() for the
// timeout to take effect. Non-positive values are ignored.
func WithTaskTimeout(d time.Duration) TaskOption {
	return func(s *taskSettings) {
		if d > 0 {
			s.timeout = d
		}
	}
}

// WithTaskOverlapping overrides the manager-level overlap setting for this task.
func WithTaskOverlapping(allow bool) TaskOption {
	return func(s *taskSettings) {
		s.overlap = &allow
	}
}

// New creates a new TaskManager with the given options.
// The manager must be started with Start() before tasks will execute.
func New(opts ...Option) *TaskManager {
	ctx, cancel := context.WithCancel(context.Background())

	tm := &TaskManager{
		tasks:           make(map[string]*taskEntry),
		ctx:             ctx,
		cancel:          cancel,
		logger:          log.Default(),
		shutdownTimeout: DefaultShutdownTimeout,
	}

	// Apply functional options
	for _, opt := range opts {
		opt(tm)
	}

	cronOpts := []cron.Option{cron.WithParser(cronParser)}
	if tm.location != nil {
		cronOpts = append(cronOpts, cron.WithLocation(tm.location))
	}
	tm.cron = cron.New(cronOpts...)

	return tm
}

// AddTask registers a new task with the given name and schedule.
// Returns ErrTaskExists if a task with the same name already exists, or an
// error if the schedule is invalid.
func (tm *TaskManager) AddTask(name string, schedule Schedule, task Task, opts ...TaskOption) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if schedule == nil {
		return fmt.Errorf("schedule cannot be nil")
	}
	if task == nil {
		return fmt.Errorf("task function cannot be nil")
	}

	expr, err := scheduleExpr(schedule)
	if err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}

	var settings taskSettings
	for _, opt := range opts {
		opt(&settings)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.state == managerStopped {
		return fmt.Errorf("cannot add task %q: %w", name, ErrTaskManagerStopped)
	}

	if _, exists := tm.tasks[name]; exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskExists)
	}

	// Add to cron scheduler with execution tracking, error handling,
	// concurrency control, and overlap prevention.
	entryID, err := tm.cron.AddFunc(expr, tm.wrapTask(name))
	if err != nil {
		return fmt.Errorf("failed to add task %q: %w", name, err)
	}

	tm.tasks[name] = &taskEntry{
		name:     name,
		schedule: expr,
		task:     task,
		entryID:  entryID,
		addedAt:  time.Now(),
		enabled:  true,
		settings: settings,
	}

	tm.logger.Printf("Task '%s' added with schedule: %s", name, expr)
	return nil
}

// UpdateSchedule changes the schedule of an existing task while preserving
// its statistics and settings. The new schedule takes effect immediately.
func (tm *TaskManager) UpdateSchedule(name string, schedule Schedule) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if schedule == nil {
		return fmt.Errorf("schedule cannot be nil")
	}

	expr, err := scheduleExpr(schedule)
	if err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.state == managerStopped {
		return fmt.Errorf("cannot update task %q: %w", name, ErrTaskManagerStopped)
	}

	e, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	// Register the new entry first so an invalid expression leaves the
	// existing schedule untouched.
	entryID, err := tm.cron.AddFunc(expr, tm.wrapTask(name))
	if err != nil {
		return fmt.Errorf("failed to update task %q: %w", name, err)
	}
	tm.cron.Remove(e.entryID)
	e.entryID = entryID
	e.schedule = expr

	tm.logger.Printf("Task '%s' schedule updated to: %s", name, expr)
	return nil
}

// wrapTask adapts a named task for the cron scheduler.
func (tm *TaskManager) wrapTask(name string) func() {
	return func() {
		exec, err := tm.beginExecution(name, TriggerScheduled)
		if err != nil {
			tm.noteSkip(name, TriggerScheduled, err)
			return
		}
		_ = tm.runExecution(exec, nil)
	}
}

// beginExecution admits an execution: it checks manager and task state,
// applies overlap and concurrency rules, and records the start.
func (tm *TaskManager) beginExecution(name string, trigger TaskTrigger) (*taskExecution, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.state == managerStopped {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskManagerStopped)
	}

	e := tm.tasks[name]
	if e == nil {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}
	// Disabled tasks pause scheduled executions only; manual triggers are
	// an explicit request and always allowed.
	if trigger == TriggerScheduled && !e.enabled {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskDisabled)
	}

	allowOverlap := tm.allowOverlapping
	if e.settings.overlap != nil {
		allowOverlap = *e.settings.overlap
	}
	if !allowOverlap && e.runningCount > 0 {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskRunning)
	}

	acquiredSlot := false
	if tm.semaphore != nil {
		select {
		case tm.semaphore <- struct{}{}:
			acquiredSlot = true
		default:
			return nil, fmt.Errorf("task %q: %w", name, ErrMaxConcurrencyReached)
		}
	}

	now := time.Now()
	e.runningCount++
	e.lastRun = now
	e.runCount++
	tm.wg.Add(1)

	return &taskExecution{
		name:         name,
		trigger:      trigger,
		task:         e.task,
		timeout:      e.settings.timeout,
		startedAt:    now,
		runCount:     e.runCount,
		runningCount: e.runningCount,
		acquiredSlot: acquiredSlot,
	}, nil
}

// noteSkip logs and reports executions rejected by overlap prevention or the
// concurrency limit. Other admission errors are not skips and stay silent.
func (tm *TaskManager) noteSkip(name string, trigger TaskTrigger, err error) {
	if !errors.Is(err, ErrTaskRunning) && !errors.Is(err, ErrMaxConcurrencyReached) {
		return
	}
	if trigger == TriggerScheduled {
		tm.logger.Printf("Task '%s' skipped: %v", name, err)
	}
	if tm.onTaskSkip == nil {
		return
	}
	defer tm.recoverHookPanic("OnTaskSkip", name)
	tm.onTaskSkip(TaskSkipEvent{
		TaskName:  name,
		Trigger:   trigger,
		SkippedAt: time.Now(),
		Reason:    err,
	})
}

// runExecution runs an admitted execution to completion.
// The optional callerCtx only propagates cancellation; its values are not
// part of the task context.
func (tm *TaskManager) runExecution(exec *taskExecution, callerCtx context.Context) error {
	defer func() {
		if exec.acquiredSlot {
			<-tm.semaphore
		}
		tm.wg.Done()
	}()

	ctx, cleanup, err := tm.safeTaskContext(exec.name, callerCtx)
	if err != nil {
		finishedAt := time.Now()
		completeEvent := tm.completeExecution(exec, finishedAt, err)
		tm.logger.Printf("Task '%s' failed before start after %v: %v", exec.name, completeEvent.Duration, err)
		tm.fireTaskComplete(completeEvent)
		return err
	}
	defer cleanup()

	if exec.timeout > 0 {
		var cancelTimeout context.CancelFunc
		ctx, cancelTimeout = context.WithTimeout(ctx, exec.timeout)
		defer cancelTimeout()
	}

	tm.fireTaskStart(TaskStartEvent{
		TaskName:     exec.name,
		Trigger:      exec.trigger,
		StartedAt:    exec.startedAt,
		Context:      ctx,
		RunCount:     exec.runCount,
		RunningCount: exec.runningCount,
	})

	err = tm.callTask(exec.task, ctx)
	finishedAt := time.Now()
	completeEvent := tm.completeExecution(exec, finishedAt, err)

	if err != nil {
		tm.logger.Printf("Task '%s' failed after %v: %v", exec.name, completeEvent.Duration, err)
	} else {
		tm.logger.Printf("Task '%s' completed successfully in %v", exec.name, completeEvent.Duration)
	}

	tm.fireTaskComplete(completeEvent)
	return err
}

func (tm *TaskManager) callTask(task Task, ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
		}
	}()
	return task(ctx)
}

func (tm *TaskManager) safeTaskContext(name string, callerCtx context.Context) (ctx context.Context, cleanup func(), err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
			cleanup = func() {}
		}
	}()
	base := tm.getTaskContext(name)
	ctx, cleanup = withCallerCancellation(base, callerCtx)
	return ctx, cleanup, nil
}

// withCallerCancellation derives a context from base that is also canceled
// when the caller's context is canceled.
func withCallerCancellation(base context.Context, caller context.Context) (context.Context, func()) {
	if caller == nil || caller.Done() == nil {
		return base, func() {}
	}

	ctx, cancel := context.WithCancel(base)
	stop := context.AfterFunc(caller, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

func panicAsError(value any) error {
	if err, ok := value.(error); ok {
		return fmt.Errorf("task panic: %w", err)
	}
	return fmt.Errorf("task panic: %v", value)
}

func (tm *TaskManager) completeExecution(exec *taskExecution, finishedAt time.Time, taskErr error) TaskCompleteEvent {
	event := TaskCompleteEvent{
		TaskName:   exec.name,
		Trigger:    exec.trigger,
		StartedAt:  exec.startedAt,
		FinishedAt: finishedAt,
		Duration:   finishedAt.Sub(exec.startedAt),
		Error:      taskErr,
		RunCount:   exec.runCount,
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if e, ok := tm.tasks[exec.name]; ok {
		if taskErr != nil {
			e.errorCount++
			e.lastError = taskErr.Error()
		} else {
			e.lastError = ""
		}
		if e.runningCount > 0 {
			e.runningCount--
		}
		event.ErrorCount = e.errorCount
		event.RunningCount = e.runningCount
	}

	return event
}

func (tm *TaskManager) fireTaskStart(event TaskStartEvent) {
	if tm.onTaskStart == nil {
		return
	}
	defer tm.recoverHookPanic("OnTaskStart", event.TaskName)
	tm.onTaskStart(event)
}

func (tm *TaskManager) fireTaskComplete(event TaskCompleteEvent) {
	if tm.onTaskComplete == nil {
		return
	}
	defer tm.recoverHookPanic("OnTaskComplete", event.TaskName)
	tm.onTaskComplete(event)
}

func (tm *TaskManager) recoverHookPanic(hookName, taskName string) {
	if r := recover(); r != nil {
		tm.logger.Printf("%s hook for task '%s' panicked: %v", hookName, taskName, r)
	}
}

// RemoveTask removes a task from the manager.
// Returns an error if the task does not exist.
func (tm *TaskManager) RemoveTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	e, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	tm.cron.Remove(e.entryID)
	delete(tm.tasks, name)
	tm.logger.Printf("Task '%s' removed", name)
	return nil
}

// EnableTask enables a previously disabled task.
// The task will resume executing on its schedule.
func (tm *TaskManager) EnableTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	e, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	e.enabled = true
	tm.logger.Printf("Task '%s' enabled", name)
	return nil
}

// DisableTask disables a task without removing it.
// Scheduled executions are paused until the task is re-enabled;
// manual triggers still work.
func (tm *TaskManager) DisableTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	e, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	e.enabled = false
	tm.logger.Printf("Task '%s' disabled", name)
	return nil
}

// GetTask returns a snapshot of the task information for the given task name.
// Returns an error if the task does not exist.
func (tm *TaskManager) GetTask(name string) (TaskInfo, error) {
	if name == "" {
		return TaskInfo{}, fmt.Errorf("task name cannot be empty")
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	e, exists := tm.tasks[name]
	if !exists {
		return TaskInfo{}, fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	return e.snapshot(tm.cron.Entry(e.entryID).Next), nil
}

// ListTasks returns a snapshot of all task information, sorted by task name.
func (tm *TaskManager) ListTasks() []TaskInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	// One Entries() call gets every next-run time; per-task cron.Entry
	// lookups would each copy the full entry list.
	nextRuns := make(map[cron.EntryID]time.Time, len(tm.tasks))
	for _, entry := range tm.cron.Entries() {
		nextRuns[entry.ID] = entry.Next
	}

	tasks := make([]TaskInfo, 0, len(tm.tasks))
	for _, e := range tm.tasks {
		tasks = append(tasks, e.snapshot(nextRuns[e.entryID]))
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })

	return tasks
}

// Start begins the task scheduler.
// Tasks will start executing according to their schedules.
// Returns ErrTaskManagerStarted if already started, or ErrTaskManagerStopped
// if the manager has been stopped (a stopped manager cannot be restarted).
func (tm *TaskManager) Start() error {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	switch tm.state {
	case managerStarted:
		return ErrTaskManagerStarted
	case managerStopped:
		return ErrTaskManagerStopped
	}
	tm.state = managerStarted
	tm.cron.Start()
	tm.logger.Println("Task manager started")
	return nil
}

// Stop gracefully shuts down the task manager: it stops accepting new
// executions, waits up to the shutdown timeout (WithShutdownTimeout,
// DefaultShutdownTimeout by default) for running tasks to finish, and only
// then cancels their contexts. Stop is terminal; the manager cannot be
// restarted afterwards.
func (tm *TaskManager) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), tm.shutdownTimeout)
	defer cancel()
	return tm.StopContext(ctx)
}

// StopContext is like Stop but waits until ctx is done instead of the
// configured shutdown timeout. When ctx expires before all tasks finish,
// their contexts are canceled, StopContext waits a short grace period for
// them to exit, and returns ctx's error. Returns ErrTaskManagerStopped if
// the manager was already stopped.
func (tm *TaskManager) StopContext(ctx context.Context) error {
	tm.mu.Lock()
	if tm.state == managerStopped {
		tm.mu.Unlock()
		return ErrTaskManagerStopped
	}
	tm.state = managerStopped
	tm.cron.Stop()
	tm.mu.Unlock()

	tm.logger.Println("Stopping task manager...")

	done := make(chan struct{})
	go func() {
		tm.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		tm.cancel()
		tm.logger.Println("All tasks completed gracefully")
		return nil
	case <-ctx.Done():
		// Deadline reached: signal running tasks and give them a moment to exit.
		tm.cancel()
		select {
		case <-done:
			tm.logger.Println("Tasks canceled and exited after shutdown deadline")
		case <-time.After(forceStopGrace):
			tm.logger.Println("Timeout waiting for canceled tasks to exit")
		}
		return fmt.Errorf("task manager stop: %w", ctx.Err())
	}
}

// RunTaskNow immediately executes a task outside of its regular schedule.
// The execution is asynchronous and subject to the same concurrency limits
// and overlap rules as scheduled executions; disabled tasks can be triggered
// manually. The returned channel receives the task's result (nil on success)
// exactly once and may be ignored by callers that don't need it.
func (tm *TaskManager) RunTaskNow(name string) (<-chan error, error) {
	if name == "" {
		return nil, fmt.Errorf("task name cannot be empty")
	}

	exec, err := tm.beginExecution(name, TriggerManual)
	if err != nil {
		tm.noteSkip(name, TriggerManual, err)
		return nil, err
	}

	done := make(chan error, 1)
	go func() {
		done <- tm.runExecution(exec, nil)
	}()
	return done, nil
}

// RunTaskNowAndWait immediately executes a task and blocks until it completes.
// The provided context controls cancellation for this manual execution; its values
// are not injected into the task context. Use WithContextValue or WithContextInjector
// for task context values. Disabled tasks can be triggered manually.
func (tm *TaskManager) RunTaskNowAndWait(ctx context.Context, name string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}

	exec, err := tm.beginExecution(name, TriggerManual)
	if err != nil {
		tm.noteSkip(name, TriggerManual, err)
		return err
	}

	return tm.runExecution(exec, ctx)
}

// SetContextValue adds or updates a static context value that will be
// injected into all task contexts.
func (tm *TaskManager) SetContextValue(key string, value any) {
	if key == "" {
		return
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.contextValues == nil {
		tm.contextValues = make(map[string]any)
	}
	tm.contextValues[key] = value
}

// GetContextValue retrieves a static context value.
// Returns nil if the key does not exist.
func (tm *TaskManager) GetContextValue(key string) any {
	if key == "" {
		return nil
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.contextValues == nil {
		return nil
	}
	return tm.contextValues[key]
}

// getTaskContext creates a context for task execution with injected values.
// It includes the task name, static context values, and any custom injections.
func (tm *TaskManager) getTaskContext(name string) context.Context {
	ctx := tm.ctx

	// Inject task name using a private key type immune to user-key collisions
	ctx = context.WithValue(ctx, taskNameKey{}, name)

	// Inject static context values (copy to avoid holding lock during injection)
	var contextValues map[string]any
	tm.mu.RLock()
	if len(tm.contextValues) > 0 {
		contextValues = make(map[string]any, len(tm.contextValues))
		maps.Copy(contextValues, tm.contextValues)
	}
	tm.mu.RUnlock()

	// Apply static values without holding lock
	for key, value := range contextValues {
		ctx = context.WithValue(ctx, valueKey(key), value)
	}

	// Call custom injector without holding any locks
	if tm.contextInjector != nil {
		ctx = tm.contextInjector(ctx, name)
	}

	return ctx
}

// GetTaskName extracts the task name from a task context.
// Returns empty string if the context doesn't contain a task name.
func GetTaskName(ctx context.Context) string {
	if name, ok := ctx.Value(taskNameKey{}).(string); ok {
		return name
	}
	return ""
}

// ContextValue retrieves a value injected with WithContextValue or SetContextValue.
func ContextValue(ctx context.Context, key string) any {
	if key == "" {
		return nil
	}
	return ctx.Value(valueKey(key))
}

// Stats returns aggregated statistics about the task manager and all tasks.
func (tm *TaskManager) Stats() Stats {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	stats := Stats{
		TotalTasks:       len(tm.tasks),
		MaxConcurrent:    tm.maxConcurrent,
		AllowOverlapping: tm.allowOverlapping,
	}

	for _, e := range tm.tasks {
		if e.enabled {
			stats.EnabledTasks++
		}
		if e.runningCount > 0 {
			stats.RunningTasks++
		}
		stats.TotalRuns += e.runCount
		stats.TotalErrors += e.errorCount
	}

	return stats
}

// IsStarted reports whether the task manager is currently started.
func (tm *TaskManager) IsStarted() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.state == managerStarted
}
