package mita

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Schedule represents a task scheduling expression.
//
// String returns a cron expression (or an @-descriptor such as "@every 1m30s").
// An implementation may additionally provide an `Err() error` method;
// AddTask and UpdateSchedule check it and reject schedules that report an error.
type Schedule interface {
	String() string
}

// CronSchedule wraps a raw cron expression string.
type CronSchedule struct {
	expression string
}

// Cron creates a Schedule from a cron expression string.
//
// Both the 6-field format "second minute hour day month weekday" and the
// standard 5-field format "minute hour day month weekday" are accepted;
// 5-field expressions run at second 0. Descriptors such as "@hourly" and
// "@every 90s" are also supported. The expression is validated when the
// schedule is registered with AddTask or UpdateSchedule.
func Cron(expr string) *CronSchedule {
	return &CronSchedule{expression: expr}
}

// String returns the cron expression.
func (c *CronSchedule) String() string {
	return c.expression
}

// normalizeCronExpr converts standard 5-field cron expressions to the 6-field
// (with seconds) format used internally. Descriptors are passed through as-is.
func normalizeCronExpr(expr string) string {
	expr = strings.TrimSpace(expr)
	if strings.HasPrefix(expr, "@") {
		return expr
	}
	if len(strings.Fields(expr)) == 5 {
		return "0 " + expr
	}
	return expr
}

// scheduleExpr extracts the validated, normalized cron expression from a Schedule.
func scheduleExpr(schedule Schedule) (string, error) {
	if v, ok := schedule.(interface{ Err() error }); ok {
		if err := v.Err(); err != nil {
			return "", fmt.Errorf("invalid schedule: %w", err)
		}
	}
	return normalizeCronExpr(schedule.String()), nil
}

// ScheduleBuilder provides a fluent API for building cron schedules.
//
// Invalid arguments (for example Seconds(90) or At(25, 0)) are recorded and
// reported by Err; AddTask and UpdateSchedule reject builders holding an
// error. Builder methods never panic.
type ScheduleBuilder struct {
	second  string
	minute  string
	hour    string
	day     string
	month   string
	weekday string

	descriptor string // set by Interval; mutually exclusive with the fields above
	touched    bool   // whether any field-based method has been called
	err        error
}

// Every creates a new ScheduleBuilder with default values (runs every minute).
func Every() *ScheduleBuilder {
	return &ScheduleBuilder{
		second:  "0",
		minute:  "*",
		hour:    "*",
		day:     "*",
		month:   "*",
		weekday: "*",
	}
}

// String converts the builder to a cron expression string.
func (s *ScheduleBuilder) String() string {
	if s.descriptor != "" {
		return s.descriptor
	}
	return s.second + " " + s.minute + " " + s.hour + " " + s.day + " " + s.month + " " + s.weekday
}

// Err reports the first invalid argument passed to the builder, if any.
func (s *ScheduleBuilder) Err() error {
	return s.err
}

// fail records the first error and keeps the builder chainable.
func (s *ScheduleBuilder) fail(format string, args ...any) *ScheduleBuilder {
	if s.err == nil {
		s.err = fmt.Errorf(format, args...)
	}
	return s
}

// useFields marks the builder as field-based, rejecting mixes with Interval.
func (s *ScheduleBuilder) useFields() bool {
	if s.descriptor != "" {
		s.fail("Interval cannot be combined with other schedule methods")
		return false
	}
	s.touched = true
	return true
}

// Interval configures the schedule to run at a fixed interval using the
// "@every" descriptor. Unlike Seconds/Minutes/Hours, the interval does not
// reset at unit boundaries, so any duration is honored exactly:
// Interval(90*time.Second) runs every 90 seconds.
//
// The duration must be a whole number of seconds and at least one second.
// Interval cannot be combined with other schedule methods.
func (s *ScheduleBuilder) Interval(d time.Duration) *ScheduleBuilder {
	if s.touched {
		return s.fail("Interval cannot be combined with other schedule methods")
	}
	if d < time.Second {
		return s.fail("interval must be at least one second, got %v", d)
	}
	if d%time.Second != 0 {
		return s.fail("interval must be a whole number of seconds, got %v", d)
	}
	s.descriptor = "@every " + d.String()
	return s
}

// Second configures the schedule to run every second.
func (s *ScheduleBuilder) Second() *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	s.second = "*"
	s.minute = "*"
	s.hour = "*"
	return s
}

// Minute configures the schedule to run every minute (at 0 seconds).
func (s *ScheduleBuilder) Minute() *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	s.second = "0"
	s.minute = "*"
	s.hour = "*"
	return s
}

// Hour configures the schedule to run every hour (at 0 minutes, 0 seconds).
func (s *ScheduleBuilder) Hour() *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	s.second = "0"
	s.minute = "0"
	s.hour = "*"
	return s
}

// Day configures the schedule to run every day (at midnight).
func (s *ScheduleBuilder) Day() *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	s.second = "0"
	s.minute = "0"
	s.hour = "0"
	s.day = "*"
	return s
}

// Seconds configures the schedule to run at the specified second interval.
// For example, Seconds(30) runs every 30 seconds.
//
// The interval must be between 1 and 59. Cron step intervals reset at the
// top of every minute, so Seconds(45) runs at :00 and :45 of each minute;
// use Interval for exact fixed-length intervals or periods over 59 seconds.
func (s *ScheduleBuilder) Seconds(interval int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if interval < 1 || interval > 59 {
		return s.fail("seconds interval must be between 1 and 59, got %d (use Interval for longer periods)", interval)
	}
	s.second = "*/" + strconv.Itoa(interval)
	s.minute = "*"
	s.hour = "*"
	return s
}

// Minutes configures the schedule to run at the specified minute interval.
// For example, Minutes(15) runs every 15 minutes.
//
// The interval must be between 1 and 59. Cron step intervals reset at the
// top of every hour, so Minutes(45) runs at :00 and :45 of each hour;
// use Interval for exact fixed-length intervals or periods over 59 minutes.
func (s *ScheduleBuilder) Minutes(interval int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if interval < 1 || interval > 59 {
		return s.fail("minutes interval must be between 1 and 59, got %d (use Interval for longer periods)", interval)
	}
	s.second = "0"
	s.minute = "*/" + strconv.Itoa(interval)
	s.hour = "*"
	return s
}

// Hours configures the schedule to run at the specified hour interval.
// For example, Hours(6) runs every 6 hours.
//
// The interval must be between 1 and 23. Cron step intervals reset at
// midnight, so Hours(7) runs at 0:00, 7:00, 14:00 and 21:00 each day;
// use Interval for exact fixed-length intervals or periods over 23 hours.
func (s *ScheduleBuilder) Hours(interval int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if interval < 1 || interval > 23 {
		return s.fail("hours interval must be between 1 and 23, got %d (use Interval for longer periods)", interval)
	}
	s.second = "0"
	s.minute = "0"
	s.hour = "*/" + strconv.Itoa(interval)
	return s
}

// Days configures the schedule to run at the specified day interval.
// For example, Days(2) runs every 2 days at midnight.
//
// The interval must be between 1 and 31. Cron step intervals reset at the
// start of each month, so the last interval of a month may be shorter.
func (s *ScheduleBuilder) Days(interval int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if interval < 1 || interval > 31 {
		return s.fail("days interval must be between 1 and 31, got %d", interval)
	}
	s.second = "0"
	s.minute = "0"
	s.hour = "0"
	s.day = "*/" + strconv.Itoa(interval)
	return s
}

// At specifies a specific time of day for the schedule.
// For example, At(14, 30) runs at 2:30 PM.
// Hour must be 0-23 and minute must be 0-59.
func (s *ScheduleBuilder) At(hour, minute int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if hour < 0 || hour > 23 {
		return s.fail("hour must be between 0 and 23, got %d", hour)
	}
	if minute < 0 || minute > 59 {
		return s.fail("minute must be between 0 and 59, got %d", minute)
	}
	s.second = "0"
	s.minute = strconv.Itoa(minute)
	s.hour = strconv.Itoa(hour)
	return s
}

// OnWeekday restricts the schedule to a specific day of the week.
// For example, OnWeekday(time.Monday) runs only on Mondays.
func (s *ScheduleBuilder) OnWeekday(weekday time.Weekday) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if weekday < time.Sunday || weekday > time.Saturday {
		return s.fail("weekday must be between Sunday and Saturday, got %d", weekday)
	}
	s.weekday = strconv.Itoa(int(weekday))
	return s
}

// OnDay restricts the schedule to a specific day of the month.
// For example, OnDay(15) runs on the 15th of each month.
// Day must be between 1 and 31.
func (s *ScheduleBuilder) OnDay(day int) *ScheduleBuilder {
	if !s.useFields() {
		return s
	}
	if day < 1 || day > 31 {
		return s.fail("day must be between 1 and 31, got %d", day)
	}
	s.day = strconv.Itoa(day)
	return s
}
