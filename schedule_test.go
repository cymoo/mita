package mita

import (
	"context"
	"testing"
	"time"
)

func TestScheduleBuilder(t *testing.T) {
	tests := []struct {
		name     string
		schedule Schedule
		want     string
	}{
		{
			name:     "every second",
			schedule: Every().Second(),
			want:     "* * * * * *",
		},
		{
			name:     "every minute",
			schedule: Every().Minute(),
			want:     "0 * * * * *",
		},
		{
			name:     "every hour",
			schedule: Every().Hour(),
			want:     "0 0 * * * *",
		},
		{
			name:     "every day",
			schedule: Every().Day(),
			want:     "0 0 0 * * *",
		},
		{
			name:     "every 5 seconds",
			schedule: Every().Seconds(5),
			want:     "*/5 * * * * *",
		},
		{
			name:     "every 15 minutes",
			schedule: Every().Minutes(15),
			want:     "0 */15 * * * *",
		},
		{
			name:     "every 6 hours",
			schedule: Every().Hours(6),
			want:     "0 0 */6 * * *",
		},
		{
			name:     "at specific time",
			schedule: Every().Day().At(14, 30),
			want:     "0 30 14 * * *",
		},
		{
			name:     "on specific weekday",
			schedule: Every().Day().OnWeekday(time.Monday),
			want:     "0 0 0 * * 1",
		},
		{
			name:     "on specific day of month",
			schedule: Every().Day().OnDay(15),
			want:     "0 0 0 15 * *",
		},
		{
			name:     "cron expression",
			schedule: Cron("0 0 12 * * *"),
			want:     "0 0 12 * * *",
		},
		{
			name:     "interval of 90 seconds",
			schedule: Every().Interval(90 * time.Second),
			want:     "@every 1m30s",
		},
		{
			name:     "interval of 2 hours",
			schedule: Every().Interval(2 * time.Hour),
			want:     "@every 2h0m0s",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.schedule.String()
			if got != tt.want {
				t.Errorf("Schedule.String() = %v, want %v", got, tt.want)
			}
			if b, ok := tt.schedule.(*ScheduleBuilder); ok && b.Err() != nil {
				t.Errorf("Err() = %v, want nil", b.Err())
			}
		})
	}
}

func TestScheduleBuilderErrors(t *testing.T) {
	tests := []struct {
		name    string
		builder *ScheduleBuilder
	}{
		{
			name:    "negative seconds interval",
			builder: Every().Seconds(-1),
		},
		{
			name:    "zero minutes interval",
			builder: Every().Minutes(0),
		},
		// Out-of-range step values silently misfire in cron ("*/90" in the
		// seconds field runs every 60s, not 90s), so the builder must reject them.
		{
			name:    "seconds interval over 59",
			builder: Every().Seconds(90),
		},
		{
			name:    "minutes interval over 59",
			builder: Every().Minutes(90),
		},
		{
			name:    "hours interval over 23",
			builder: Every().Hours(24),
		},
		{
			name:    "days interval over 31",
			builder: Every().Days(32),
		},
		{
			name:    "invalid hour in At",
			builder: Every().At(24, 0),
		},
		{
			name:    "invalid minute in At",
			builder: Every().At(12, 60),
		},
		{
			name:    "invalid day",
			builder: Every().OnDay(32),
		},
		{
			name:    "zero day",
			builder: Every().OnDay(0),
		},
		{
			name:    "invalid weekday",
			builder: Every().OnWeekday(time.Weekday(9)),
		},
		{
			name:    "sub-second interval",
			builder: Every().Interval(500 * time.Millisecond),
		},
		{
			name:    "fractional-second interval",
			builder: Every().Interval(1500 * time.Millisecond),
		},
		{
			name:    "interval combined with field methods",
			builder: Every().Seconds(5).Interval(time.Minute),
		},
		{
			name:    "field method after interval",
			builder: Every().Interval(time.Minute).Seconds(5),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.builder.Err() == nil {
				t.Fatal("Err() = nil, want error")
			}
			// The invalid schedule must also be rejected at registration time.
			tm := New()
			defer tm.Stop()
			if err := tm.AddTask("bad-schedule", tt.builder, func(ctx context.Context) error { return nil }); err == nil {
				t.Fatal("AddTask() accepted an invalid schedule")
			}
		})
	}
}

func TestScheduleBuilderFirstErrorWins(t *testing.T) {
	b := Every().Seconds(90).Minutes(90)
	err := b.Err()
	if err == nil {
		t.Fatal("Err() = nil, want error")
	}
	if got := err.Error(); got != "seconds interval must be between 1 and 59, got 90 (use Interval for longer periods)" {
		t.Fatalf("Err() = %q, want first recorded error", got)
	}
}

func TestNormalizeCronExpr(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"* * * * * *", "* * * * * *"},   // 6-field passes through
		{"*/5 * * * *", "0 */5 * * * *"}, // 5-field gains a seconds column
		{"  0 2 * * *  ", "0 0 2 * * *"}, // whitespace trimmed
		{"@hourly", "@hourly"},           // descriptors pass through
		{"@every 1m30s", "@every 1m30s"}, // @every passes through
		{"not a cron", "not a cron"},     // garbage left for the parser to reject
	}
	for _, tt := range tests {
		if got := normalizeCronExpr(tt.expr); got != tt.want {
			t.Errorf("normalizeCronExpr(%q) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}
