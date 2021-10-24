package utils

import (
	"fmt"
	"time"
)

// TimeSecondAt returns the result of rounding t down to the nearest multiple of a second
func TimeSecondAt(t time.Time) time.Time {
	return t.Local().Truncate(time.Second)
}

// TimeMinuteAt returns the result of rounding t down to the nearest multiple of a minute
func TimeMinuteAt(t time.Time) time.Time {
	return t.Local().Truncate(time.Minute)
}

// TimeHourAt returns the result of rounding t down to the nearest multiple of a hour
func TimeHourAt(t time.Time) time.Time {
	return t.Local().Truncate(time.Hour)
}

// TimeDayAt returns the result of rounding t down to the nearest multiple of a day
func TimeDayAt(t time.Time, offset time.Duration) time.Time {
	strDay := t.Local().Add(-offset).Format("2006-01-02")
	dayAt, _ := time.Parse("2006-01-02", strDay)
	return dayAt.Add(offset)
}

// TimeMonthAt returns the result of rounding t down to the nearest multiple of a month
func TimeMonthAt(t time.Time, offset time.Duration) time.Time {
	strMonth := t.Local().Add(-offset).Format("2006-01") + "-01"
	monthAt, _ := time.Parse("2006-01-02", strMonth)
	return monthAt.Add(offset)
}

// TimeBetween returns true if t is between start and end
// start=nil means -∞, end=nil means ∞
func TimeBetween(t time.Time, start *time.Time, end *time.Time) bool {
	if start != nil && start.After(t) {
		return false
	}
	if end != nil && end.Before(t) {
		return false
	}
	return true
}

// TimeParseAny parses string with any common format to time.Time
func TimeParseAny(value string) (*time.Time, error) {
	if t, err := time.Parse("2006-01-02 15:04:05", value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC3339, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC3339Nano, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC822, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC822Z, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC850, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC1123, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RFC1123Z, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.UnixDate, value); err == nil {
		return &t, nil
	} else if t, err = time.Parse(time.RubyDate, value); err == nil {
		return &t, nil
	}
	return nil, fmt.Errorf("parse datetime %s error", value)
}
