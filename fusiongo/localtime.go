package fusiongo

import (
	"fmt"
	"strconv"
	"time"
)

// DateTimeRange is TimeRange with a date.
type DateTimeRange struct {
	Date
	TimeRange
}

func (v DateTimeRange) String() string {
	return v.Date.String() + " " + v.TimeRange.String()
}

func (v DateTimeRange) In(loc *time.Location) (start, end time.Time) {
	start = v.Start.WithDate(v.Date).In(loc)
	end = v.End.WithDate(v.Date).In(loc)
	if end.Before(start) {
		end = end.AddDate(0, 0, 1)
	}
	return
}

func (v DateTimeRange) StartIn(loc *time.Location) time.Time {
	start, _ := v.In(loc)
	return start
}

func (v DateTimeRange) EndIn(loc *time.Location) time.Time {
	_, end := v.In(loc)
	return end
}

func (v DateTimeRange) Less(x DateTimeRange) bool {
	if v.Date == x.Date {
		return v.TimeRange.Less(x.TimeRange)
	}
	return v.Date.Less(x.Date)
}

// TimeRange represents a start and end time. If the end time is before than
// the start time, it is assumed that it ends on the next day.
type TimeRange struct {
	Start Time
	End   Time
}

func (v TimeRange) String() string {
	return v.Start.String() + " - " + v.End.String()
}

func (v TimeRange) WithDate(d Date) DateTimeRange {
	return DateTimeRange{d, v}
}

func (v TimeRange) Less(x TimeRange) bool {
	if v.Start == x.Start {
		return v.End.Less(x.End)
	}
	return v.Start.Less(x.Start)
}

// DateTime combines Date and Time.
type DateTime struct {
	Date
	Time
}

func ParseDateTime(s string) (datetime DateTime, ok bool) {
	for i, r := range s {
		if r == 'T' || r == ' ' {
			if datetime.Date, ok = ParseDate(s[:i]); ok {
				if ok = i+1 < len(s); ok {
					datetime.Time, ok = ParseTime(s[i+1:])
				}
			}
		}
	}
	return
}

func (v DateTime) String() string {
	return v.Date.String() + " " + v.Time.String()
}

func (v DateTime) In(loc *time.Location) time.Time {
	if loc == nil {
		loc = time.Local
	}
	return time.Date(v.Year, v.Month, v.Day, v.Hour, v.Minute, v.Second, 0, loc)
}

func (v DateTime) Less(x DateTime) bool {
	if v.Date == x.Date {
		return v.Time.Less(x.Time)
	}
	return v.Date.Less(x.Date)
}

// Date is a date in local time.
type Date struct {
	Year  int
	Month time.Month
	Day   int
}

func ParseDate(s string) (date Date, ok bool) {
	if len(s) == len("YYYY-MM-DD") {
		if s[4] == '-' && s[7] == '-' {
			if y, e := strconv.ParseInt(s[0:4], 10, 64); e == nil && y > 0 {
				if m, e := strconv.ParseInt(s[5:7], 10, 64); e == nil && 1 <= m && m <= 12 {
					if d, e := strconv.ParseInt(s[8:10], 10, 64); e == nil && 1 <= d && d <= 31 {
						date.Year = int(y)
						date.Month = time.Month(m)
						date.Day = int(d)
						ok = true
					}
				}
			}
		}
	}
	return
}

func (v Date) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", v.Year, v.Month, v.Day)
}

func (v Date) In(loc *time.Location) time.Time {
	if loc == nil {
		loc = time.Local
	}
	return time.Date(v.Year, v.Month, v.Day, 0, 0, 0, 0, loc)
}

func (v Date) WithTime(t Time) DateTime {
	return DateTime{v, t}
}

func (v Date) Less(x Date) bool {
	if v.Year == x.Year {
		if v.Month == x.Month {
			return v.Day < x.Day
		}
		return v.Month < x.Month
	}
	return v.Year < x.Year
}

// Time is a time in local time.
type Time struct {
	Hour   int
	Minute int
	Second int
}

func ParseTime(s string) (time Time, ok bool) {
	if len(s) == len("HH:MM:SS") {
		if s[2] == ':' && s[5] == ':' {
			if h, e := strconv.ParseInt(s[0:2], 10, 64); e == nil && 0 <= h && h < 24 {
				if m, e := strconv.ParseInt(s[3:5], 10, 64); e == nil && 0 <= m && m < 60 {
					if s, e := strconv.ParseInt(s[6:8], 10, 64); e == nil && 0 <= s && s < 60 {
						time.Hour = int(h)
						time.Minute = int(m)
						time.Second = int(s)
						ok = true
					}
				}
			}
		}
	}
	return
}

func (v Time) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", v.Hour, v.Minute, v.Second)
}

func (v Time) WithDate(d Date) DateTime {
	return DateTime{d, v}
}

func (v Time) Less(x Time) bool {
	if v.Hour == x.Hour {
		if v.Minute == x.Minute {
			return v.Second < x.Second
		}
		return v.Minute < x.Minute
	}
	return v.Hour < x.Hour
}
