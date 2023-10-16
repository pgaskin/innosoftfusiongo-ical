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

func (v DateTimeRange) StringCompact() string {
	return v.Date.String() + " " + v.TimeRange.StringCompact()
}

func (v DateTimeRange) Range() (start, end DateTime) {
	start = v.TimeRange.Start.WithDate(v.Date)
	end = v.TimeRange.End.WithDate(v.Date)
	if end.Less(start) {
		end = end.AddDays(1)
	}
	return start, end
}

func (v DateTimeRange) Start() DateTime {
	start, _ := v.Range()
	return start
}

func (v DateTimeRange) End() DateTime {
	_, end := v.Range()
	return end
}

func (v DateTimeRange) In(loc *time.Location) (start, end time.Time) {
	a, b := v.Range()
	return a.In(loc), b.In(loc)
}

func (v DateTimeRange) Less(x DateTimeRange) bool {
	if v.Date == x.Date {
		return v.TimeRange.Less(x.TimeRange)
	}
	return v.Date.Less(x.Date)
}

func (v DateTimeRange) Compare(x DateTimeRange) int {
	if v == x {
		return 0
	}
	if v.Less(x) {
		return -1
	}
	return 1
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

func (v TimeRange) StringCompact() string {
	return v.Start.StringCompact() + " - " + v.End.StringCompact()
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

func (v TimeRange) Compare(x TimeRange) int {
	if v == x {
		return 0
	}
	if v.Less(x) {
		return -1
	}
	return 1
}

func (v TimeRange) TimeOverlaps(x TimeRange) bool {
	x1, x2 := v.Start, v.End
	y1, y2 := x.Start, x.End
	if x2.Less(x1) {
		x2.Hour += 24
	}
	if y2.Less(y1) {
		y2.Hour += 24
	}
	return !(x2.Less(y1) || y2.Less(x1))
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

func GoDateTime(t time.Time) DateTime {
	var (
		y, m, d    = t.Date()
		hh, mm, ss = t.Clock()
	)
	return DateTime{
		Date: Date{y, m, d},
		Time: Time{hh, mm, ss},
	}
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

func (v DateTime) Compare(x DateTime) int {
	if v == x {
		return 0
	}
	if v.Less(x) {
		return -1
	}
	return 1
}

func (v DateTime) AddDays(d int) DateTime {
	y, m, d := civilFromDays(daysFromCivil(v.Year, v.Month, v.Day) + d)
	return Date{y, m, d}.WithTime(v.Time)
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

func (v Date) Compare(x Date) int {
	if v == x {
		return 0
	}
	if v.Less(x) {
		return -1
	}
	return 1
}

func (v Date) Date() (year int, month time.Month, day int) {
	return v.Year, v.Month, v.Day
}

func (v Date) Weekday() time.Weekday {
	// https://en.wikipedia.org/wiki/Determination_of_the_day_of_the_week#Sakamoto's_methods
	y, m, d := v.Year, int(v.Month), v.Day
	if y < 1752 {
		panic("weekday: unsupported year")
	}
	if m < 3 {
		y--
	}
	return time.Weekday((y + y/4 - y/100 + y/400 + [...]int{0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4}[m-1] + d) % 7)
}

func (v Date) AddDays(d int) Date {
	y, m, d := civilFromDays(daysFromCivil(v.Year, v.Month, v.Day) + d)
	return Date{y, m, d}
}

// http://howardhinnant.github.io/date_algorithms.html#days_from_civil
func daysFromCivil(y int, m time.Month, d int) int {
	if m < 3 {
		y--
	}
	var era int
	if y >= 0 {
		era = y
	} else {
		era = y - 399
	}
	era /= 400
	yoe := uint(y - era*400)
	var doy uint
	if m > 2 {
		doy = uint(m) - 3
	} else {
		doy = uint(m) + 9
	}
	doy = (153*doy+2)/5 + uint(d) - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + int(doe) - 719468
}

// http://howardhinnant.github.io/date_algorithms.html#civil_from_days
func civilFromDays(z int) (y int, m time.Month, d int) {
	z += 719468
	var era int
	if z >= 0 {
		era = z
	} else {
		era = z - 146096
	}
	era /= 146097
	doe := uint(z - era*146097)
	yoe := (doe - doe/1460 + doe/36524 - doe/146096) / 365
	y = int(yoe) + era*400
	doy := doe - (365*yoe + yoe/4 - yoe/100)
	mp := (5*doy + 2) / 153
	d = int(doy - (153*mp+2)/5 + 1)
	if mp < 10 {
		m = time.Month(mp + 3)
	} else {
		m = time.Month(mp - 9)
	}
	if m < 3 {
		y++
	}
	return
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

func (v Time) StringCompact() string {
	if v.Second == 0 {
		return fmt.Sprintf("%02d:%02d", v.Hour, v.Minute)
	}
	return fmt.Sprintf("%02d:%02d:%02d", v.Hour, v.Minute, v.Second)
}

func (v Time) WithDate(d Date) DateTime {
	return DateTime{d, v}
}

func (v Time) WithStart(t Time) TimeRange {
	return TimeRange{t, v}
}

func (v Time) WithEnd(t Time) TimeRange {
	return TimeRange{v, t}
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

func (v Time) Compare(x Time) int {
	if v == x {
		return 0
	}
	if v.Less(x) {
		return -1
	}
	return 1
}
