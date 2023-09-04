package main

import (
	"fmt"
	"strconv"
	"time"
)

type FusionNotifications struct {
	LastUpdate    time.Time `json:"lastUpdateUtcDateTime"`
	Notifications []FusionNotification
}

type FusionNotification struct {
	ID           int    `json:"id,string"`
	Notification string `json:"notification"`
	Sent         string `json:"datetime_sent"`
}

type FusionSchedule struct {
	LastUpdate time.Time                `json:"lastUpdateUtcDateTime"`
	Categories []FusionScheduleCategory `json:"categories"`
}

type FusionScheduleCategory struct {
	Category string              `json:"category"`
	ID       int                 `json:"id,string"`
	Days     []FusionScheduleDay `json:"days"`
}

type FusionScheduleDay struct {
	Date                FusionDate               `json:"date"` // YYYY-MM-DD
	DayOfTheWeek        string                   `json:"dayofTheWeek"`
	ScheduledActivities []FusionScheduleActivity `json:"scheduled_activities"`
}

type FusionScheduleActivity struct {
	Activity       string     `json:"activity"`
	Location       string     `json:"location"`
	StartTime      FusionTime `json:"startTime"`
	EndTime        FusionTime `json:"endTime"`
	Description    string     `json:"description"`
	IsCanceled     int        `json:"isCancelled,string"`
	DetailURL      string     `json:"detailUrl"`
	ActivityID     string     `json:"activityID"`
	AvailableSpots int        `json:"availableSpots"`
}

func (x FusionScheduleActivity) Date(d FusionDate, loc *time.Location) (start, end time.Time) {
	start = d.Time(loc)
	end = start
	if x.EndTime.Less(x.StartTime) {
		end = end.AddDate(0, 0, 1)
	}
	start = x.StartTime.Time(start)
	end = x.EndTime.Time(end)
	return
}

type FusionDate struct {
	Year  int
	Month time.Month
	Day   int
}

func (d FusionDate) String() string {
	return fmt.Sprintf("%04d-%02d-%02d", d.Year, d.Month, d.Day)
}

func (d *FusionDate) UnmarshalText(b []byte) error {
	if len(b) != len("YYYY-MM-DD") || b[4] != '-' || b[7] != '-' {
		return fmt.Errorf("invalid date %q: not in format YYYY-MM-DD", string(b))
	}
	if v, err := strconv.ParseInt(string(b[0:4]), 10, 64); err != nil {
		return fmt.Errorf("invalid date %q: parse year: %w", string(b), err)
	} else if year := int(v); year <= 0 {
		return fmt.Errorf("invalid date %q: invalid year", string(b))
	} else {
		d.Year = int(year)
	}
	if v, err := strconv.ParseInt(string(b[5:7]), 10, 64); err != nil {
		return fmt.Errorf("invalid date %q: parse month: %w", string(b), err)
	} else if month := time.Month(v); month < time.January || month > time.December {
		return fmt.Errorf("invalid date %q: invalid month", string(b))
	} else {
		d.Month = month
	}
	if v, err := strconv.ParseInt(string(b[8:10]), 10, 64); err != nil {
		return fmt.Errorf("invalid date %q: parse day: %w", string(b), err)
	} else if day := int(v); day <= 0 || day > 31 {
		return fmt.Errorf("invalid date %q: invalid day", string(b))
	} else {
		d.Day = int(day)
	}
	return nil
}

func (d FusionDate) Time(loc *time.Location) time.Time {
	return time.Date(d.Year, d.Month, d.Day, 0, 0, 0, 0, loc)
}

func (d FusionDate) Less(x FusionDate) bool {
	if d.Year == x.Year {
		if d.Month == x.Month {
			return d.Day < x.Day
		}
		return d.Month < x.Month
	}
	return d.Year < x.Year
}

func (d FusionDate) IsZero() bool {
	return d == FusionDate{}
}

type FusionTime struct {
	Hour   int
	Minute int
	Second int
}

func (t FusionTime) String() string {
	return fmt.Sprintf("%02d:%02d:%02d", t.Hour, t.Minute, t.Second)
}

func (t *FusionTime) UnmarshalText(b []byte) error {
	if len(b) != len("HH:MM:SS") || b[2] != ':' || b[5] != ':' {
		return fmt.Errorf("invalid time %q: not in format HH:MM:SS", string(b))
	}
	if v, err := strconv.ParseInt(string(b[0:2]), 10, 64); err != nil {
		return fmt.Errorf("invalid time %q: parse hour: %w", string(b), err)
	} else if hour := int(v); hour < 0 || hour >= 24 {
		return fmt.Errorf("invalid time %q: invalid hour", string(b))
	} else {
		t.Hour = hour
	}
	if v, err := strconv.ParseInt(string(b[3:5]), 10, 64); err != nil {
		return fmt.Errorf("invalid time %q: parse minute: %w", string(b), err)
	} else if minute := int(v); minute < 0 || minute >= 60 {
		return fmt.Errorf("invalid time %q: invalid minute", string(b))
	} else {
		t.Minute = minute
	}
	if v, err := strconv.ParseInt(string(b[6:8]), 10, 64); err != nil {
		return fmt.Errorf("invalid time %q: parse second: %w", string(b), err)
	} else if second := int(v); second < 0 || second >= 60 {
		return fmt.Errorf("invalid time %q: invalid second", string(b))
	} else {
		t.Second = second
	}
	return nil
}

func (t FusionTime) Time(d time.Time) time.Time {
	loc := d.Location()
	year, month, day := d.Date()
	return time.Date(year, month, day, t.Hour, t.Minute, t.Second, 0, loc)
}

func (t FusionTime) Less(x FusionTime) bool {
	if t.Hour == x.Hour {
		if t.Minute == x.Minute {
			return t.Second < x.Second
		}
		return t.Minute < x.Minute
	}
	return t.Hour < x.Hour
}
