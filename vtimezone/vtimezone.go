// Package vtimezone generates iCalendar VTimezone objects.
package vtimezone

import (
	"strconv"
	"time"
)

// Append formats the provided timezone as a RRULE-based VTimezone using the
// extended timezone information, using loc.String() as the TZID.
//
// This is generally the most compatible VTimezone format across clients.
func Append(b []byte, loc *time.Location) ([]byte, error) {
	x, err := tzinfoFromLoc(loc)
	if err == nil {
		b = icalTimezone(b, loc.String(), x)
	}
	return b, err
}

// AppendTZ is like Append, but takes a custom TZID and POSIX extended timezone,
// returning false if the timezone string is invalid.
func AppendTZ(b []byte, tzid, tz string) ([]byte, bool) {
	x, ok := tzinfoFromTZ(tz)
	if ok {
		b = icalTimezone(b, tzid, x)
	}
	return b, ok
}

// AppendFixed is like Append, but takes a fixed standard offset, returning
// false if the name/offset is invalid.
func AppendFixed(b []byte, tzid, name string, offset int) ([]byte, bool) {
	x, ok := tzinfoFixed(name, offset)
	if ok {
		b = icalTimezone(b, tzid, x)
	}
	return b, ok
}

func icalTimezone(b []byte, tzid string, x tzinfo) []byte {
	b = icalProp(b, "BEGIN", "VTIMEZONE")
	b = icalProp(b, "TZID", tzid)
	b = icalProp(b, "X-LIC-LOCATION", tzid)
	if x.HasDST() {
		b = icalProp(b, "BEGIN", "DAYLIGHT")
		b = icalOffset(b, "TZOFFSETFROM", x.Standard.Offset)
		b = icalOffset(b, "TZOFFSETTO", x.Daylight.Offset)
		b = icalProp(b, "TZNAME", x.Daylight.Name)
		b = icalDateTimeLocal(b, "DTSTART", time.Unix(int64(tzruleTime(1970, x.Transition.Start, x.Standard.Offset)), 0).In(time.FixedZone(x.Standard.Name, x.Standard.Offset)))
		b = icalRule(b, "RRULE", x.Transition.Start)
		b = icalProp(b, "END", "DAYLIGHT")
	}
	b = icalProp(b, "BEGIN", "STANDARD")
	if x.HasDST() {
		b = icalOffset(b, "TZOFFSETFROM", x.Daylight.Offset)
	} else {
		b = icalOffset(b, "TZOFFSETFROM", x.Standard.Offset)
	}
	b = icalOffset(b, "TZOFFSETTO", x.Standard.Offset)
	b = icalProp(b, "TZNAME", x.Standard.Name)
	if x.HasDST() {
		b = icalDateTimeLocal(b, "DTSTART", time.Unix(int64(tzruleTime(1970, x.Transition.End, x.Daylight.Offset)), 0).In(time.FixedZone(x.Daylight.Name, x.Daylight.Offset)))
	} else {
		b = icalDateTimeLocal(b, "DTSTART", time.Unix(0, 0).In(time.FixedZone(x.Standard.Name, x.Standard.Offset)))
	}
	if x.HasDST() {
		b = icalRule(b, "RRULE", x.Transition.End)
	}
	b = icalProp(b, "END", "STANDARD")
	b = icalProp(b, "END", "VTIMEZONE")
	return b
}

func icalProp(b []byte, k, v string) []byte {
	b = append(b, k...)
	b = append(b, ':')
	b = append(b, v...)
	b = append(b, '\r', '\n')
	return b
}

func icalDateTimeLocal(b []byte, k string, t time.Time) []byte {
	b = append(b, k...)
	b = append(b, ':')
	b = t.AppendFormat(b, "20060102T150405")
	b = append(b, '\r', '\n')
	return b
}

func icalOffset(b []byte, k string, secs int) []byte {
	b = append(b, k...)
	b = append(b, ':')
	zone := secs / 60
	if zone < 0 {
		b = append(b, '-')
		zone = -zone
	} else {
		b = append(b, '+')
	}
	b = append(b, '0'+byte(zone/60/10%10))
	b = append(b, '0'+byte(zone/60%10))
	b = append(b, '0'+byte(zone%60/10%10))
	b = append(b, '0'+byte(zone%60%10))
	b = append(b, '\r', '\n')
	return b
}

func icalRule(b []byte, k string, r rule) []byte {
	b = append(b, k...)
	b = append(b, ':')
	switch r.kind {
	// https://man7.org/linux/man-pages/man3/tzset.3.html
	case ruleJulian:
		// 1-based julian, skip feb 29 (i.e., day 60 is always March 1)
		if r.day > 0 && r.day <= 365 {
			for m := time.December; m >= time.January; m-- {
				if x := int(daysBefore[m-1]); r.day > x {
					b = append(b, "FREQ=YEARLY;BYMONTH="...)
					b = strconv.AppendInt(b, int64(m), 10)
					b = append(b, ";BYMONTHDAY="...)
					b = strconv.AppendInt(b, int64(r.day-x), 10)
					b = append(b, '\r', '\n')
					return b
				}
			}
		}
	case ruleDOY:
		// 0-based julian, including feb 29
		if r.day >= 0 && r.day <= 365 {
			b = append(b, "FREQ=YEARLY;BYYEARDAY="...)
			b = strconv.AppendInt(b, int64(r.day+1), 10)
			b = append(b, '\r', '\n')
			return b
		}
	case ruleMonthWeekDay:
		// 0-sunday-based day, 1-based week (5 is always the last), 1-based month
		if r.day >= 0 && r.day < 7 && r.week >= 0 && r.week <= 5 && r.mon > 0 && r.mon <= 12 {
			b = append(b, "FREQ=YEARLY;BYMONTH="...)
			b = strconv.AppendInt(b, int64(r.mon), 10)
			b = append(b, ";BYDAY="...)
			b = append(b, [...]string{"1", "2", "3", "4", "-1"}[r.week-1]...)
			b = append(b, [...]string{"SU", "MO", "TU", "WE", "TH", "FR", "SA"}[r.day]...)
			b = append(b, '\r', '\n')
			return b
		}
	}
	panic("wtf")
}
