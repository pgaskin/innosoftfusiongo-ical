package vtimezone

import (
	"fmt"
	"strconv"
	"time"
)

// AppendRRULE formats the provided timezone as a RRULE-based VTimezone using
// the extended timezone information.
//
// The output has been manually compared with Google Calendar for validation.
//
// This is generally the most compatible VTimezone format across clients.
func AppendRRULE(b []byte, tz *time.Location) ([]byte, error) {
	extend := locExtend(tz)
	stdName, stdOffset, dstName, dstOffset, startRule, endRule, hasDST, ok := tzset(extend)
	if !ok {
		if extend != "" {
			return nil, fmt.Errorf("failed to parse extended timezone data %q for %s", extend, tz)
		}

		// if it doesn't have extended data, but never has any transitions, just treat it as standard-only
		if start, end := (time.Time{}).In(tz).ZoneBounds(); start.IsZero() && end.IsZero() {
			stdName, stdOffset = (time.Time{}).In(tz).Zone()
			hasDST, ok = false, true
		} else {
			return nil, fmt.Errorf("no extended timezone data available for %s", tz)
		}
	}

	b = append(b, "BEGIN:VTIMEZONE\r\n"...)
	b = append(b, "TZID:"...)
	b = append(b, tz.String()...)
	b = append(b, "\r\n"...)
	b = append(b, "X-LIC-LOCATION:"...)
	b = append(b, tz.String()...)
	b = append(b, "\r\n"...)

	if hasDST {
		b = append(b, "BEGIN:DAYLIGHT\r\n"...)
		b = append(b, "TZOFFSETFROM:"...)
		b = appendOffset(b, stdOffset)
		b = append(b, "\r\n"...)
		b = append(b, "TZOFFSETTO:"...)
		b = appendOffset(b, dstOffset)
		b = append(b, "\r\n"...)
		b = append(b, "TZNAME:"...)
		b = append(b, dstName...)
		b = append(b, "\r\n"...)
		b = append(b, "DTSTART:"...) // in local TZOFFSETTO time
		b = appendLocalDateTime(b, time.Unix(int64(tzruleTime(1970, startRule, stdOffset)), 0).In(time.FixedZone(stdName, stdOffset)))
		b = append(b, "\r\n"...)
		b = append(b, "RRULE:"...)
		b = appendRule(b, startRule)
		b = append(b, "\r\n"...)
		b = append(b, "END:DAYLIGHT\r\n"...)
	}

	b = append(b, "BEGIN:STANDARD\r\n"...)
	b = append(b, "TZOFFSETFROM:"...)
	if hasDST {
		b = appendOffset(b, dstOffset)
	} else {
		b = appendOffset(b, stdOffset)
	}
	b = append(b, "\r\n"...)
	b = append(b, "TZOFFSETTO:"...)
	b = appendOffset(b, stdOffset)
	b = append(b, "\r\n"...)
	b = append(b, "TZNAME:"...)
	b = append(b, stdName...)
	b = append(b, "\r\n"...)
	b = append(b, "DTSTART:"...) // in local TZOFFSETTO time
	if hasDST {
		b = appendLocalDateTime(b, time.Unix(int64(tzruleTime(1970, endRule, dstOffset)), 0).In(time.FixedZone(dstName, dstOffset)))
	} else {
		b = appendLocalDateTime(b, time.Unix(0, 0).In(time.FixedZone(stdName, stdOffset)))
	}
	b = append(b, "\r\n"...)
	if hasDST {
		b = append(b, "RRULE:"...)
		b = appendRule(b, endRule)
		b = append(b, "\r\n"...)
	}
	b = append(b, "END:STANDARD\r\n"...)

	b = append(b, "END:VTIMEZONE\r\n"...)

	return b, nil
}

func appendLocalDateTime(b []byte, t time.Time) []byte {
	return t.AppendFormat(b, "20060102T150405")
}

func appendOffset(b []byte, secs int) []byte {
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
	return b
}

func appendRule(b []byte, r rule) []byte {
	// https://man7.org/linux/man-pages/man3/tzset.3.html
	switch r.kind {
	case ruleJulian:
		// 1-based julian, skip feb 29 (i.e., day 60 is always March 1)
		for i, x := range [...]int{
			// days before month 12-i
			31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
			31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
			31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
			31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
			31 + 28 + 31 + 30 + 31 + 30 + 31,
			31 + 28 + 31 + 30 + 31 + 30,
			31 + 28 + 31 + 30 + 31,
			31 + 28 + 31 + 30,
			31 + 28 + 31,
			31 + 28,
			31,
			0,
		} {
			if r.day > x {
				b = append(b, "FREQ=YEARLY;BYMONTH="...)
				b = strconv.AppendInt(b, int64(12-i), 10)
				b = append(b, ";BYMONTHDAY="...)
				b = strconv.AppendInt(b, int64(r.day-x), 10)
				return b
			}
		}
	case ruleDOY:
		// 0-based julian, including feb 29
		b = append(b, "FREQ=YEARLY;BYYEARDAY="...)
		b = strconv.AppendInt(b, int64(r.day+1), 10)
		return b
	case ruleMonthWeekDay:
		// 0-sunday-based day, 1-based week (5 is always the last), 1-based month
		b = append(b, "FREQ=YEARLY;BYMONTH="...)
		b = strconv.AppendInt(b, int64(r.mon), 10)
		b = append(b, ";BYDAY="...)
		b = append(b, [...]string{"1", "2", "3", "4", "-1"}[r.week-1]...)
		b = append(b, [...]string{"SU", "MO", "TU", "WE", "TH", "FR", "SA"}[r.day]...)
		return b
	}
	panic("wtf")
}
