// Package ifgical converts Innosoft Fusion Go drop-in schedules and global
// notifications] into an iCalendar object.
package ifgical

import (
	"cmp"
	"crypto/sha1"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/pgaskin/innosoftfusiongo-ical/fusiongo"
	"github.com/pgaskin/innosoftfusiongo-ical/vtimezone"
)

// Data contains original Innosoft Fusion Go data.
type Data struct {
	SchoolID      int                     // required
	Schedule      *fusiongo.Schedule      // optional, must be normalized (i.e., events duplicated, etc) by fusiongo.ParseSchedule
	Notifications *fusiongo.Notifications // optional
}

// Calendar stores processed Innosoft Fusion Go schedule data.
type Calendar struct {
	id   int                                // school id
	tz   *time.Location                     // for local times
	tzV  []byte                             // VTimezone
	sch  map[activityKey][]activityInstance // stable-sorted by date, all activityInstances fall on a activityKey.Weekday
	schK []activityKey                      // stable-sorted keys of sch
	schT time.Time                          // last modified
	not  []notificationInstance             // stable-sorted by id
	notT time.Time                          // last modified
}

// activityKey contains fields common to all instances (i.e., a base
// recurrence).
type activityKey struct {
	ActivityID string
	Location   string
	StartTime  fusiongo.Time
	Weekday    [7]bool
}

// activityInstance contains fields specific to an instance.
type activityInstance struct {
	IsCancelled bool
	Activity    string
	Description string
	Date        fusiongo.Date
	EndTime     fusiongo.Time
	Categories  []string
	CategoryIDs []string
}

// notificationInstance contains information about a notification.
type notificationInstance struct {
	ID   string
	Text string
	Sent fusiongo.DateTime
}

// Prepare processes the provided data.
func Prepare(tz *time.Location, data Data) (*Calendar, error) {
	var c Calendar
	if c.id = data.SchoolID; c.id <= 0 {
		return nil, fmt.Errorf("invalid school id %d", data.SchoolID)
	}
	if err := c.initTimezone(tz); err != nil {
		return nil, fmt.Errorf("prepare timezone: %w", err)
	}
	if err := c.initSchedule(data.Schedule); err != nil {
		return nil, fmt.Errorf("prepare schedule: %w", err)
	}
	if err := c.initNotifications(data.Notifications); err != nil {
		return nil, fmt.Errorf("prepare notifications: %w", err)
	}
	return &c, nil
}

func (c *Calendar) initTimezone(tz *time.Location) error {
	if tz == nil {
		return fmt.Errorf("no timezone provided")
	}

	// init
	c.tz = tz

	// generate the vtimezone object
	if v, err := vtimezone.AppendRRULE(nil, tz); err != nil {
		return fmt.Errorf("cannot generate rrule-based vtimezone object: %w", err)
	} else {
		c.tzV = v
	}

	return nil
}

func (c *Calendar) initSchedule(schedule *fusiongo.Schedule) error {
	if schedule == nil {
		return nil
	}

	// check some basic assumptions
	// - we depend (for correctness, not code) on the fact that an activity is uniquely identified by its id and location and can only occur once per start time per day
	// - we also depend on each activity having at least one category (this should always be true)
	{
		faSeen := map[string]fusiongo.ActivityInstance{}
		for i, fa := range schedule.Activities {
			iid := fmt.Sprint(fa.ActivityID, fa.Location, fa.Time.Date, fa.Time.TimeRange.Start)
			if x, seen := faSeen[iid]; !seen {
				faSeen[iid] = fa
			} else if !reflect.DeepEqual(fa, x) {
				slog.Warn("activity instance is not uniquely identified by (id, location, date, start)", "activity1", fa, "activity2", x)
			}
			if len(schedule.Categories[i].Category) == 0 {
				panic("wtf: expected activity instance to have at least one category")
			}
		}
	}

	// init
	c.schT = schedule.Updated

	// collect activity instance event info and group into recurrence groups
	c.sch = mapGroupInto(schedule.Activities, func(i int, fai fusiongo.ActivityInstance) (activityKey, activityInstance) {
		ak := activityKey{
			ActivityID: fai.ActivityID,
			StartTime:  fai.Time.TimeRange.Start,
			Location:   fai.Location,
		}
		ai := activityInstance{
			IsCancelled: fai.IsCancelled,
			Activity:    fai.Activity,
			Description: fai.Description,
			Date:        fai.Time.Date,
			EndTime:     fai.Time.TimeRange.End,
			Categories:  schedule.Categories[i].Category,
			CategoryIDs: schedule.Categories[i].CategoryID,
		}
		return ak, ai
	})

	// split out days which consistently have different end times
	{
		sch := map[activityKey][]activityInstance{}
		for activityKey, activityInstances := range c.sch {

			// group the activities by their weekday
			actByWeekday := weekdayGroupBy(activityInstances, func(i int, ai activityInstance) time.Weekday {
				return ai.Date.WithTime(activityKey.StartTime).Weekday()
			})

			// get the most common end time for each weekday
			etByWeekday := weekdayMapInto(actByWeekday, func(w time.Weekday, ais []activityInstance) fusiongo.Time {
				return mostCommonBy(ais, func(activityInstance activityInstance) fusiongo.Time {
					return activityInstance.EndTime
				})
			})

			// get the weekdays for each most common end time
			weekdaysByEt := weekdayGroupInvertIf(etByWeekday, func(wd time.Weekday, k fusiongo.Time) bool {
				return len(actByWeekday[wd]) != 0
			})

			// re-group the activities including the weekday split
			for _, wds := range weekdaysByEt {
				activityKeyNew := activityKey
				activityKeyNew.Weekday = wds
				sch[activityKeyNew] = flatFilter(actByWeekday[:], func(i int) bool {
					return wds[i]
				})
			}
		}
		c.sch = sch
	}

	// get the activity keys
	c.schK = mapKeys(c.sch)

	// deterministically sort the map
	slices.SortStableFunc(c.schK, func(a, b activityKey) int {
		return cmpMulti(
			cmp.Compare(a.ActivityID, b.ActivityID),
			a.StartTime.Compare(b.StartTime),
			cmp.Compare(a.Location, b.Location),
			// HACK
			//  - simpler than manually writing a compare function
			//  - we only care that it's deterministic
			//  - we don't care about a specific order
			cmp.Compare(fmt.Sprint(a.Weekday), fmt.Sprint(b.Weekday)),
		)
	})
	for _, ak := range c.schK {
		slices.SortStableFunc(c.sch[ak], func(a, b activityInstance) int {
			return a.Date.Compare(b.Date)
		})
	}

	return nil
}

func (c *Calendar) initNotifications(notifications *fusiongo.Notifications) error {
	if notifications == nil {
		return nil
	}

	// init
	c.notT = notifications.Updated

	// clean, clone, and append notifications
	c.not = filterMap(notifications.Notifications, func(_ int, fn fusiongo.Notification) (notificationInstance, bool) {
		n := notificationInstance{
			ID:   fn.ID,
			Text: strings.TrimSpace(fn.Text),
			Sent: fn.Sent,
		}
		return n, true
	})

	// stable-sort by id
	slices.SortStableFunc(c.not, func(a, b notificationInstance) int {
		return strings.Compare(a.ID, b.ID)
	})

	return nil
}

// Matcher matches against filter against a list of strings.
type Matcher interface {
	Match(...string) bool
}

// Options contains options used to render a prepared calendar.
type Options struct {

	// Set the calendar name.
	CalendarName string

	// Set the calendar color (must be a CSS3 extended color keyword).
	CalendarColor string

	// Filter by category name.
	Category Matcher

	// Filter by category ID.
	CategoryID Matcher

	// Filter by activity name.
	Activity Matcher

	// Filter by activity UUID (without dashes).
	ActivityID Matcher

	// Filter by activity location.
	Location Matcher

	// Don't include notifications in the calendar.
	NoNotifications bool

	// Don't set the cancellation status on cancelled events (e.g., if you want outlook mobile to still show the events).
	FakeCancelled bool

	// Entirely exclude cancelled events.
	DeleteCancelled bool

	// List recurrence info and exceptions in event description.
	DescribeRecurrence bool
}

// Render renders c as an iCalendar object. It is safe for concurrent use.
func (c *Calendar) Render(o Options) []byte {
	b := icalAppendPropRaw(nil, "BEGIN", "VCALENDAR")

	// pre-filter activities
	schFilter := map[activityKey][]bool{}
	for _, ak := range c.schK {
		if o.Location != nil && !o.Location.Match(ak.Location) {
			continue
		}
		if o.ActivityID != nil && !o.ActivityID.Match(ak.ActivityID) {
			continue
		}
		for i, ai := range c.sch[ak] {
			if o.DeleteCancelled && ai.IsCancelled {
				continue
			}
			if o.Activity != nil && !o.Activity.Match(ai.Activity) {
				continue
			}
			if o.Category != nil && !o.Category.Match(ai.Categories...) {
				continue
			}
			if o.CategoryID != nil && !o.CategoryID.Match(ai.CategoryIDs...) {
				continue
			}
			if _, ok := schFilter[ak]; !ok {
				schFilter[ak] = make([]bool, len(c.sch[ak]))
			}
			schFilter[ak][i] = true
		}
	}

	// basic stuff
	b = icalAppendPropRaw(b, "VERSION", "2.0")
	b = icalAppendPropRaw(b, "PRODID", "IFGICAL")

	// calendar name
	if o.CalendarName != "" {
		b = icalAppendPropText(b, "NAME", o.CalendarName)
		b = icalAppendPropText(b, "X-WR-CALNAME", o.CalendarName)
	}

	// refresh interval
	b = icalAppendPropRaw(b, "REFRESH-INTERVAL;VALUE=DURATION", "PT60M")
	b = icalAppendPropRaw(b, "X-PUBLISHED-TTL", "PT60M")

	// last modified
	if c.notT.After(c.schT) {
		b = icalAppendPropDateTimeUTC(b, "LAST-MODIFIED", fusiongo.GoDateTime(c.notT.UTC()))
	} else {
		b = icalAppendPropDateTimeUTC(b, "LAST-MODIFIED", fusiongo.GoDateTime(c.schT.UTC()))
	}

	// calendar color
	if o.CalendarColor != "" {
		b = icalAppendPropRaw(b, "COLOR", o.CalendarColor)
	}

	// timezone
	b = append(b, c.tzV...)

	// schedule
	for _, ak := range c.schK {
		if _, include := schFilter[ak]; !include {
			continue
		}
		ais := c.sch[ak]

		// get the iCalendar (2-letter uppercase) days the instances occurs on
		akDays := weekdayFilterMap(ak.Weekday, func(wd time.Weekday, x bool) (string, bool) {
			if x {
				return strings.ToUpper(time.Weekday(wd).String()[:2]), true
			}
			return "", false
		})

		// generate a uid from all fields of activityKey
		uid := fmt.Sprintf(
			"%s-%x-%s-%02d%02d%02d@school%d.innosoftfusiongo.com",
			ak.ActivityID, sha1.Sum([]byte(ak.Location)),
			strings.Join(akDays, "-"),
			ak.StartTime.Hour, ak.StartTime.Minute, ak.StartTime.Second,
			c.id,
		)

		// base and last instances, recurrence (ignoring filters)
		var (
			aiBase = ais[0]
			aiLast = ais[len(ais)-1]
			recur  func(fn func(d fusiongo.Date, ex bool, i int)) // iterates over recurrence dates, using (d, true, -1) for exclusions, and setting ex if aiBase != ais[ai]
		)
		if len(ais) > 1 {

			// set the fields to the first (for determinism) most common values
			aiBase.EndTime = mostCommonBy(ais, func(ai activityInstance) fusiongo.Time {
				return ai.EndTime
			})
			aiBase.Activity = mostCommonBy(ais, func(ai activityInstance) string {
				return ai.Activity
			})
			aiBase.Description = mostCommonBy(ais, func(ai activityInstance) string {
				return ai.Description
			})
			aiBase.IsCancelled = false // cancellation should only be set on exceptions

			// set the recurrence function
			recur = func(fn func(d fusiongo.Date, ex bool, i int)) {
				for d := aiBase.Date; !aiLast.Date.Less(d); d = d.AddDays(1) {
					if ak.Weekday[d.Weekday()] {
						if i := slices.IndexFunc(ais, func(ai activityInstance) bool {
							return ai.Date == d
						}); i != -1 {
							ai := ais[i]
							fn(d, ai.EndTime != aiBase.EndTime || ai.Activity != aiBase.Activity || ai.Description != aiBase.Description || ai.IsCancelled, i)
						} else {
							fn(d, true, -1)
						}
					}
				}
			}
		}

		// describe recurrence information if enabled
		var excDesc strings.Builder
		if o.DescribeRecurrence && recur != nil {
			excDesc.WriteString("\n\n")
			excDesc.WriteString("Repeats ")
			excDesc.WriteString(ak.StartTime.StringCompact())
			excDesc.WriteString(" - ")
			excDesc.WriteString(aiBase.EndTime.StringCompact())
			if slices.Contains(ak.Weekday[:], false) {
				excDesc.WriteString(" [")
				for i, x := range akDays {
					if i != 0 {
						excDesc.WriteString(", ")
					}
					excDesc.WriteString(x)
				}
				excDesc.WriteString("]")
			}
			recur(func(d fusiongo.Date, ex bool, i int) {
				if ex {
					var (
						hasDiff bool
						diff    string
						diffs   []string
					)
					if i == -1 {
						diff = "not"
						hasDiff = true
					} else if !schFilter[ak][i] {
						diff = "does not match filter"
						hasDiff = true
					} else {
						ai := ais[i]
						switch {
						case ai.IsCancelled:
							diff = "cancelled"
							hasDiff = true
						case ai.EndTime != aiBase.EndTime:
							diff = "ends at " + ai.EndTime.StringCompact()
							hasDiff = true
						default:
							diff = "differs"
						}
						if ai.Activity != aiBase.Activity {
							diffs = append(diffs, "name="+strconv.Quote(ai.Activity))
							hasDiff = true
						}
						if ai.Description != aiBase.Description {
							diffs = append(diffs, "description="+strconv.Quote(ai.Description))
							hasDiff = true
						}
					}
					if hasDiff {
						excDesc.WriteString("\n • ")
						excDesc.WriteString(diff)
						excDesc.WriteString(" on ")
						excDesc.WriteString(fmt.Sprintf("%s %s %02d", d.Weekday().String()[:3], d.Month.String()[:3], d.Day))
						for i, x := range diffs {
							if i == 0 {
								excDesc.WriteString(": ")
							} else {
								excDesc.WriteString(", ")
							}
							excDesc.WriteString(x)
						}
					}
				}
			})
		}

		// write events
		writeEvent := func(ai activityInstance, base bool) {
			// write event info
			b = icalAppendPropRaw(b, "BEGIN", "VEVENT")
			b = icalAppendPropRaw(b, "UID", uid)
			b = icalAppendPropDateTimeUTC(b, "DTSTAMP", fusiongo.GoDateTime(c.schT.UTC())) // this should be when the event was created, but unfortunately, we can't determine that deterministically, so just use the schedule update time

			// write event status information if it's the only event or a recurrence exception
			if recur == nil || !base {
				if !ai.IsCancelled {
					b = icalAppendPropText(b, "SUMMARY", ai.Activity)
				} else {
					b = icalAppendPropText(b, "SUMMARY", "CANCELLED - "+ai.Activity)
					if !o.FakeCancelled {
						b = icalAppendPropRaw(b, "STATUS", "CANCELLED")
					}
				}
			} else {
				b = icalAppendPropText(b, "SUMMARY", ai.Activity)
			}

			// write more event info
			b = icalAppendPropText(b, "LOCATION", ak.Location)
			b = icalAppendPropText(b, "DESCRIPTION", ai.Description+excDesc.String())
			b = icalAppendPropDateTimeLocal(b, "DTSTART", ak.StartTime.WithDate(ai.Date), c.tz)
			b = icalAppendPropDateTimeLocal(b, "DTEND", ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End(), c.tz)

			// write custom props
			b = icalAppendPropText(b, "X-FUSION-ACTIVITY-ID", ak.ActivityID)
			for i := range ai.Categories {
				b = icalAppendPropText(b, "X-FUSION-CATEGORY;X-FUSION-CATEGORY-ID="+ai.CategoryIDs[i], ai.Categories[i])
			}

			// write recurrence info if it's a recurring event
			if recur != nil {
				if base {
					b = icalAppendPropRaw(b, "RRULE", fmt.Sprintf(
						"FREQ=WEEKLY;INTERVAL=1;UNTIL=%s;BYDAY=%s",
						string(icalAppendDateTimeUTC(nil, fusiongo.GoDateTime(ak.StartTime.WithEnd(aiLast.EndTime).WithDate(aiLast.Date).End().In(time.Local).UTC()))),
						strings.Join(akDays, ","),
					))
					recur(func(d fusiongo.Date, _ bool, i int) {
						if i == -1 || !schFilter[ak][i] {
							b = icalAppendPropDateTimeLocal(b, "EXDATE", ak.StartTime.WithDate(d), c.tz)
						}
					})
				} else {
					b = icalAppendPropDateTimeLocal(b, "RECURRENCE-ID", ak.StartTime.WithDate(ai.Date), c.tz)
				}
			}

			// done
			b = icalAppendPropRaw(b, "END", "VEVENT")
		}

		// write the base event
		writeEvent(aiBase, true)

		// write recurrence exceptions
		if recur != nil {
			recur(func(_ fusiongo.Date, ex bool, i int) {
				if ex {
					if i != -1 && schFilter[ak][i] {
						writeEvent(ais[i], false)
					}
				}
			})
		}
	}

	// notifications
	if !o.NoNotifications {
		for _, ni := range c.not {
			// https://stackoverflow.com/questions/1716237/single-day-all-day-appointments-in-ics-files
			uid := fmt.Sprintf("notification-%s@school%d.innosoftfusiongo.com", ni.ID, c.id)
			b = icalAppendPropRaw(b, "BEGIN", "VEVENT")
			b = icalAppendPropRaw(b, "UID", uid)
			b = icalAppendPropDateTimeUTC(b, "DTSTAMP", fusiongo.GoDateTime(c.notT.UTC()))
			b = icalAppendPropText(b, "SUMMARY", ni.Text)
			b = icalAppendPropText(b, "DESCRIPTION", ni.Text)
			b = icalAppendPropDate(b, "DTSTART;VALUE=DATE", ni.Sent.Date)
			b = icalAppendPropRaw(b, "END", "VEVENT")
		}
	}

	b = icalAppendPropRaw(b, "END", "VCALENDAR")
	return b
}

// cmpMulti combines multiple compare results for use in a sort function by
// returning the first non-zero (i.e., not equal) value.
func cmpMulti(v ...int) int {
	for _, x := range v {
		if x != 0 {
			return x
		}
	}
	return 0
}

// mapKeys returns a slice of map keys.
func mapKeys[T comparable, U any](m map[T]U) []T {
	ks := make([]T, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

// mostCommon returns the first seen most common T in xs, returning the zero
// value of T if xs is empty.
func mostCommon[T comparable](xs []T) (value T) {
	var (
		els      []T
		elCounts = map[T]int{}
	)
	for _, x := range xs {
		if _, seen := elCounts[x]; !seen {
			els = append(els, x)
		}
		elCounts[x]++
	}
	var elCount int
	for _, el := range els {
		if n := elCounts[el]; n > elCount {
			value = el
			elCount = n
		}
	}
	return
}

// mostCommonBy is like mostCommon, but converts V into T first.
func mostCommonBy[T comparable, V any](vs []V, fn func(V) T) (value T) {
	var xs []T
	for _, v := range vs {
		xs = append(xs, fn(v))
	}
	return mostCommon(xs)
}

// flatFilter flattens a, skipping values where !fn(i).
func flatFilter[T any](a [][]T, fn func(int) bool) []T {
	var r []T
	for i, x := range a {
		if fn(i) {
			r = append(r, x...)
		}
	}
	return r
}

// filterMap filters []T into []U, skipping elements where fn(i, T) == (U,
// false).
func filterMap[T, U any](xs []T, fn func(int, T) (U, bool)) []U {
	xn := make([]U, 0, len(xs))
	for i, x := range xs {
		if y, ok := fn(i, x); ok {
			xn = append(xn, y)
		}
	}
	return slices.Clip(xn)
}

// mapGroupInto groups xs by arbitrary keys, converting the values.
func mapGroupInto[T any, K comparable, V any](xs []T, fn func(int, T) (K, V)) map[K][]V {
	m := make(map[K][]V)
	for i, x := range xs {
		k, v := fn(i, x)
		m[k] = append(m[k], v)
	}
	return m
}

// weekdayMapOf is essentially a map[time.Weekday]T.
type weekdayMapOf[T any] [7]T

// weekdayGroupBy groups xs by arbitrary [time.Weekday] keys.
func weekdayGroupBy[T any](xs []T, fn func(int, T) time.Weekday) weekdayMapOf[[]T] {
	var m [7][]T
	for i, x := range xs {
		wd := fn(i, x)
		m[wd] = append(m[wd], x)
	}
	return m
}

// weekdayMapInto converts weekdayMapOf[T] into weekdayMapOf[U].
func weekdayMapInto[T any, U any](xs weekdayMapOf[T], fn func(time.Weekday, T) U) weekdayMapOf[U] {
	var r weekdayMapOf[U]
	for i, x := range xs {
		r[i] = fn(time.Weekday(i), x)
	}
	return r
}

// weekdayFilterMap converts weekdayMapOf[T] into []U where fn(T) returns (U, true).
func weekdayFilterMap[T, U any](xs weekdayMapOf[T], fn func(time.Weekday, T) (U, bool)) []U {
	var r []U
	for i, x := range xs {
		if u, ok := fn(time.Weekday(i), x); ok {
			r = append(r, u)
		}
	}
	return r
}

// weekdayGroupInvertIf returns r such that r[k][weekday] iff fn(wd, k) and
// m[weekday] == k. Note that this means that r[k][weekday] will only be true
// for a single k.
func weekdayGroupInvertIf[T comparable](m weekdayMapOf[T], fn func(time.Weekday, T) bool) map[T][7]bool {
	r := map[T][7]bool{}
	for i, k := range m {
		if fn(time.Weekday(i), k) {
			x := r[k]
			x[i] = true
			r[k] = x
		}
	}
	return r
}

// icalAppendPropRaw appends an iCalendar property to b, unchecked.
func icalAppendPropRaw(b []byte, key, value string) []byte {
	b = append(b, key...)
	b = append(b, ':')
	b = append(b, value...)
	b = append(b, '\r', '\n')
	return b
}

// icalAppendPropText appends a iCalendar text property to b.
func icalAppendPropText(b []byte, key string, ss ...string) []byte {
	b = append(b, key...)
	b = append(b, ':')
	b = icalAppendTextValue(b, ss...)
	b = append(b, '\r', '\n')
	return b
}

// icalAppendTextValue appends an escaped iCalendar text value to b.
func icalAppendTextValue(b []byte, ss ...string) []byte {
	for i, s := range ss {
		if i != 0 {
			b = append(b, ',')
		}
		b = slices.Grow(b, len(s))
		for i, c := range s {
			switch c {
			case '\r':
				if i+1 == len(s) || s[i+1] != '\n' {
					b = utf8.AppendRune(b, c)
				}
			case '\n':
				b = append(b, '\\', 'n')
			case '\\', ';', ',':
				b = append(b, '\\', byte(c))
			default:
				b = utf8.AppendRune(b, c)
			}
		}
	}
	return b
}

// icalAppendPropDate appends a date property.
func icalAppendPropDate(b []byte, key string, d fusiongo.Date) []byte {
	return fmt.Appendf(b, "%s:%04d%02d%02d\r\n", key, d.Year, d.Month, d.Day)
}

// icalAppendPropDateTimeLocal appends a date-time property, floating if tz is
// nil, and with TZID as the last param otherwise.
func icalAppendPropDateTimeLocal(b []byte, key string, dt fusiongo.DateTime, tz *time.Location) []byte {
	if tz != nil {
		return fmt.Appendf(b, "%s;TZID=%s:%04d%02d%02dT%02d%02d%02d\r\n", key, tz, dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
	}
	return fmt.Appendf(b, "%s:%04d%02d%02dT%02d%02d%02d\r\n", key, dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
}

// icalAppendPropDateTimeUTC appends a date-time property.
func icalAppendPropDateTimeUTC(b []byte, key string, dt fusiongo.DateTime) []byte {
	return fmt.Appendf(b, "%s:%04d%02d%02dT%02d%02d%02dZ\r\n", key, dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
}

// icalAppendDateTimeUTC appends a date-time.
func icalAppendDateTimeUTC(b []byte, dt fusiongo.DateTime) []byte {
	return fmt.Appendf(b, "%04d%02d%02dT%02d%02d%02dZ", dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
}