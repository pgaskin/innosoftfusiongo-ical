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
	"unsafe"

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
	id   int                                 // school id
	tz   *time.Location                      // for local times
	tzV  []byte                              // VTimezone
	sch  map[activityKey]activityRecurrences // stable-sorted by date, all activityInstances fall on a activityKey.Weekday
	schK []activityKey                       // stable-sorted keys of sch
	schT time.Time                           // last modified
	not  []notificationInstance              // stable-sorted by id
	notT time.Time                           // last modified
}

// activityKey contains fields common to all instances (i.e., a base
// recurrence).
type activityKey struct {
	ActivityID string
	Location   string
	StartTime  fusiongo.Time
	Weekday    weekdayMapOf[bool]
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

// activityRecurrences contains information about activity recurrences.
type activityRecurrences struct {
	Base      activityInstance
	Weekday   weekdayMapOf[bool]
	Instances []activityInstance // sorted, len(Instances) >= 1, no recurrence if == 1
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
	if v, err := vtimezone.Append(nil, tz); err != nil {
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
	sch := remap(

		// first, do it by the unique fields other than the date
		mapGroupInto(schedule.Activities, func(i int, fai fusiongo.ActivityInstance) (activityKey, activityInstance) {
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
		}),

		// then regroup it by the weekday, splitting days with consistently different end times
		func(ak activityKey, ai []activityInstance, sch map[activityKey][]activityInstance) {

			// group the activities by their weekday
			actByWeekday := weekdayGroupBy(ai, func(i int, ai activityInstance) time.Weekday {
				return ai.Date.WithTime(ak.StartTime).Weekday()
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
				activityKeyNew := ak
				activityKeyNew.Weekday = wds
				sch[activityKeyNew] = flatFilter(actByWeekday[:], func(i int) bool {
					return wds[i]
				})
			}
		},
	)

	// get the activity keys
	c.schK = mapKeysSortedFunc(sch, func(a, b activityKey) int {
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

	// deterministically sort the instances
	for _, ak := range c.schK {
		slices.SortStableFunc(sch[ak], func(a, b activityInstance) int {
			return a.Date.Compare(b.Date)
		})
	}

	// build the recurrence information
	c.sch = make(map[activityKey]activityRecurrences, len(c.schK))
	for _, ak := range c.schK {
		ais := sch[ak]

		// set the fields to the first (for determinism) most common values
		c.sch[ak] = activityRecurrences{
			Weekday:   ak.Weekday,
			Instances: ais,
			Base: activityInstance{

				// set the fields to the first (for determinism) most common values
				Activity: mostCommonBy(ais, func(ai activityInstance) string {
					return ai.Activity
				}),
				Description: mostCommonBy(ais, func(ai activityInstance) string {
					return ai.Description
				}),
				EndTime: mostCommonBy(ais, func(ai activityInstance) fusiongo.Time {
					return ai.EndTime
				}),
				Categories: strings.Split(mostCommonBy(ais, func(v activityInstance) string {
					return strings.Join(v.Categories, "\x00") // HACK
				}), "\x00"),
				CategoryIDs: strings.Split(mostCommonBy(ais, func(v activityInstance) string {
					return strings.Join(v.CategoryIDs, "\x00") // HACK
				}), "\x00"),

				// use the earliest date
				Date: ais[0].Date,

				// cancellation should only be set on exceptions
				IsCancelled: false,
			},
		}
	}

	return nil
}

// Recur returns true if r recurs.
func (r activityRecurrences) Recur() bool {
	return len(r.Instances) != 1
}

// Iter iterates over recurrences for r.
func (r activityRecurrences) Iter(fn func(d fusiongo.Date, ex bool, i int)) {
	for d := r.Base.Date; !r.Instances[len(r.Instances)-1].Date.Less(d); d = d.AddDays(1) {
		if r.Weekday[d.Weekday()] {
			ex, i := true, slices.IndexFunc(r.Instances, func(ai activityInstance) bool {
				return ai.Date == d
			})
			if i != -1 {
				ai, aib := r.Instances[i], r.Base
				switch {
				case ai.IsCancelled != aib.IsCancelled:
				case ai.Activity != aib.Activity:
				case ai.Description != aib.Description:
				case ai.EndTime != aib.EndTime:
				case !slices.Equal(ai.Categories, aib.Categories):
				case !slices.Equal(ai.CategoryIDs, aib.CategoryIDs):
				default:
					ex = false // no difference
				}
			}
			fn(d, ex, i)
		}
	}
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

func (c *Calendar) filter(o Options) map[activityKey][]bool {
	schFilter := map[activityKey][]bool{}
	for _, ak := range c.schK {
		if o.Location != nil && !o.Location.Match(ak.Location) {
			continue
		}
		if o.ActivityID != nil && !o.ActivityID.Match(ak.ActivityID) {
			continue
		}
		for i, ai := range c.sch[ak].Instances {
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
				schFilter[ak] = make([]bool, len(c.sch[ak].Instances))
			}
			schFilter[ak][i] = true
		}
	}
	return schFilter
}

// RenderICS renders c as an iCalendar object. It is safe for concurrent use.
//
// The output is designed to be compatible with most calendar applications,
// including:
//
//   - Microsoft Exchange Server (OWA)
//   - Microsoft Outlook
//   - Google Calendar
//   - ICSx5/iCal4j
//   - Apple Calendar (iCloud, iOS, macOS)
//   - Thunderbird
//
// It is compliant with the following specifications (the MS one is generally a
// subset of the RFCs, and for the common properties, the intersection of it
// with the RFCs is close to the lowest common denominator of most clients):
//
//   - https://datatracker.ietf.org/doc/html/rfc5545
//   - https://datatracker.ietf.org/doc/html/rfc6868
//   - https://datatracker.ietf.org/doc/html/rfc7986
//   - https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcical/a685a040-5b69-4c84-b084-795113fb4012
//
// In particular, VTimezone objects and RRULEs are limited to a strict subset to
// ensure compatibility.
func (c *Calendar) RenderICS(o Options) []byte {
	schFilter := c.filter(o)

	b := icalAppendPropRaw(nil, "BEGIN", "VCALENDAR")

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
		ar := c.sch[ak]

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

		// describe recurrence information if enabled
		excDesc := c.describeRecurrence(ak, schFilter)
		if excDesc != "" {
			excDesc = "\n\n" + excDesc
		}

		// write events
		writeEvent := func(ai activityInstance, base bool) {
			// write event info
			b = icalAppendPropRaw(b, "BEGIN", "VEVENT")
			b = icalAppendPropRaw(b, "UID", uid)
			b = icalAppendPropDateTimeUTC(b, "DTSTAMP", fusiongo.GoDateTime(c.schT.UTC())) // this should be when the event was created, but unfortunately, we can't determine that deterministically, so just use the schedule update time

			// write event status information if it's the only event or a recurrence exception
			if !ar.Recur() || !base {
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
			b = icalAppendPropText(b, "DESCRIPTION", ai.Description+excDesc)
			b = icalAppendPropDateTimeLocal(b, "DTSTART", ak.StartTime.WithDate(ai.Date), c.tz)
			b = icalAppendPropDateTimeLocal(b, "DTEND", ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End(), c.tz)
			// note: not CATEGORIES since it isn't supported by most applications, and it breaks the Outlook Web App (causing the filter list to be empty) as of 2023-09-23

			// write custom props
			b = icalAppendPropText(b, "X-FUSION-ACTIVITY-ID", ak.ActivityID)
			for i := range ai.Categories {
				b = icalAppendPropText(b, "X-FUSION-CATEGORY;X-FUSION-CATEGORY-ID="+ai.CategoryIDs[i], ai.Categories[i])
			}

			// write recurrence info if it's a recurring event
			if ar.Recur() {
				if base {
					b = icalAppendPropRaw(b, "RRULE", fmt.Sprintf(
						"FREQ=WEEKLY;INTERVAL=1;UNTIL=%s;BYDAY=%s",
						string(icalAppendDateTimeUTC(nil, fusiongo.GoDateTime(ak.StartTime.WithEnd(ar.Instances[len(ar.Instances)-1].EndTime).WithDate(ar.Instances[len(ar.Instances)-1].Date).End().In(time.Local).UTC()))),
						strings.Join(akDays, ","),
					))
					ar.Iter(func(d fusiongo.Date, _ bool, i int) {
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
		if ar.Recur() {
			writeEvent(ar.Base, true)
		} else {
			writeEvent(ar.Instances[0], true)
		}

		// write recurrence exceptions
		if ar.Recur() {
			ar.Iter(func(_ fusiongo.Date, ex bool, i int) {
				if ex {
					if i != -1 && schFilter[ak][i] {
						writeEvent(ar.Instances[i], false)
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

// RenderJSON renders c as a JSON object. It is safe for concurrent use.
//
// Some notes about the output:
//   - As an internal convention, camelCase is used for keys, and underscores
//     are used to show alternate representations of data.
//   - Instances can be built by assigning properties from each instance in the
//     instances array over the base instance.
//   - All instances include the date, isException, and isExclusion.
//   - If an activity does not recur, there will be exactly one instance, which
//     will always be !isException and !isExclusion.
//   - If isExclusion, then isException will be true.
func (c *Calendar) RenderJSON(o Options) []byte {
	schFilter := c.filter(o)

	b := jsonObject(nil, '{')

	// timezone
	b = jsonStr(jsonKey(b, "timezone"), c.tz.String())

	// update times
	b = jsonObject(jsonKey(b, "updated"), '{')
	if !c.schT.IsZero() {
		b = jsonTime(jsonKey(b, "schedule"), c.schT)
	}
	if !c.notT.IsZero() {
		b = jsonTime(jsonKey(b, "notifications"), c.notT)
	}
	b = jsonObject(b, '}')

	// schedule
	b = jsonObject(jsonKey(b, "schedule"), '[')
	for _, ak := range c.schK {
		if _, include := schFilter[ak]; !include {
			continue
		}
		ar := c.sch[ak]

		writeEvent := func(d fusiongo.Date, ex bool, i int) {
			// base recurrence properties
			b = jsonStr(jsonKey(b, "date"), d.String())
			if i != -2 {
				b = jsonBool(jsonKey(b, "isException"), ex)
				b = jsonBool(jsonKey(b, "isExclusion"), i == -1)
			}
			b = jsonTime(jsonKey(b, "date_at"), d.In(c.tz))
			b = jsonStr(jsonKey(b, "date_weekday"), d.Weekday().String())
			b = jsonInt(jsonKey(b, "date_weekday_num"), d.Weekday())

			// instance information
			if i == -2 || i >= 0 {
				var ai activityInstance
				if i == -2 {
					ai = ar.Base
				} else {
					ai = ar.Instances[i]
				}
				if i != -2 && ai.IsCancelled != ar.Base.IsCancelled { // only if not the base instance
					b = jsonBool(jsonKey(b, "isCancelled"), ai.IsCancelled)
				}
				if i == -2 || ai.Activity != ar.Base.Activity {
					b = jsonStr(jsonKey(b, "activity"), ai.Activity)
				}
				if o.FakeCancelled && ai.IsCancelled {
					ai.Description = "CANCELLED - " + ai.Description
				}
				if i == -2 || ai.Description != ar.Base.Description {
					b = jsonStr(jsonKey(b, "description"), ai.Description)
				}
				if i == -2 || ai.EndTime != ar.Base.EndTime {
					b = jsonStr(jsonKey(b, "endTime"), ai.EndTime.StringCompact())
				}
				if i == -2 || !slices.Equal(ai.Categories, ar.Base.Categories) {
					b = jsonObject(jsonKey(b, "categories"), '[')
					for i := range ai.Categories {
						b = jsonObject(b, '{')
						b = jsonStr(jsonKey(b, "id"), ai.CategoryIDs[i])
						b = jsonStr(jsonKey(b, "name"), ai.Categories[i])
						b = jsonObject(b, '}')
					}
					b = jsonObject(b, ']')
				}
				b = jsonStr(jsonKey(b, "_start"), ak.StartTime.WithDate(ai.Date).String())
				b = jsonTime(jsonKey(b, "_start_at"), ak.StartTime.WithDate(ai.Date).In(c.tz))
				b = jsonStr(jsonKey(b, "_end"), ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().String())
				b = jsonTime(jsonKey(b, "_end_at"), ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz))
				b = jsonStr(jsonKey(b, "_duration"), ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).Sub(ak.StartTime.WithDate(ai.Date).In(c.tz)).Truncate(time.Second).String())
				b = jsonInt(jsonKey(b, "_duration_secs"), int(ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).Sub(ak.StartTime.WithDate(ai.Date).In(c.tz)).Truncate(time.Second).Seconds()))
			}
		}

		b = jsonObject(b, '{')
		b = jsonStr(jsonKey(b, "activityID"), ak.ActivityID)
		b = jsonStr(jsonKey(b, "location"), ak.Location)
		b = jsonStr(jsonKey(b, "startTime"), ak.StartTime.StringCompact())
		b = jsonObject(jsonKey(b, "weekdays"), '[')
		for _, x := range ar.Weekday {
			b = jsonBool(b, x)
		}
		b = jsonObject(b, ']')
		b = jsonObject(jsonKey(b, "base"), '{')
		writeEvent(ar.Base.Date, false, -2)
		b = jsonObject(b, '}')
		b = jsonObject(jsonKey(b, "instances"), '[')
		ar.Iter(func(d fusiongo.Date, ex bool, i int) {
			if i != -1 && !schFilter[ak][i] {
				i = -1
			}
			b = jsonObject(b, '{')
			writeEvent(d, ex, i)
			b = jsonObject(b, '}')
		})
		b = jsonObject(b, ']')
		if o.DescribeRecurrence && ar.Recur() {
			b = jsonStr(jsonKey(b, "recurrenceDescription"), c.describeRecurrence(ak, schFilter))
		}
		b = jsonObject(b, '}')
	}
	b = jsonObject(b, ']')

	// notifications
	b = jsonObject(jsonKey(b, "notifications"), '[')
	if !o.NoNotifications {
		for _, ni := range c.not {
			b = jsonObject(b, '{')
			b = jsonStr(jsonKey(b, "id"), ni.ID)
			b = jsonStr(jsonKey(b, "text"), ni.Text)
			b = jsonStr(jsonKey(b, "sent"), ni.Sent.String())
			b = jsonTime(jsonKey(b, "sent_at"), ni.Sent.In(c.tz))
			b = jsonObject(b, '}')
		}
	}
	b = jsonObject(b, ']')

	return jsonObject(b, '}')
}

// RenderJSON renders c as a FullCalendar JSON EventSource. It is safe for
// concurrent use.
//
// The output is compatible with the FullCalendar iCalendar plugin, and should
// result in the same output as using it with RenderICS.
func (c *Calendar) RenderFullCalendarJSON(o Options) []byte {
	schFilter := c.filter(o)

	b := jsonObject(nil, '[')

	for _, ak := range c.schK {
		if _, include := schFilter[ak]; include {
			ar := c.sch[ak]
			rd := ""
			if o.DescribeRecurrence {
				if rd = c.describeRecurrence(ak, schFilter); rd != "" {
					rd = "\n\n" + rd
				}
			}
			ar.Iter(func(_ fusiongo.Date, ex bool, i int) {
				if i != -1 && schFilter[ak][i] {
					ai := ar.Instances[i]
					if !ai.IsCancelled || !o.DeleteCancelled {
						b = jsonObject(b, '{')
						b = jsonStr(jsonKey(b, "title"), ai.Activity)
						b = jsonInt(jsonKey(b, "start"), ak.StartTime.WithDate(ai.Date).In(c.tz).UnixMilli())
						b = jsonInt(jsonKey(b, "end"), ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).UnixMilli())
						b = jsonBool(jsonKey(b, "allDay"), false)
						b = jsonObject(jsonKey(b, "extendedProps"), '{')
						b = jsonStr(jsonKey(b, "description"), ai.Description+rd) // same as icalendar plugin
						b = jsonStr(jsonKey(b, "location"), ak.Location)          // same as icalendar plugin
						b = jsonBool(jsonKey(b, "isRecurrence"), ar.Recur())      // custom
						b = jsonBool(jsonKey(b, "isException"), ex)               // custom
						b = jsonObject(b, '}')
						b = jsonObject(b, '}')
					}
				}
			})
		}
	}

	if !o.NoNotifications {
		for _, ni := range c.not {
			b = jsonObject(b, '{')
			b = jsonStr(jsonKey(b, "title"), ni.Text)
			b = jsonInt(jsonKey(b, "start"), ni.Sent.In(c.tz).UnixMilli())
			b = jsonBool(jsonKey(b, "allDay"), true)
			b = jsonObject(jsonKey(b, "extendedProps"), '{')
			b = jsonStr(jsonKey(b, "description"), ni.Text)
			b = jsonObject(b, '}')
			b = jsonObject(b, '}')
		}
	}

	return jsonObject(b, ']')
}

func (c *Calendar) describeRecurrence(ak activityKey, schFilter map[activityKey][]bool) string {
	var excDesc strings.Builder
	ar := c.sch[ak]
	if ar.Recur() {
		excDesc.WriteString("Repeats ")
		excDesc.WriteString(ak.StartTime.StringCompact())
		excDesc.WriteString(" - ")
		excDesc.WriteString(ar.Base.EndTime.StringCompact())
		if slices.Contains(ak.Weekday[:], false) {
			excDesc.WriteString(" [")
			for i, x := range weekdayFilterMap(ak.Weekday, func(wd time.Weekday, x bool) (string, bool) {
				if x {
					return strings.ToUpper(time.Weekday(wd).String()[:2]), true
				}
				return "", false
			}) {
				if i != 0 {
					excDesc.WriteString(", ")
				}
				excDesc.WriteString(x)
			}
			excDesc.WriteString("]")
		}
		ar.Iter(func(d fusiongo.Date, ex bool, i int) {
			if ex {
				var (
					hasDiff bool
					diff    string
					diffs   []string
				)
				if i == -1 {
					diff = "not"
					hasDiff = true
				} else if !schFilter[ak][i] && !ar.Instances[i].IsCancelled { // still say cancelled even if DeleteCancelled is enabled
					diff = "does not match filter"
					hasDiff = true
				} else {
					ai := ar.Instances[i]
					switch {
					case ai.IsCancelled:
						diff = "cancelled"
						hasDiff = true
					case ai.EndTime != ar.Base.EndTime:
						diff = "ends at " + ai.EndTime.StringCompact()
						hasDiff = true
					default:
						diff = "differs"
					}
					if ai.Activity != ar.Base.Activity {
						diffs = append(diffs, "name="+strconv.Quote(ai.Activity))
						hasDiff = true
					}
					if ai.Description != ar.Base.Description {
						diffs = append(diffs, "description="+strconv.Quote(ai.Description))
						hasDiff = true
					}
					if !slices.Equal(ai.Categories, ar.Base.Categories) {
						diffs = append(diffs, "categories="+fmt.Sprintf("%q", ai.Categories))
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
	return excDesc.String()
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

// mapKeysSortedFunc returns a stable-sorted slice of map keys.
func mapKeysSortedFunc[T comparable, U any](m map[T]U, cmp func(T, T) int) []T {
	ks := mapKeys(m)
	slices.SortStableFunc(ks, cmp)
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

// remap iterates over a map, building a new one.
func remap[K comparable, V any](m map[K]V, fns ...func(K, V, map[K]V)) map[K]V {
	for _, fn := range fns {
		n := map[K]V{}
		for k, v := range m {
			if fn != nil {
				fn(k, v, n)
			} else {
				n[k] = v
			}
		}
		m = n
	}
	return m
}

// last gets the last element of a, or returns the zero value if len(a) == 0.
func last[T any](a []T) (v T) {
	if len(a) != 0 {
		v = a[len(a)-1]
	}
	return
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
		x := 0 // note: this won't break utf-8 since we only check for < 0x20
		for i := 0; i < len(s); {
			if s[i] == '\r' && i+1 != len(s) && s[i+1] == '\n' {
				b = append(b, s[x:i]...)
				// skip the \r since it's followed by a \n
				i++
				x = i
				continue
			}
			if c := s[i]; c == '\n' || c == '\\' || c == ';' || c == ',' {
				b = append(b, s[x:i]...)
				if c == '\n' {
					b = append(b, '\\', 'n')
				} else {
					b = append(b, '\\', c)
				}
				i++
				x = i
				continue
			}
			i++
		}
		b = append(b, s[x:]...)
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

func jsonKey(b []byte, k string) []byte {
	b = jsonObject(b, ',')
	b = append(b, '"')
	b = append(b, k...)
	b = append(b, '"', ':')
	return b
}

func jsonObject(b []byte, c byte) []byte {
	switch c {
	case '{', '[': // start object/array
		switch last(b) {
		case 0, ':', ',', '[':
			b = append(b, c)
			return b
		case ']', '}', 'l', 'e', '"', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			b = append(b, ',')
			b = append(b, c)
			return b
		}
	case '}', ']': // end object/array
		switch last(b) {
		case '[', '{', ']', '}', 'l', 'e', '"', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			b = append(b, c)
			return b
		case ':', ',':
			b[len(b)-1] = c
			return b
		}
	case ',': // key
		switch last(b) {
		case 0, '{', ',':
			return b
		case ']', '}', 'l', 'e', '"', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			b = append(b, ',')
			return b
		}
	case ':': // value
		switch last(b) {
		case 0, ':', ',', '[':
			return b
		case ']', '}', 'l', 'e', '"', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			b = append(b, ',')
			return b
		}
	}
	if i := len(b) - 100; i > 0 {
		panic("json: cannot add " + string(c) + " at ..." + string(b[i:]))
	}
	panic("json: cannot add " + string(c) + " at " + string(b))
}

func jsonNull(b []byte) []byte {
	return append(jsonObject(b, ':'), "null"...)
}

func jsonBool[T ~bool](b []byte, v T) []byte {
	b = jsonObject(b, ':')
	if v {
		b = append(b, "true"...)
	} else {
		b = append(b, "false"...)
	}
	return b
}

func jsonInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64](b []byte, n T) []byte {
	return strconv.AppendInt(jsonObject(b, ':'), int64(n), 10)
}

func jsonUint[T ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](b []byte, n T) []byte {
	return strconv.AppendUint(jsonObject(b, ':'), uint64(n), 10)
}

func jsonFloat[T ~float32 | ~float64](b []byte, n T) []byte {
	return strconv.AppendFloat(jsonObject(b, ':'), float64(n), 'f', -1, int(unsafe.Sizeof(n)*8))
}

func jsonStr[T ~[]byte | ~string](b []byte, s T) []byte {
	b = jsonObject(b, ':')
	b = slices.Grow(b, len(s)+2)
	b = append(b, '"')
	x := 0 // note: this won't break utf-8 since we only check for < 0x20
	for i := 0; i < len(s); {
		if c := s[i]; c < 0x20 || c == '\\' || c == '"' {
			b = append(b, s[x:i]...)
			switch c {
			case '\\', '"':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', "0123456789abcdef"[c>>4], "0123456789abcdef"[c&0xF])
			}
			i++
			x = i
			continue
		}
		i++
	}
	b = append(b, s[x:]...)
	b = append(b, '"')
	return b
}

func jsonTime(b []byte, t time.Time) []byte {
	x, err := t.MarshalText() // RFC3339
	if err != nil {
		panic(err)
	}
	return jsonStr(b, x)
}
