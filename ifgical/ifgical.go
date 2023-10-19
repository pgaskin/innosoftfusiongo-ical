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
	StartTime  fusiongo.Time // all instances with this activity/location/start are part of this key, but not all may have this start time (if another key was merged into this one... see the end of initSchedule)
	Weekday    weekdayMapOf[bool]
}

// for debugging
func (ak activityKey) String() string {
	return fmt.Sprintf("ak(id=%s loc=%q st=%s wd=%s)",
		ak.ActivityID,
		ak.Location,
		ak.StartTime.StringCompact(),
		strings.Join(weekdayFilterMap(ak.Weekday, func(wd time.Weekday, x bool) (string, bool) {
			if x {
				return strings.ToUpper(time.Weekday(wd).String()[:2]), true
			}
			return "", false
		}), ","),
	)
}

// activityInstance contains fields specific to an instance.
type activityInstance struct {
	IsCancelled bool
	Activity    string
	Description string
	Date        fusiongo.Date
	StartTime   fusiongo.Time
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

	// Queen's University (school110) frequently does this silly thing where
	// they don't actually use the cancellation feature of Innosoft Fusion, and
	// they instead remove the entire instance, and add an entirely new activity
	// with a single instance which is the same except it has " - CANCELLED" or
	// " - CANCELED" appended to it... smh...
	{
		var (
			stupidCancelled     []int
			cancelledActivities []int
			syntheticActivities []fusiongo.ActivityInstance
		)
		for i, fa := range schedule.Activities {

			// check if it's a fake-cancelled event
			name, cancelled := strings.CutSuffix(fa.Activity, " - CANCELLED")
			if !cancelled {
				name, cancelled = strings.CutSuffix(fa.Activity, " - CANCELED")
			}
			if !cancelled {
				continue
			}

			// find matching activities
			fn := func(exact bool) (m []int) {
				for i1, fa1 := range schedule.Activities {
					if fa1.Activity != name {
						continue // activity should be the same, but without the cancelled part
					}
					if fa1.ActivityID == fa.ActivityID {
						continue // activityID should be different
					}
					if fa1.Description != "" && fa1.Description != fa.Description {
						continue // description should be empty or identical
					}
					if fa1.Location != fa.Location {
						continue // location should be the same
					}
					if fa1.Time.TimeRange.Start != fa.Time.TimeRange.Start {
						continue // start should be the same (end time doesn't matter; it could be an exception that was cancelled, plus we group by start time -- see below)
					}
					if exact {
						if fa1.Time.TimeRange.End != fa.Time.TimeRange.End {
							continue // end time should be the same
						}
						if fa1.Time.Date != fa.Time.Date {
							continue // date should be the same
						}
					}
					m = append(m, i1)
				}
				return
			}

			// if we have an exact match, set it to cancelled and drop the fake cancellation
			if m := fn(true); len(m) == 1 {
				slog.Debug(fmt.Sprintf("dropping fake cancellation activity %d (id=%s name=%q date=%s) for existing cancelled activity %d (id=%s name=%q)", i, schedule.Activities[i].ActivityID, schedule.Activities[i].Activity, schedule.Activities[i].Time.Date, m[0], schedule.Activities[m[0]].ActivityID, schedule.Activities[m[0]].Activity))
				stupidCancelled = append(stupidCancelled, i)
				cancelledActivities = append(cancelledActivities, m[0])
				continue
			}

			// if not, but we have a match for the recurrence group, add an instance and drop the fake cancellation
			if m := fn(false); len(m) >= 1 {
				slog.Debug(fmt.Sprintf("converting fake cancellation activity %d (id=%s name=%q date=%s) into real cancellation for activity %d (id=%s name=%q)", i, schedule.Activities[i].ActivityID, schedule.Activities[i].Activity, schedule.Activities[i].Time.Date, m[0], schedule.Activities[m[0]].ActivityID, schedule.Activities[m[0]].Activity))
				stupidCancelled = append(stupidCancelled, i)
				syntheticActivities = append(syntheticActivities, schedule.Activities[m[0]])
				syntheticActivities[len(syntheticActivities)-1].Time = fa.Time
				syntheticActivities[len(syntheticActivities)-1].IsCancelled = true
				syntheticActivities[len(syntheticActivities)-1].Category = slices.Clone(syntheticActivities[len(syntheticActivities)-1].Category)
				continue
			}
		}

		// if we have things to process, clone the schedule and do stuff
		if len(stupidCancelled) != 0 {
			schedule = cloneSchedule(schedule)

			// set cancellations
			for _, x := range cancelledActivities {
				schedule.Activities[x].IsCancelled = true
			}

			// delete the processed fake cancellations in reverse order
			for i := len(stupidCancelled) - 1; i >= 0; i-- {
				x := stupidCancelled[i]
				schedule.Activities = slices.Delete(schedule.Activities, x, x+1)
			}

			// add the new synthetic instances
			schedule.Activities = append(schedule.Activities, syntheticActivities...)
		}
	}

	// check some basic assumptions
	// - we depend (for correctness, not code) on the fact that an activity is uniquely identified by its id and location and can only occur once per start time per day
	// - we also depend on each activity having at least one category (this should always be true)
	{
		faSeen := map[string]fusiongo.ActivityInstance{}
		for _, fa := range schedule.Activities {
			iid := fmt.Sprint(fa.ActivityID, fa.Location, fa.Time.Date, fa.Time.TimeRange.Start)
			if x, seen := faSeen[iid]; !seen {
				faSeen[iid] = fa
			} else if !reflect.DeepEqual(fa, x) {
				slog.Warn("activity instance is not uniquely identified by (id, location, date, start)", "activity1", fa, "activity2", x)
			}
			if len(fa.Category) == 0 {
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
				StartTime:   fai.Time.TimeRange.Start,
				EndTime:     fai.Time.TimeRange.End,
				Categories:  fai.CategoryNames(),
				CategoryIDs: fai.CategoryIDs(),
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
			for et, wds := range weekdaysByEt {
				activityKeyNew := ak
				activityKeyNew.Weekday = wds
				sch[activityKeyNew] = flatFilter(actByWeekday[:], func(i int) bool {
					return wds[i]
				})
				slog.Debug(fmt.Sprintf("recurrence group %v (et=%s, n=%d)", activityKeyNew, et.StringCompact(), len(sch[activityKeyNew])))
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

				// use the start time from the key
				StartTime: ak.StartTime,

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
				Categories: strings.Split(mostCommonBy(ais, func(ai activityInstance) string {
					return strings.Join(ai.Categories, "\x00") // HACK
				}), "\x00"),
				CategoryIDs: strings.Split(mostCommonBy(ais, func(ai activityInstance) string {
					return strings.Join(ai.CategoryIDs, "\x00") // HACK
				}), "\x00"),

				// use the earliest date
				Date: ais[0].Date,

				// cancellation should only be set on exceptions
				IsCancelled: false,
			},
		}
	}

	// merge recurrence groups which are strict subsets of recurrence exclusions (plus/minus the previous/next occurrence), and have overlapping times
	// example: event with instances MO/WE on MO wk1 and WE wk2 (with a different start time, but same end time) merges into event with instances on MO/WE/FR on WE wk1, FR wk1, MO wk2, FR wk2
	// example: testdata/20231015/school110 lane swim shallow end at 16:00-18:00 TU merges with shallow end lane swim 14:30-18:00 TU
	//
	//	FROM
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   Starts Tue 2023-10-17
	//	   Until Tue 2023-11-21
	//	   Repeats 14:30 - 18:00 [TU]
	//	   • not on Tue Oct 24
	//
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   Starts Tue 2023-10-24
	//	   Until Tue 2023-11-28
	//	   Repeats 16:00 - 18:00 [TU]
	//	   • not on Tue Oct 31
	//	   • not on Tue Nov 07
	//	   • not on Tue Nov 14
	//	   • not on Tue Nov 21
	//
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   Starts Thu 2023-10-12
	//	   Until Thu 2023-11-23
	//	   Repeats 14:30 - 17:45 [TH]
	//	   • not on Thu Oct 26
	//
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   On Thu 2023-10-26
	//	   At 16:00
	//
	// TO
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   Starts Tue 2023-10-17
	//	   Until Tue 2023-11-28
	//	   Repeats 14:30 - 18:00 [TU]
	//	   • starts at 16:00 on Tue Oct 24
	//	   • starts at 16:00 on Tue Nov 28
	//
	//	  Member Lane Swim
	//	   In Pool - Shallow End
	//	   Starts Thu 2023-10-12
	//	   Until Thu 2023-11-23
	//	   Repeats 14:30 - 17:45 [TH]
	//	   • starts at 16:00 on Thu Oct 26
	//
	// note: test using the describe recurrence stuff, e.g., with the last jq example in the README
	// note: this is really inefficient, but I'd rather get it right, and keep the rest of the logic used for most cases simple
	// note: we don't have to treat non-recurring instances specially since they'll automatically become recurring if we add another instance
	// note: we don't attempt to find the optimal solution for cases where there are chains of merges since this is a rare edge-case anyways
	{
		var merged []int // keys which have been merged into another
	sch1:
		for aki, ak := range c.schK {
			ar := c.sch[ak]

			// get the non-excluded dates
			var arDates []fusiongo.Date
			ar.Iter(func(d fusiongo.Date, _ bool, i int) {
				if i != -1 {
					arDates = append(arDates, d)
				}
			})

			// find other keys we can merge ak into
		sch2:
			for aki2, ak2 := range c.schK {
				if aki == aki2 || slices.Contains(merged, aki2) {
					continue
				}
				ar2 := c.sch[ak2]

				// ensure the key is the is the same other than the start time
				if ak.ActivityID != ak2.ActivityID || ak.Location != ak2.Location {
					continue
				}

				// ensure that the weekdays are a superset (this is checked later too, but checking it first is faster)
				for i, c := range ar.Weekday {
					if ar2.Weekday[i] && !c {
						continue sch2
					}
				}

				// find the previous recurrence before the first
				ar2DateBefore := ar2.Instances[0].Date.AddDays(-1)
				for !ar2.Weekday[ar2DateBefore.Weekday()] {
					ar2DateBefore = ar2DateBefore.AddDays(-1)
				}

				// find the next recurrence after the last
				ar2DateAfter := ar2.Instances[len(ar2.Instances)-1].Date.AddDays(1)
				for !ar2.Weekday[ar2DateAfter.Weekday()] {
					ar2DateAfter = ar2DateAfter.AddDays(-1)
				}

				// get the non-excluded dates
				var ar2Dates []fusiongo.Date
				ar2.Iter(func(d fusiongo.Date, _ bool, i int) {
					if i != -1 {
						ar2Dates = append(ar2Dates, d)
					}
				})

				// ensure the dates which actually have instances are distinct
				for _, d := range arDates {
					if slices.Contains(ar2Dates, d) {
						continue sch2
					}
				}

				// get the potential recurrence dates according to the rule
				var ar2Recurrences []fusiongo.Date
				ar2Recurrences = append(ar2Recurrences, ar2DateBefore)
				ar2.Iter(func(d fusiongo.Date, _ bool, _ int) {
					ar2Recurrences = append(ar2Recurrences, d)
				})
				ar2Recurrences = append(ar2Recurrences, ar2DateAfter)

				// ensure it is a superset of recurrence dates
				for _, d := range arDates {
					if !slices.Contains(ar2Recurrences, d) {
						continue sch2
					}
				}

				// ensure all times we're merging overlap with at least one from the ones we're merging into
				// note: this is to prevent merging completely different times as exceptions (since it makes it the recurrence somewhat misleading) just because it fits within the exclusions, e.g., if it's from 6:30-9:30 on mondays on most weeks, but repeatedly 19:30-21:30 on days it isn't at 6:30-9:30
				// note: see testdata/20231015/school20 for a bunch of instances like this
				for _, ai := range ar.Instances {
					if !slices.ContainsFunc(ar2.Instances, func(ai2 activityInstance) bool {
						return ai.StartTime.WithEnd(ai.EndTime).TimeOverlaps(ai2.StartTime.WithEnd(ai2.EndTime))
					}) {
						slog.Debug(fmt.Sprintf("don't merge %s (n=%d) <- %s (n=%d)", ak2, len(ar2.Instances), ak, len(ar.Instances)))
						continue sch2
					}
				}

				// merge the instances
				ar2.Instances = append(ar2.Instances, ar.Instances...)
				c.sch[ak2] = ar2

				// deterministically sort the instances again
				slices.SortStableFunc(ar2.Instances, func(a, b activityInstance) int {
					return a.Date.Compare(b.Date)
				})

				// update the start date
				ar2.Base.Date = ar2.Instances[0].Date
				c.sch[ak2] = ar2

				// sanity check
				{
					tmp := slices.Clone(arDates)
					ar2.Iter(func(d fusiongo.Date, ex bool, _ int) {
						if i := slices.Index(tmp, d); i != -1 {
							if !ex {
								panic("wtf: merged activity dates aren't exceptions")
							}
							tmp = slices.Delete(tmp, i, i+1)
						}
					})
					if len(tmp) != 0 {
						panic("wtf: merged activity doesn't include all dates")
					}
				}
				slog.Debug(fmt.Sprintf("merge %s (n=%d) <- %s (n=%d)", ak2, len(ar2.Instances), ak, len(ar.Instances)))

				// we've merged it
				merged = append(merged, aki)
				continue sch1
			}
		}

		// delete the merged keys
		for i := len(merged) - 1; i >= 0; i-- {
			x := merged[i]
			k := c.schK[x]
			c.schK = slices.Delete(c.schK, x, x+1)
			delete(c.sch, k)
		}

		// update the key start times to the most common to reduce the number of exceptions
		// note: we could also eliminate the need for this by building a list of merge candidates, weighting graph edges by the number of exceptions, then minimizing the graph
		for aki, ak := range c.schK {
			ar := c.sch[ak]
			if t := mostCommonBy(ar.Instances, func(ai activityInstance) fusiongo.Time {
				return ai.StartTime
			}); ak.StartTime != t {
				delete(c.sch, ak)
				ak.StartTime = t
				ar.Base.StartTime = t
				c.schK[aki] = ak
				c.sch[ak] = ar
			}
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
				case ai.StartTime != aib.StartTime:
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
			ak.StartTime.Hour, ak.StartTime.Minute, ak.StartTime.Second, // yes, we use the activityKey start time
			c.id,
		)

		// describe recurrence information if enabled
		var excDesc string
		if o.DescribeRecurrence {
			if v := c.describeRecurrence(ak, schFilter); v != "" {
				excDesc = "\n\n" + v
			}
		}

		// write events
		writeEvent := func(ai activityInstance, base bool) {
			if ai.IsCancelled {
				ai.Activity = "CANCELLED - " + ai.Activity
			}

			// write event info
			b = icalAppendPropRaw(b, "BEGIN", "VEVENT")
			b = icalAppendPropRaw(b, "UID", uid)
			b = icalAppendPropDateTimeUTC(b, "DTSTAMP", fusiongo.GoDateTime(c.schT.UTC())) // this should be when the event was created, but unfortunately, we can't determine that deterministically, so just use the schedule update time
			b = icalAppendPropText(b, "SUMMARY", ai.Activity)

			// write event status information if it's the only event or a recurrence exception
			if !ar.Recur() || !base {
				if ai.IsCancelled && !o.FakeCancelled {
					b = icalAppendPropRaw(b, "STATUS", "CANCELLED")
				}
			}

			// write more event info
			b = icalAppendPropText(b, "LOCATION", ak.Location)
			b = icalAppendPropText(b, "DESCRIPTION", ai.Description+excDesc)
			b = icalAppendPropDateTimeLocal(b, "DTSTART", ai.StartTime.WithDate(ai.Date), c.tz)
			b = icalAppendPropDateTimeLocal(b, "DTEND", ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End(), c.tz)
			// note: not CATEGORIES since it isn't supported by most applications, and it breaks the Outlook Web App (causing the filter list to be empty) as of 2023-09-23

			// write custom props
			b = icalAppendPropText(b, "X-FUSION-ACTIVITY-ID", ak.ActivityID)
			for i := range ai.Categories {
				b = icalAppendPropText(b, "X-FUSION-CATEGORY;X-FUSION-CATEGORY-ID="+ai.CategoryIDs[i], ai.Categories[i])
			}

			// write recurrence info if it's a recurring event
			// note: we use the activityKey start time
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
				if ai.IsCancelled {
					ai.Description = "CANCELLED - " + ai.Activity
				}
				if i == -2 || ai.Activity != ar.Base.Activity {
					b = jsonStr(jsonKey(b, "activity"), ai.Activity)
				}
				if i == -2 || ai.Description != ar.Base.Description {
					b = jsonStr(jsonKey(b, "description"), ai.Description)
				}
				if i == -2 || ai.StartTime != ar.Base.StartTime {
					b = jsonStr(jsonKey(b, "startTime"), ai.StartTime.StringCompact())
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
				b = jsonStr(jsonKey(b, "_start"), ai.StartTime.WithDate(ai.Date).String())
				b = jsonTime(jsonKey(b, "_start_at"), ai.StartTime.WithDate(ai.Date).In(c.tz))
				b = jsonStr(jsonKey(b, "_end"), ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().String())
				b = jsonTime(jsonKey(b, "_end_at"), ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz))
				b = jsonStr(jsonKey(b, "_duration"), ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).Sub(ai.StartTime.WithDate(ai.Date).In(c.tz)).Truncate(time.Second).String())
				b = jsonInt(jsonKey(b, "_duration_secs"), int(ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).Sub(ai.StartTime.WithDate(ai.Date).In(c.tz)).Truncate(time.Second).Seconds()))
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
					if ai.IsCancelled {
						ai.Activity = "CANCELLED - " + ai.Activity
					}
					if !ai.IsCancelled || !o.DeleteCancelled {
						b = jsonObject(b, '{')
						b = jsonStr(jsonKey(b, "title"), ai.Activity)
						b = jsonInt(jsonKey(b, "start"), ai.StartTime.WithDate(ai.Date).In(c.tz).UnixMilli())
						b = jsonInt(jsonKey(b, "end"), ai.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End().In(c.tz).UnixMilli())
						b = jsonBool(jsonKey(b, "allDay"), false)
						b = jsonObject(jsonKey(b, "extendedProps"), '{')
						b = jsonStr(jsonKey(b, "description"), ai.Description+rd) // same as icalendar plugin
						b = jsonStr(jsonKey(b, "location"), ak.Location)          // same as icalendar plugin
						b = jsonBool(jsonKey(b, "isRecurrence"), ar.Recur())      // custom
						b = jsonBool(jsonKey(b, "isException"), ex)               // custom
						if !o.FakeCancelled && ai.IsCancelled {
							b = jsonBool(jsonKey(b, "isCancelled"), ai.IsCancelled) // custom
						}
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
					case ai.StartTime != ar.Base.StartTime || ai.EndTime != ar.Base.EndTime:
						s := ai.StartTime != ar.Base.StartTime
						e := ai.EndTime != ar.Base.EndTime
						switch {
						case s && e:
							diff = "starts at " + ai.StartTime.StringCompact() + ", ends at " + ai.EndTime.StringCompact()
						case s:
							diff = "starts at " + ai.StartTime.StringCompact()
						case e:
							diff = "ends at " + ai.EndTime.StringCompact()
						}
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

func cloneSchedule(s *fusiongo.Schedule) *fusiongo.Schedule {
	ns := &fusiongo.Schedule{
		Updated:    s.Updated,
		Activities: slices.Clone(s.Activities),
	}
	for i := range s.Activities {
		ns.Activities[i].Category = slices.Clone(ns.Activities[i].Category)
	}
	return ns
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
