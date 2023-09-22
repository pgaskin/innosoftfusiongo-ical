package main

import (
	"bytes"
	"context"
	"crypto/sha1"
	_ "embed"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pgaskin/innosoftfusiongo-ical/fusiongo"
)

const EnvPrefix = "IFGICAL"

var (
	Addr              = flag.String("addr", ":8080", "Listen address")
	LogLevel          = flag_Level("log-level", 0, "Log level (debug/info/warn/error)")
	LogJSON           = flag.Bool("log-json", false, "Output logs as JSON")
	CacheTime         = flag.Duration("cache-time", time.Minute*5, "Time to cache Innosoft Fusion Go data for")
	StaleTime         = flag.Duration("stale-time", time.Hour, "Amount of time after cache-time to continue using stale data for if the update fails")
	Timeout           = flag.Duration("timeout", time.Second*5, "Timeout for fetching Innosoft Fusion Go data")
	ProxyHeader       = flag.String("proxy-header", "", "Trusted header containing the remote address (e.g., X-Forwarded-For)")
	InstanceWhitelist = flag.String("instance-whitelist", "", "Comma-separated whitelist of Innosoft Fusion Go instances to get data from")
	Testdata          = flag.String("testdata", "", "Path to directory containing school%d/*.json files to test with")
)

func flag_Level(name string, value slog.Level, usage string) *slog.Level {
	v := new(slog.Level)
	flag.TextVar(v, name, value, usage)
	return v
}

func main() {
	// parse config
	flag.CommandLine.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: %s [options]\n", flag.CommandLine.Name())
		fmt.Fprintf(flag.CommandLine.Output(), "\noptions:\n")
		flag.CommandLine.PrintDefaults()
		fmt.Fprintf(flag.CommandLine.Output(), "\nnote: all options can be specified as environment variables with the prefix %q and dashes replaced with underscores\n", EnvPrefix)
	}
	for _, e := range os.Environ() {
		if e, ok := strings.CutPrefix(e, EnvPrefix+"_"); ok {
			if k, v, ok := strings.Cut(e, "="); ok {
				if err := flag.CommandLine.Set(strings.ReplaceAll(strings.ToLower(k), "_", "-"), v); err != nil {
					fmt.Fprintf(flag.CommandLine.Output(), "env %s: %v\n", k, err)
					flag.CommandLine.Usage()
					os.Exit(2)
				}
			}
		}
	}
	if flag.Parse(); flag.NArg() != 0 {
		fmt.Fprintf(flag.CommandLine.Output(), "extra arguments %q provided\n", flag.Args())
		flag.CommandLine.Usage()
		os.Exit(2)
	}

	// setup slog if required
	var logOptions *slog.HandlerOptions
	if *LogLevel != 0 {
		logOptions = &slog.HandlerOptions{
			Level: *LogLevel,
		}
	}
	if *LogJSON {
		slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, logOptions)))
	} else if logOptions != nil {
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, logOptions)))
	}

	// setup http server
	srv := &http.Server{
		Addr:    *Addr,
		Handler: http.HandlerFunc(handle),
	}
	if *ProxyHeader != "" {
		next := srv.Handler
		srv.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if x, _, _ := strings.Cut(r.Header.Get(*ProxyHeader), ","); x != "" {
				r1 := *r
				r = &r1
				if xap, err := netip.ParseAddrPort(x); err == nil {
					// valid ip/port; keep the entire thing
					r.RemoteAddr = xap.String()
				} else if xa, err := netip.ParseAddr(x); err == nil {
					// only an ip; keep the existing port if possible
					eap, _ := netip.ParseAddrPort(r.RemoteAddr)
					r.RemoteAddr = netip.AddrPortFrom(xa, eap.Port()).String()
				} else {
					// invalid
					slog.Warn("failed to parse proxy remote ip header", "header", *ProxyHeader, "value", x)
				}
			}
			next.ServeHTTP(w, r)
		})
	}
	if l, err := net.Listen("tcp", srv.Addr); err != nil {
		slog.Error("listen", "error", err)
		os.Exit(1)
	} else {
		go srv.Serve(l)
	}

	// ready; stop on ^C
	slog.Info("started server", "addr", srv.Addr)

	ctx, done := signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()
	<-ctx.Done()

	// stop; force-stop on ^C
	slog.Info("stopping")

	ctx, done = signal.NotifyContext(context.Background(), os.Interrupt)
	defer done()

	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("failed to stop server gracefully", "error", err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimLeft(path.Clean(r.URL.Path), "/")

	if p == "favicon.ico" {
		handleFavicon(w, r)
		return
	}

	if !strings.ContainsRune(p, '/') || !strings.HasPrefix(p, ".") {
		if instance, ext, _ := strings.Cut(p, "."); instance != "" {
			ext = strings.ToLower(ext)
			if instance, _ := strings.CutPrefix(instance, "school"); instance != "" {
				if schoolID, _ := strconv.ParseInt(instance, 10, 64); schoolID != 0 {
					if whitelist := *InstanceWhitelist; whitelist != "" {
						var (
							match           bool
							more            bool
							instanceCurrent string
							instanceShort   = strconv.FormatInt(schoolID, 10)
							instanceLong    = "school" + instanceShort
						)
						for {
							instanceCurrent, whitelist, more = strings.Cut(whitelist, ",")
							instanceCurrent = strings.TrimSpace(instanceCurrent)
							if instanceCurrent == instanceShort || instanceCurrent == instanceLong {
								match = true
								break
							}
							if !more {
								break
							}
						}
						if !match {
							http.Error(w, fmt.Sprintf("Instance %q not on whitelist", instance), http.StatusForbidden)
							return
						}
					}
					if ext != "ics" {
						if ua := r.Header.Get("User-Agent"); false ||
							strings.HasPrefix(ua, "Google-Calendar-Importer") || // Google Calendar
							strings.HasPrefix(ua, "Microsoft.Exchange/") || // OWA
							strings.Contains(ua, "CalendarAgent/") || // macOS Calendar
							strings.Contains(ua, "dataaccessd/") || // iOS Calendar
							strings.HasPrefix(ua, "ICSx5/") || // ICSx5 Android
							strings.HasPrefix(r.Header.Get("Accept"), "text/calendar") || // misc
							false {
							w.Header().Set("Cache-Control", "private, no-cache, no-store")
							w.Header().Set("Pragma", "no-cache")
							w.Header().Set("Expires", "0")
							http.Redirect(w, r, (&url.URL{
								Path:     "/" + strconv.FormatInt(schoolID, 10) + ".ics",
								RawQuery: r.URL.RawQuery,
							}).String(), http.StatusFound)
							return
						}
					}
					if ext == "" {
						if r.Header.Get("Sec-Fetch-Dest") == "document" || strings.HasPrefix(r.Header.Get("User-Agent"), "Mozilla/") {
							ext = "html"
						} else {
							ext = "ics"
						}
					}
					switch ext {
					case "html":
						handleWeb(w, r, int(schoolID))
						return
					case "ics":
						handleCalendar(w, r, int(schoolID))
						return
					}
					http.Error(w, fmt.Sprintf("No handler for extension %q", ext), http.StatusNotFound)
					return
				}
			}
			http.Error(w, fmt.Sprintf("Invalid instance %q", instance), http.StatusBadRequest)
			return
		}
	}

	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

//go:embed favicon.ico
var favicon []byte

func handleFavicon(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Content-Length", strconv.Itoa(len(favicon)))
	w.WriteHeader(http.StatusOK)
	w.Write(favicon)
}

//go:embed calendar.html
var calendarHTML []byte

func handleWeb(w http.ResponseWriter, _ *http.Request, _ int) {
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(calendarHTML)))
	w.WriteHeader(http.StatusOK)
	w.Write(calendarHTML)
}

type cacheEntry struct {
	Mu sync.Mutex

	// error info (will be cleared on success)
	Failure time.Time // when the error happened
	Error   error     // error details
	ErrorN  int       // error count

	// success info (will be cleared on invalidation)
	Success  time.Time            // last time the calendar data was successfully updated
	Calendar generateCalendarFunc // ics generate function
}

var cache sync.Map // [schoolID int]*cacheEntry

// backoff returns the minimum time to try again for attempt n with the last
// error at t.
func backoff(t time.Time, n int) time.Time {
	if n <= 0 {
		return t
	}
	switch n {
	case 1, 2:
		return t.Add(time.Second)
	case 3, 4, 5, 6:
		return t.Add(time.Second * 15)
	case 7, 8, 9, 10:
		return t.Add(time.Minute)
	default:
		return t.Add(time.Minute * 15)
	}
}

// getCalendarCached gets the latest generate function for schoolID. Even if it
// returns an error, it may still return an old generateCalendarFunc if it isn't
// too stale.
func getCalendarCached(schoolID int) (generateCalendarFunc, error) {
	entryV, _ := cache.LoadOrStore(schoolID, new(cacheEntry))
	entry := entryV.(*cacheEntry)

	entry.Mu.Lock()
	defer entry.Mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), *Timeout)
	defer cancel()

	if entry.Success.IsZero() || time.Since(entry.Success) > *CacheTime {
		if entry.Success.IsZero() {
			slog.Info("fusion data missing or stale; fetching", "instance", schoolID, "cache_time", *CacheTime, "attempt", entry.ErrorN)
		} else {
			slog.Info("fusion data missing or stale; fetching", "instance", schoolID, "last_update", entry.Success, "cache_time", *CacheTime, "attempt", entry.ErrorN)
		}

		if bt := backoff(entry.Failure, entry.ErrorN); !bt.IsZero() && !bt.Before(time.Now()) {
			slog.Warn("not attempting update due to backoff after last error", "instance", schoolID, "error", entry.Error, "error_time", entry.Failure, "attempt", entry.ErrorN, "backoff_until", bt)
		} else {
			update := time.Now()
			if calendar, err := prepareCalendar(ctx, schoolID); err == nil {
				entry.Failure = time.Time{}
				entry.Error = nil
				entry.ErrorN = 0

				entry.Success = update
				entry.Calendar = calendar

				slog.Info("successfully updated fusion data", "instance", schoolID, "update_time", update, "update_duration", time.Since(update))
			} else {
				entry.Failure = update
				entry.Error = err
				entry.ErrorN++

				if time.Since(entry.Success) > *StaleTime {
					entry.Success = time.Time{}
					entry.Calendar = nil
				}

				if !entry.Success.IsZero() {
					slog.Warn("failed to update fusion data",
						"instance", schoolID, "update_time", update, "update_duration", time.Since(update),
						"error", entry.Error, "attempt", entry.ErrorN,
						"using_old_data", true, "old_data_age", time.Since(entry.Success),
					)
				} else {
					slog.Warn("failed to update fusion data",
						"instance", schoolID, "update_time", update, "update_duration", time.Since(update),
						"error", entry.Error, "attempt", entry.ErrorN,
						"using_old_data", false,
					)
				}
			}
		}
	}

	if !entry.Failure.IsZero() {
		return entry.Calendar, fmt.Errorf("failed to fetch calendar data (attempt %d, %s ago, retry in %s): %w", entry.ErrorN, time.Since(entry.Failure).Truncate(time.Second), time.Until(backoff(entry.Failure, entry.ErrorN)).Truncate(time.Second), entry.Error)
	}
	return entry.Calendar, nil
}

func handleCalendar(w http.ResponseWriter, r *http.Request, schoolID int) {
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	if cur, lim := len(r.URL.RawQuery), 512; cur > lim {
		slog.Info("rejected request with long query string", "remote", r.RemoteAddr, "instance", schoolID)
		http.Error(w, fmt.Sprintf("Request query too long (%d > %d)", cur, lim), http.StatusRequestURITooLong)
		return
	}

	generateCalendar, err := getCalendarCached(schoolID)
	if err != nil {
		if generateCalendar == nil {
			http.Error(w, fmt.Sprintf("Failed to fetch data: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Refresh-Error", err.Error())
	}
	if generateCalendar == nil {
		http.Error(w, "No calendar data available", http.StatusInternalServerError)
		return
	}

	generateStart := time.Now()
	q := r.URL.Query()
	buf := generateCalendar(&generateCalendarOptions{
		Category:           queryFilter(q, "category", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		CategoryID:         queryFilter(q, "category_id", filterOpts{Negation: true, Wildcard: false, CaseFold: true, Collapse: true}),
		Activity:           queryFilter(q, "activity", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		ActivityID:         queryFilter(q, "activity_id", filterOpts{Negation: true, Wildcard: false, CaseFold: true, Collapse: true}),
		Location:           queryFilter(q, "location", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		NoNotifications:    queryBool(q, "no_notifications"),
		FakeCancelled:      queryBool(q, "fake_cancelled"),
		DeleteCancelled:    queryBool(q, "delete_cancelled"),
		DescribeRecurrence: queryBool(q, "describe_recurrence"),
	})
	w.Header().Set("X-Generate-Time", strconv.FormatFloat(time.Since(generateStart).Seconds(), 'f', -1, 64))

	if r.Header.Get("Sec-Fetch-Dest") != "document" {
		w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.WriteHeader(http.StatusOK)
	w.Write(buf)
}

// queryBool returns true if q[k] is empty and the last value is empty, k, or
// some form of true (case-insensitive).
func queryBool(q url.Values, k string) bool {
	if v, ok := q[k]; ok {
		if len(v) > 0 {
			if last := strings.TrimSpace(v[len(v)-1]); last != "" && !strings.EqualFold(last, k) {
				if t, _ := strconv.ParseBool(v[len(v)-1]); !t {
					return false
				}
			}
		}
		return true
	}
	return false
}

// queryFilter parses filters from instances of k or k[].
func queryFilter(q url.Values, k string, o filterOpts) filter {
	var v []string
	if x := q[k]; len(x) != 0 {
		v = append(v, x...)
	}
	if x := q[k+"[]"]; len(x) != 0 {
		v = append(v, x...)
	}
	return makeFilter(v, o)
}

type generateCalendarOptions struct {
	Category   filter
	CategoryID filter
	Activity   filter
	ActivityID filter
	Location   filter

	// Don't include notifications in the calendar.
	NoNotifications bool

	// Don't set the cancellation status on cancelled events (e.g., if you want outlook mobile to still show the events).
	FakeCancelled bool

	// Entirely exclude cancelled events.
	DeleteCancelled bool

	// List recurrence info and exceptions in event description.
	DescribeRecurrence bool
}

type generateCalendarFunc func(*generateCalendarOptions) []byte

func prepareCalendar(ctx context.Context, schoolID int) (generateCalendarFunc, error) {

	// load schedule
	var schedule *fusiongo.Schedule
	if *Testdata != "" {
		if buf, err := os.ReadFile(filepath.Join(*Testdata, "school"+strconv.Itoa(schoolID), "schedule.json")); err != nil {
			return nil, fmt.Errorf("load schedule: %w", err)
		} else if v, err := fusiongo.ParseSchedule(buf); err != nil {
			return nil, fmt.Errorf("load schedule: %w", err)
		} else {
			schedule = v
		}
	} else if v, err := fusiongo.FetchSchedule(ctx, schoolID); err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	} else {
		schedule = v
	}

	// load notifications
	var notifications *fusiongo.Notifications
	if *Testdata != "" {
		if buf, err := os.ReadFile(filepath.Join(*Testdata, "school"+strconv.Itoa(schoolID), "notifications.json")); err != nil {
			return nil, fmt.Errorf("load notifications: %w", err)
		} else if v, err := fusiongo.ParseNotifications(buf); err != nil {
			return nil, fmt.Errorf("load notifications: %w", err)
		} else {
			notifications = v
		}
	} else if v, err := fusiongo.FetchNotifications(ctx, schoolID); err != nil {
		return nil, fmt.Errorf("load notifications: %w", err)
	} else {
		notifications = v
	}

	// do some cleanup
	for ai, a := range schedule.Activities {
		schedule.Activities[ai].Description = strings.TrimSpace(a.Description)
	}
	for ni, n := range notifications.Notifications {
		notifications.Notifications[ni].Text = strings.TrimSpace(n.Text)
	}

	// check some basic assumptions
	// - we depend (for correctness, not code) on the fact that an activity is uniquely identified by its id and location and can only occur once per start time per day
	// - we also depend on each activity having at least one category (this should always be true)
	activityInstancesCheck := map[string]fusiongo.ActivityInstance{}
	for ai, a := range schedule.Activities {
		iid := fmt.Sprint(a.ActivityID, a.Location, a.Time.Date, a.Time.TimeRange.Start)
		if x, ok := activityInstancesCheck[iid]; !ok {
			activityInstancesCheck[iid] = a
		} else if !reflect.DeepEqual(a, x) {
			slog.Warn("activity instance is not uniquely identified by (id, location, date, start)", "activity1", a, "activity2", x)
		}
		if len(schedule.Categories[ai].Category) == 0 {
			panic("wtf: expected activity instance to have at least one category")
		}
	}

	type ActivityKey struct {
		ActivityID string
		Location   string
		StartTime  fusiongo.Time
		Weekday    [7]bool
	}

	type ActivityInstance struct {
		IsCancelled bool
		Activity    string
		Description string
		Date        fusiongo.Date
		EndTime     fusiongo.Time
		Categories  []string
		CategoryIDs []string
	}

	// collect activity instance event info and group into recurrence groups
	activities := mapGroupByInto(schedule.Activities, func(ai int, a fusiongo.ActivityInstance) (ActivityKey, ActivityInstance) {
		activityKey := ActivityKey{
			ActivityID: a.ActivityID,
			StartTime:  a.Time.TimeRange.Start,
			Location:   a.Location,
		}
		activityInstance := ActivityInstance{
			IsCancelled: a.IsCancelled,
			Activity:    a.Activity,
			Description: a.Description,
			Date:        a.Time.Date,
			EndTime:     a.Time.TimeRange.End,
			Categories:  schedule.Categories[ai].Category,
			CategoryIDs: schedule.Categories[ai].CategoryID,
		}
		return activityKey, activityInstance
	})

	// split out days which consistently have different end times
	{
		activitiesNew := map[ActivityKey][]ActivityInstance{}
		for activityKey, activityInstances := range activities {

			// group the activities by their weekday
			actByWeekday := weekdayGroupBy(activityInstances, func(i int, ai ActivityInstance) time.Weekday {
				return ai.Date.WithTime(activityKey.StartTime).Weekday()
			})

			// get the most common end time for each weekday
			etByWeekday := weekdayMapInto(actByWeekday, func(w time.Weekday, ais []ActivityInstance) fusiongo.Time {
				return mostCommonBy(ais, func(activityInstance ActivityInstance) fusiongo.Time {
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
				activitiesNew[activityKeyNew] = flatFilter(actByWeekday[:], func(i int) bool {
					return wds[i]
				})
			}
		}
		activities = activitiesNew
	}

	// deterministically sort the map
	activityKeys := make([]ActivityKey, 0, len(activities))
	for activityKey, activityInstances := range activities {
		sort.SliceStable(activityInstances, func(i, j int) bool {
			return activityInstances[i].Date.Less(activityInstances[j].Date)
		})
		activityKeys = append(activityKeys, activityKey)
	}
	sort.SliceStable(activityKeys, func(i, j int) bool {
		if activityKeys[i].ActivityID == activityKeys[j].ActivityID {
			if activityKeys[i].StartTime == activityKeys[j].StartTime {
				if activityKeys[i].Location == activityKeys[j].Location {
					return fmt.Sprint(activityKeys[i].Weekday) < fmt.Sprint(activityKeys[j].Weekday) // HACK
				}
				return activityKeys[i].Location < activityKeys[j].Location
			}
			return activityKeys[i].StartTime.Less(activityKeys[j].StartTime)
		}
		return activityKeys[i].ActivityID < activityKeys[j].ActivityID
	})

	return func(o *generateCalendarOptions) []byte {
		// pre-filter activities
		activityFilter := map[ActivityKey][]bool{}
		for _, ak := range activityKeys {
			if !o.Location.Match(ak.Location) {
				continue
			}
			if !o.ActivityID.Match(ak.ActivityID) {
				continue
			}
			for i, ai := range activities[ak] {
				if o.DeleteCancelled && ai.IsCancelled {
					continue
				}
				if !o.Activity.Match(ai.Activity) {
					continue
				}
				if !o.Category.Match(ai.Categories...) {
					continue
				}
				if !o.CategoryID.Match(ai.CategoryIDs...) {
					continue
				}
				if _, ok := activityFilter[ak]; !ok {
					activityFilter[ak] = make([]bool, len(activities[ak]))
				}
				activityFilter[ak][i] = true
			}
		}

		var calName, calColor string
		switch schoolID {
		case 110:
			calName = "ARC Schedule"
			calColor = "firebrick" // note: closest CSS extended color keyword to red #b90e31 (=b22222 firebrick) blue #002452 (=000080 navy)
		default:
			calName = "Schedule (" + strconv.Itoa(schoolID) + ")"
		}

		var ical bytes.Buffer

		// write calendar info
		fmt.Fprintf(&ical, "BEGIN:VCALENDAR\r\n")
		fmt.Fprintf(&ical, "VERSION:2.0\r\n")
		fmt.Fprintf(&ical, "PRODID:arcical\r\n")
		fmt.Fprintf(&ical, "NAME:%s\r\n", icalTextEscape(calName))
		fmt.Fprintf(&ical, "X-WR-CALNAME:%s\r\n", icalTextEscape(calName))
		fmt.Fprintf(&ical, "REFRESH-INTERVAL;VALUE=DURATION:PT60M\r\n")
		fmt.Fprintf(&ical, "X-PUBLISHED-TTL:PT60M\r\n")
		fmt.Fprintf(&ical, "LAST-MODIFIED:%s\r\n", icalDateTimeUTC(fusiongo.GoDateTime(time.Now().UTC())))
		if calColor != "" {
			fmt.Fprintf(&ical, "COLOR:%s\r\n", calColor)
		}

		// write activities
		for _, ak := range activityKeys {
			if _, include := activityFilter[ak]; !include {
				continue
			}
			ais := activities[ak]

			// https://www.nylas.com/blog/calendar-events-rrules/

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
				schoolID,
			)
			if len(uid) >= 255 {
				panic("wtf: generated uid too long")
			}

			// base and last instances, recurrence (ignoring filters)
			var (
				aiBase = ais[0]
				aiLast = ais[len(ais)-1]
				recur  func(fn func(d fusiongo.Date, ex bool, i int)) // iterates over recurrence dates, using (d, true, -1) for exclusions, and setting ex if aiBase != ais[ai]
			)
			if len(ais) > 1 {

				// set the fields to the first (for determinism) most common values
				aiBase.EndTime = mostCommonBy(ais, func(ai ActivityInstance) fusiongo.Time {
					return ai.EndTime
				})
				aiBase.Activity = mostCommonBy(ais, func(ai ActivityInstance) string {
					return ai.Activity
				})
				aiBase.Description = mostCommonBy(ais, func(ai ActivityInstance) string {
					return ai.Description
				})
				aiBase.IsCancelled = false // cancellation should only be set on exceptions

				// set the recurrence function
				recur = func(fn func(d fusiongo.Date, ex bool, i int)) {
					for d := aiBase.Date; !aiLast.Date.Less(d); d = d.AddDays(1) {
						if ak.Weekday[d.Weekday()] {
							if i := slices.IndexFunc(ais, func(activityInstance ActivityInstance) bool {
								return activityInstance.Date == d
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
						} else if !activityFilter[ak][i] {
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
			writeEvent := func(ai ActivityInstance, base bool) {
				// write event info
				fmt.Fprintf(&ical, "BEGIN:VEVENT\r\n")
				fmt.Fprintf(&ical, "UID:%s\r\n", uid)
				fmt.Fprintf(&ical, "DTSTAMP:%s\r\n", icalDateTimeUTC(fusiongo.GoDateTime(schedule.Updated.UTC()))) // utc; this should be when the event was created, but unfortunately, we can'te determine that determinstically, so just use the schedule update time

				// write event status information if it's the only event or a recurrence exception
				if recur == nil || !base {
					if !ai.IsCancelled {
						fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape(ai.Activity))
					} else {
						fmt.Fprintf(&ical, "SUMMARY:CANCELLED - %s\r\n", icalTextEscape(ai.Activity))
						if !o.FakeCancelled {
							fmt.Fprintf(&ical, "STATUS:CANCELLED\r\n")
						}
					}
				} else {
					fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape(ai.Activity))
				}

				// write more event info
				fmt.Fprintf(&ical, "LOCATION:%s\r\n", icalTextEscape(ak.Location))
				fmt.Fprintf(&ical, "DESCRIPTION:%s\r\n", icalTextEscape(ai.Description+excDesc.String()))
				fmt.Fprintf(&ical, "DTSTART:%s\r\n", icalDateTimeLocal(ak.StartTime.WithDate(ai.Date)))                         // local
				fmt.Fprintf(&ical, "DTEND:%s\r\n", icalDateTimeLocal(ak.StartTime.WithEnd(ai.EndTime).WithDate(ai.Date).End())) // local

				// write custom props
				fmt.Fprintf(&ical, "X-FUSION-ACTIVITY-ID:%s\r\n", icalTextEscape(ak.ActivityID))
				for i := range ai.Categories {
					fmt.Fprintf(&ical, "X-FUSION-CATEGORY;X-FUSION-CATEGORY-ID=%s:%s\r\n", ai.CategoryIDs[i], icalTextEscape(ai.Categories[i]))
				}

				// write recurrence info if it's a recurring event
				if recur != nil {
					if base {
						fmt.Fprintf(&ical, "RRULE:FREQ=WEEKLY;INTERVAL=1;UNTIL=%s;BYDAY=%s\r\n", icalDateTimeLocal(ak.StartTime.WithEnd(aiLast.EndTime).WithDate(aiLast.Date).End()), strings.Join(akDays, ",")) // local if dtstart is local, else utc
						recur(func(d fusiongo.Date, _ bool, i int) {
							if i == -1 || !activityFilter[ak][i] {
								fmt.Fprintf(&ical, "EXDATE:%s\r\n", icalDateTimeLocal(ak.StartTime.WithDate(d)))
							}
						})
					} else {
						fmt.Fprintf(&ical, "RECURRENCE-ID:%s\r\n", icalDateTimeLocal(ak.StartTime.WithDate(ai.Date))) // local
					}
				}

				// done
				fmt.Fprintf(&ical, "END:VEVENT\r\n")
			}

			// write the base event
			writeEvent(aiBase, true)

			// write recurrence exceptions
			if recur != nil {
				recur(func(_ fusiongo.Date, ex bool, i int) {
					if ex {
						if i != -1 && activityFilter[ak][i] {
							writeEvent(ais[i], false)
						}
					}
				})
			}
		}

		// write notifications as all-day events for the current day
		if !o.NoNotifications {
			for _, n := range notifications.Notifications {
				// https://stackoverflow.com/questions/1716237/single-day-all-day-appointments-in-ics-files

				uid := fmt.Sprintf("notification-%s@school%d.innosoftfusiongo.com", n.ID, schoolID)
				if len(uid) >= 255 {
					panic("wtf: generated uid too long")
				}
				fmt.Fprintf(&ical, "BEGIN:VEVENT\r\n")
				fmt.Fprintf(&ical, "UID:%s\r\n", uid)
				fmt.Fprintf(&ical, "SEQUENCE:1\r\n")
				fmt.Fprintf(&ical, "DTSTAMP:%s\r\n", icalDateTimeUTC(fusiongo.GoDateTime(notifications.Updated.UTC()))) // utc
				fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape(n.Text))
				fmt.Fprintf(&ical, "DESCRIPTION:%s\r\n", icalTextEscape(n.Text))
				fmt.Fprintf(&ical, "DTSTART;VALUE=DATE:%s\r\n", icalDate(n.Sent.Date)) // local
				fmt.Fprintf(&ical, "END:VEVENT\r\n")
			}
		}

		// done
		fmt.Fprintf(&ical, "END:VCALENDAR\r\n")
		return ical.Bytes()
	}, nil
}

type filterOpts struct {
	Negation bool // support exclude patterns
	Wildcard bool // support * wildcards
	CaseFold bool // case-insensitive match
	Collapse bool // replace consecutive whitespace with a single space, trim leading/trailing
}

type filter struct {
	collapse bool
	include  []*regexp.Regexp
	exclude  []*regexp.Regexp
}

func makeFilter(ps []string, o filterOpts) filter {
	f := filter{
		collapse: o.Collapse,
	}
	for _, p := range ps {
		var negate bool
		if o.Negation {
			p, negate = strings.CutPrefix(p, "-")
		}

		var wildStart, wildEnd bool
		if o.Wildcard {
			p, wildStart = strings.CutPrefix(p, "*")
			p, wildEnd = strings.CutSuffix(p, "*")
		}

		if o.Collapse {
			p = strings.Join(strings.Fields(p), " ")
		}

		var re strings.Builder
		if o.CaseFold {
			re.WriteString("(?si)")
		} else {
			re.WriteString("(?s)")
		}
		if !wildStart {
			re.WriteByte('^')
		}
		for i, part := range strings.FieldsFunc(p, func(r rune) bool {
			return r == '*'
		}) {
			if i != 0 {
				re.WriteString(".+")
			}
			re.WriteString(regexp.QuoteMeta(part))
		}
		if !wildEnd {
			re.WriteByte('$')
		}
		if c := regexp.MustCompile(re.String()); negate {
			f.exclude = append(f.exclude, c)
		} else {
			f.include = append(f.include, c)
		}
	}
	return f
}

func (f filter) Match(ss ...string) bool {
	var match bool
	if len(f.include) == 0 {
		match = true
	}
	if !match || len(f.exclude) != 0 {
		if f.collapse {
			for i, s := range ss {
				ss[i] = strings.Join(strings.Fields(s), " ")
			}
		}
		for _, c := range f.include {
			for _, s := range ss {
				if c.MatchString(s) {
					match = true
					break
				}
			}
		}
		for _, c := range f.exclude {
			for _, s := range ss {
				if c.MatchString(s) {
					match = false
					break
				}
			}
		}
	}
	return match
}

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

// mapGroupByInto groups xs by arbitrary keys, converting the values.
func mapGroupByInto[T any, K comparable, V any](xs []T, fn func(int, T) (K, V)) map[K][]V {
	m := make(map[K][]V)
	for i, x := range xs {
		k, v := fn(i, x)
		m[k] = append(m[k], v)
	}
	return m
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

// icalTextEscape escapes a string for use as an iCalendar value.
func icalTextEscape(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i, c := range s {
		switch c {
		case '\r':
			if i+1 == len(s) || s[i+1] != '\n' {
				b.WriteRune(c)
			}
		case '\n':
			b.WriteByte('\\')
			b.WriteByte('n')
		case '\\', ';', ',':
			b.WriteByte('\\')
			b.WriteByte(byte(c))
		default:
			b.WriteRune(c)
		}
	}
	return b.String()
}

// icalDate formats d as an iCalendar date.
func icalDate(d fusiongo.Date) string {
	return fmt.Sprintf("%04d%02d%02d", d.Year, d.Month, d.Day)
}

// icalDate formats d as an iCalendar utc date-time.
func icalDateTimeUTC(dt fusiongo.DateTime) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02dZ", dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
}

// icalDate formats d as an iCalendar local date-time.
func icalDateTimeLocal(dt fusiongo.DateTime) string {
	return fmt.Sprintf("%04d%02d%02dT%02d%02d%02d", dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second)
}
