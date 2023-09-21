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
	Timeout           = flag.Duration("timeout", time.Second*5, "Timeout for fetching Innosoft Fusion Go data")
	ProxyHeader       = flag.String("proxy-header", "", "Trusted header containing the remote address (e.g., X-Forwarded-For)")
	InstanceWhitelist = flag.String("instance-whitelist", "", "Comma-separated whitelist of Innosoft Fusion Go instances to get data from")
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

	if !strings.ContainsRune(p, '/') && p != "favicon.ico" || !strings.HasPrefix(p, ".") {
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

var (
	// TODO: refactor
	cacheLock     sync.Mutex
	cacheUpdated  = map[int]time.Time{}
	cacheCalendar = map[int]generateCalendarFunc{}
)

func handleCalendar(w http.ResponseWriter, r *http.Request, schoolID int) {
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	if cur, lim := len(r.URL.RawQuery), 512; cur > lim {
		slog.Info("rejected request with long query string", "remote", r.RemoteAddr, "instance", schoolID)
		http.Error(w, fmt.Sprintf("Request query too long (%d > %d)", cur, lim), http.StatusRequestURITooLong)
		return
	}

	q := r.URL.Query()

	var opt generateCalendarOptions
	opt.Category = filterer{Negation: true, Wildcard: true, CaseFold: true, Collapse: true, Patterns: q["category"]}
	opt.CategoryID = filterer{Negation: true, Wildcard: false, CaseFold: true, Collapse: true, Patterns: q["category_id"]}
	opt.Activity = filterer{Negation: true, Wildcard: true, CaseFold: true, Collapse: true, Patterns: q["activity"]}
	opt.ActivityID = filterer{Negation: true, Wildcard: false, CaseFold: true, Collapse: true, Patterns: q["activity_id"]}
	opt.Location = filterer{Negation: true, Wildcard: true, CaseFold: true, Collapse: true, Patterns: q["location"]}
	_, opt.NoNotifications = q["no_notifications"]
	_, opt.FakeCancelled = q["fake_cancelled"]     // don't set the cancellation status on cancelled events (e.g., if you want outlook mobile to still show the events)
	_, opt.DeleteCancelled = q["delete_cancelled"] // entirely exclude cancelled events
	_, opt.DescribeRecurrence = q["describe_recurrence"]

	var generateCalendar generateCalendarFunc
	if err := func() error {
		cacheLock.Lock()
		defer cacheLock.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), *Timeout)
		defer cancel()

		if u, ok := cacheUpdated[schoolID]; !ok || time.Since(u) > *CacheTime {
			slog.Info("fusion data missing or stale; fetching", "instance", schoolID, "last_update", u, "cache_time", *CacheTime)
			u = time.Now()

			generateCalendar, err := prepareCalendar(ctx, schoolID)
			if err != nil {
				return fmt.Errorf("prepare calendar: %w", err)
			}

			cacheUpdated[schoolID] = u
			cacheCalendar[schoolID] = generateCalendar
		}

		generateCalendar = cacheCalendar[schoolID]

		return nil
	}(); err != nil {
		slog.Error("failed to fetch data", "error", err, "instance", schoolID)
		http.Error(w, fmt.Sprintf("Failed to fetch data: %v", err), http.StatusInternalServerError)
		return
	}

	buf := generateCalendar(&opt)
	if r.Header.Get("Sec-Fetch-Dest") != "document" {
		w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.WriteHeader(http.StatusOK)
	w.Write(buf)
}

type generateCalendarOptions struct {
	Category   filterer
	CategoryID filterer
	Activity   filterer
	ActivityID filterer
	Location   filterer

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
	// get the schedule timezone
	tz, err := time.LoadLocation("America/Toronto")
	if err != nil {
		return nil, fmt.Errorf("load timezone: %w", err)
	}

	// load schedule
	schedule, err := fusiongo.FetchSchedule(ctx, schoolID)
	if err != nil {
		return nil, fmt.Errorf("load schedule: %w", err)
	}

	// load notifications
	notifications, err := fusiongo.FetchNotifications(ctx, schoolID)
	if err != nil {
		return nil, fmt.Errorf("load notifications: %w", err)
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
		iid := fmt.Sprint(a.ActivityID, a.Location, a.Time.Date, a.Time.Start)
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
	activities := map[ActivityKey][]ActivityInstance{}
	for ai, a := range schedule.Activities {
		activityKey := ActivityKey{
			ActivityID: a.ActivityID,
			StartTime:  a.Time.Start,
			Location:   a.Location,
		}
		activityInstance := ActivityInstance{
			IsCancelled: a.IsCancelled,
			Activity:    a.Activity,
			Description: a.Description,
			Date:        a.Time.Date,
			EndTime:     a.Time.End,
			Categories:  schedule.Categories[ai].Category,
			CategoryIDs: schedule.Categories[ai].CategoryID,
		}
		activities[activityKey] = append(activities[activityKey], activityInstance)
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
			return activityKeys[i].StartTime.Less(activityKeys[j].StartTime)
		}
		return activityKeys[i].ActivityID < activityKeys[j].ActivityID
	})

	type ActivityBase struct {
		Instance ActivityInstance

		Recurrence map[time.Weekday]int
		Last       fusiongo.Date
	}

	// heuristically determine the recurrence info
	activityBase := make(map[ActivityKey]ActivityBase, len(activities))
	for _, activityKey := range activityKeys {
		activityInstances := activities[activityKey]

		// if there's only one instance, no recurrence
		if len(activityInstances) == 1 {
			activityBase[activityKey] = ActivityBase{
				Instance: activityInstances[0],
			}
			continue
		}

		base := ActivityBase{
			Recurrence: map[time.Weekday]int{},
		}

		// find the first and last occurrences (note: we sorted it earlier)
		base.Instance.Date = activityInstances[0].Date
		base.Last = activityInstances[len(activityInstances)-1].Date

		// figure out the recurrence pattern
		for _, activityInstance := range activityInstances {
			base.Recurrence[time.Weekday(activityInstance.Date.In(tz).Weekday())]++
		}

		// find the first most common name
		{
			names := []string{}
			nameCounts := map[string]int{}
			nameCount := 0
			for _, activityInstance := range activityInstances {
				if _, seen := nameCounts[activityInstance.Activity]; !seen {
					names = append(names, activityInstance.Activity)
				}
				nameCounts[activityInstance.Activity]++
			}
			for _, name := range names {
				if n := nameCounts[name]; n > nameCount {
					nameCount = n
					base.Instance.Activity = name
				}
			}
		}

		// find the most common end time
		{
			ends := []fusiongo.Time{}
			endCounts := map[fusiongo.Time]int{}
			endCount := 0
			for _, activityInstance := range activityInstances {
				if _, seen := endCounts[activityInstance.EndTime]; !seen {
					ends = append(ends, activityInstance.EndTime)
				}
				endCounts[activityInstance.EndTime]++
			}
			for _, end := range ends {
				if n := endCounts[end]; n > endCount {
					endCount = n
					base.Instance.EndTime = end
				}
			}
		}

		// keep the description if all are the same
		base.Instance.Activity = activityInstances[0].Activity
		base.Instance.Description = activityInstances[0].Description

		activityBase[activityKey] = base
	}

	// get calendar boundaries
	var calStart, calEnd time.Time
	for _, a := range schedule.Activities {
		aStart, aEnd := a.Time.In(tz)
		if calStart.IsZero() || aStart.Before(calStart) {
			calStart = aStart
		}
		if calEnd.IsZero() || aEnd.After(calEnd) {
			calStart = aEnd
		}
	}
	if calStart.IsZero() {
		calStart = time.Now().In(tz)
	}
	if calEnd.IsZero() {
		calEnd = time.Now().In(tz)
	}

	return func(o *generateCalendarOptions) []byte {
		// compute excluded activity keys
		activityKeyExcludes := map[ActivityKey]bool{}
		for _, activityKey := range activityKeys {
			if !o.Location.Match(activityKey.Location) {
				// location doesn't match
				activityKeyExcludes[activityKey] = true
				continue
			}
			if !slices.ContainsFunc(activities[activityKey], func(activityInstance ActivityInstance) bool {
				if !o.Activity.Match(activityInstance.Activity) || !o.ActivityID.Match(activityKey.ActivityID) {
					return false
				}
				if !slices.ContainsFunc(activityInstance.Categories, o.Category.Match) {
					return false
				}
				if !slices.ContainsFunc(activityInstance.CategoryIDs, o.CategoryID.Match) {
					return false
				}
				return true
			}) {
				// no matching instances
				activityKeyExcludes[activityKey] = true
				continue
			}
		}

		// generate calendar
		// TODO: refactor, handle values better
		icalTextEscape := strings.NewReplacer(
			"\r\n", "\\n",
			"\n", "\\n",
			"\\", "\\\\",
			";", "\\;",
			",", "\\,",
		)

		var ical bytes.Buffer
		fmt.Fprintf(&ical, "BEGIN:VCALENDAR\r\n")
		fmt.Fprintf(&ical, "VERSION:2.0\r\n")
		fmt.Fprintf(&ical, "PRODID:arcical\r\n")
		fmt.Fprintf(&ical, "NAME:ARC Schedule\r\n")
		fmt.Fprintf(&ical, "X-WR-CALNAME:ARC Schedule\r\n")
		fmt.Fprintf(&ical, "REFRESH-INTERVAL;VALUE=DURATION:PT60M\r\n")
		fmt.Fprintf(&ical, "X-PUBLISHED-TTL:PT60M\r\n")
		fmt.Fprintf(&ical, "LAST-MODIFIED:%s\r\n", time.Now().UTC().Format("20060102T150405Z"))
		fmt.Fprintf(&ical, "COLOR:%s\r\n", "firebrick") // note: closest CSS extended color keyword to red #b90e31 (=b22222 firebrick) blue #002452 (=000080 navy)
		fmt.Fprintf(&ical, "BEGIN:VTIMEZONE\r\n")
		fmt.Fprintf(&ical, "TZID:%s\r\n", tz)
		for start, end := calStart.ZoneBounds(); !start.After(calEnd.AddDate(1, 0, 0)); start, end = end.ZoneBounds() {
			var tztype string
			if start.IsDST() {
				tztype = "DAYLIGHT"
			} else {
				tztype = "STANDARD"
			}
			fmt.Fprintf(&ical, "BEGIN:%s\r\n", tztype)
			fmt.Fprintf(&ical, "TZNAME:%s\r\n", start.Format("MST"))
			fmt.Fprintf(&ical, "DTSTART:%s\r\n", start.Format("20060102T150405")) // local
			fmt.Fprintf(&ical, "TZOFFSETFROM:%s\r\n", start.AddDate(0, 0, -1).Format("-0700"))
			fmt.Fprintf(&ical, "TZOFFSETTO:%s\r\n", start.Format("-0700"))
			fmt.Fprintf(&ical, "END:%s\r\n", tztype)
			if end.IsZero() {
				break
			}
		}
		fmt.Fprintf(&ical, "END:VTIMEZONE\r\n")
		for _, activityKey := range activityKeys {
			if activityKeyExcludes[activityKey] {
				continue
			}

			// https://www.nylas.com/blog/calendar-events-rrules/

			// generate a uid from all fields of activityKey
			uid := fmt.Sprintf(
				"%s-%x-%02d%02d%02d@school%d.innosoftfusiongo.com",
				activityKey.ActivityID, sha1.Sum([]byte(activityKey.Location)),
				activityKey.StartTime.Hour, activityKey.StartTime.Minute, activityKey.StartTime.Second,
				schoolID,
			)
			if len(uid) >= 255 {
				panic("wtf: generated uid too long")
			}

			base := activityBase[activityKey]

			var byday []string
			if base.Recurrence != nil {
				if base.Recurrence[time.Sunday] > 0 {
					byday = append(byday, "SU")
				}
				if base.Recurrence[time.Monday] > 0 {
					byday = append(byday, "MO")
				}
				if base.Recurrence[time.Tuesday] > 0 {
					byday = append(byday, "TU")
				}
				if base.Recurrence[time.Wednesday] > 0 {
					byday = append(byday, "WE")
				}
				if base.Recurrence[time.Thursday] > 0 {
					byday = append(byday, "TH")
				}
				if base.Recurrence[time.Friday] > 0 {
					byday = append(byday, "FR")
				}
				if base.Recurrence[time.Saturday] > 0 {
					byday = append(byday, "SA")
				}
			}

			var excDesc strings.Builder
			if base.Recurrence != nil && o.DescribeRecurrence {
				excDesc.WriteString("\n\n")
				excDesc.WriteString("Repeats ")
				excDesc.WriteString(activityKey.StartTime.String())
				excDesc.WriteString(" - ")
				excDesc.WriteString(base.Instance.EndTime.String())
				var hasDayExceptions bool
				for x := time.Weekday(0); x < 7; x++ {
					if base.Recurrence[x] == 0 {
						hasDayExceptions = true
						break
					}
				}
				if hasDayExceptions {
					excDesc.WriteString(" [")
					for i, x := range byday {
						if i != 0 {
							excDesc.WriteString(", ")
						}
						excDesc.WriteString(x)
					}
					excDesc.WriteString("]")
				}
				excDesc.WriteString("\n")
				if base.Recurrence != nil {
					for d := base.Instance.Date.In(tz); !base.Last.In(tz).Before(d); d = d.AddDate(0, 0, 1) {
						if base.Recurrence[d.Weekday()] > 0 {
							dy, dm, dd := d.Date()
							df := fusiongo.Date{Year: dy, Month: dm, Day: dd}
							i := slices.IndexFunc(activities[activityKey], func(activityInstance ActivityInstance) bool {
								return activityInstance.Date == df
							})
							if i == -1 {
								excDesc.WriteString(" • not on ")
								excDesc.WriteString(df.In(tz).Format("Mon Jan 02"))
								excDesc.WriteString("\n")
								continue
							}
							activityInstance := activities[activityKey][i]

							var diffs []string
							if activityInstance.Activity != base.Instance.Activity {
								diffs = append(diffs, "name="+strconv.Quote(activityInstance.Activity))
							}
							if activityInstance.Description != base.Instance.Description {
								diffs = append(diffs, "description")
							}
							if activityInstance.EndTime != base.Instance.EndTime {
								diffs = append(diffs, "end="+activityInstance.EndTime.String())
							}
							if activityInstance.IsCancelled {
								diffs = append(diffs, "cancelled")
							}
							if len(diffs) != 0 {
								excDesc.WriteString(" • exception on ")
								excDesc.WriteString(df.In(tz).Format("Mon Jan 02"))
								for i, x := range diffs {
									if i == 0 {
										excDesc.WriteString(": ")
									} else {
										excDesc.WriteString(", ")
									}
									excDesc.WriteString(x)
								}
								excDesc.WriteString("\n")
							}
						}
					}
				}
			}

			// write the base event
			fmt.Fprintf(&ical, "BEGIN:VEVENT\r\n")
			fmt.Fprintf(&ical, "UID:%s\r\n", uid)
			fmt.Fprintf(&ical, "DTSTAMP:%s\r\n", schedule.Updated.UTC().Format("20060102T150405Z")) // utc; this should be when the event was created, but unfortunately, we can'te determine that determinstically, so just use the schedule update time
			fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape.Replace(base.Instance.Activity))
			fmt.Fprintf(&ical, "LOCATION:%s\r\n", icalTextEscape.Replace(activityKey.Location))
			fmt.Fprintf(&ical, "DESCRIPTION:%s\r\n", icalTextEscape.Replace(base.Instance.Description+excDesc.String()))
			fmt.Fprintf(&ical, "DTSTART;TZID=%s:%s\r\n", tz, activityKey.StartTime.WithDate(base.Instance.Date).In(tz).Format("20060102T150405")) // local
			fmt.Fprintf(&ical, "DTEND;TZID=%s:%s\r\n", tz, base.Instance.EndTime.WithDate(base.Instance.Date).In(tz).Format("20060102T150405"))   // local
			if base.Recurrence != nil {
				fmt.Fprintf(&ical, "RRULE:FREQ=WEEKLY;INTERVAL=1;UNTIL=%s;BYDAY=%s\r\n", base.Instance.EndTime.WithDate(base.Last).In(tz).Format("20060102T150405"), strings.Join(byday, ",")) // local if dtstart is local, else utc
				for d := base.Instance.Date.In(tz); !base.Last.In(tz).Before(d); d = d.AddDate(0, 0, 1) {
					if base.Recurrence[d.Weekday()] > 0 {
						if dy, dm, dd := d.Date(); !slices.ContainsFunc(activities[activityKey], func(activityInstance ActivityInstance) bool {
							if activityInstance.Date != (fusiongo.Date{Year: dy, Month: dm, Day: dd}) {
								return false
							}
							if activityInstance.IsCancelled && o.DeleteCancelled {
								return false
							}
							if !o.Activity.Match(activityInstance.Activity) || !o.ActivityID.Match(activityKey.ActivityID) {
								return false
							}
							if !slices.ContainsFunc(activityInstance.Categories, o.Category.Match) {
								return false
							}
							if !slices.ContainsFunc(activityInstance.CategoryIDs, o.Category.Match) {
								return false
							}
							return true
						}) {
							fmt.Fprintf(&ical, "EXDATE;TZID=%s:%s\r\n", tz, activityKey.StartTime.WithDate(fusiongo.Date{Year: dy, Month: dm, Day: dd}).In(tz).Format("20060102T150405"))
						}
					}
				}
			}
			fmt.Fprintf(&ical, "END:VEVENT\r\n")

			// write recurrence exceptions
			if base.Recurrence != nil {
				for _, activityInstance := range activities[activityKey] {
					if activityInstance.IsCancelled && o.DeleteCancelled {
						continue
					}
					if activityInstance.Activity != base.Instance.Activity || activityInstance.Description != base.Instance.Description || activityInstance.EndTime != base.Instance.EndTime || activityInstance.IsCancelled {
						if !o.Activity.Match(activityInstance.Activity) || !o.ActivityID.Match(activityKey.ActivityID) {
							continue
						}
						if !slices.ContainsFunc(activityInstance.Categories, o.Category.Match) {
							continue
						}
						if !slices.ContainsFunc(activityInstance.CategoryIDs, o.CategoryID.Match) {
							continue
						}
						fmt.Fprintf(&ical, "BEGIN:VEVENT\r\n")
						fmt.Fprintf(&ical, "UID:%s\r\n", uid)
						fmt.Fprintf(&ical, "DTSTAMP:%s\r\n", schedule.Updated.UTC().Format("20060102T150405Z"))       // utc; this should be when the event was created, but unfortunately, we can'te determine that determinstically, so just use the schedule update time
						fmt.Fprintf(&ical, "LAST-MODIFIED:%s\r\n", schedule.Updated.UTC().Format("20060102T150405Z")) // utc
						if !activityInstance.IsCancelled {
							fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape.Replace(activityInstance.Activity))
						} else {
							fmt.Fprintf(&ical, "SUMMARY:CANCELLED - %s\r\n", icalTextEscape.Replace(activityInstance.Activity))
							if !o.FakeCancelled {
								fmt.Fprintf(&ical, "STATUS:CANCELLED\r\n")
							}
						}
						fmt.Fprintf(&ical, "LOCATION:%s\r\n", icalTextEscape.Replace(activityKey.Location))
						fmt.Fprintf(&ical, "DESCRIPTION:%s\r\n", icalTextEscape.Replace(activityInstance.Description+excDesc.String()))
						fmt.Fprintf(&ical, "DTSTART;TZID=%s:%s\r\n", tz, activityKey.StartTime.WithDate(activityInstance.Date).In(tz).Format("20060102T150405"))  // local
						fmt.Fprintf(&ical, "DTEND;TZID=%s:%s\r\n", tz, activityInstance.EndTime.WithDate(activityInstance.Date).In(tz).Format("20060102T150405")) // local
						if base.Recurrence != nil {
							fmt.Fprintf(&ical, "RECURRENCE-ID;TZID=%s:%s\r\n", tz, activityKey.StartTime.WithDate(activityInstance.Date).In(tz).Format("20060102T150405")) // local
						}
						fmt.Fprintf(&ical, "END:VEVENT\r\n")
					}
				}
			}
		}
		if !o.NoNotifications {
			// add notifications as all-day events for the current day
			for _, n := range notifications.Notifications {
				// https://stackoverflow.com/questions/1716237/single-day-all-day-appointments-in-ics-files

				uid := fmt.Sprintf("notification-%s@school%d.innosoftfusiongo.com", n.ID, schoolID)
				if len(uid) >= 255 {
					panic("wtf: generated uid too long")
				}
				fmt.Fprintf(&ical, "BEGIN:VEVENT\r\n")
				fmt.Fprintf(&ical, "UID:%s\r\n", uid)
				fmt.Fprintf(&ical, "SEQUENCE:1\r\n")
				fmt.Fprintf(&ical, "DTSTAMP:%s\r\n", notifications.Updated.UTC().Format("20060102T150405Z")) // utc
				fmt.Fprintf(&ical, "SUMMARY:%s\r\n", icalTextEscape.Replace(n.Text))
				fmt.Fprintf(&ical, "DESCRIPTION:%s\r\n", icalTextEscape.Replace(n.Text))
				fmt.Fprintf(&ical, "DTSTART;VALUE=DATE;TZID=%s:%s\r\n", tz, n.Sent.In(tz).Format("20060102")) // local
				fmt.Fprintf(&ical, "END:VEVENT\r\n")
			}
		}
		fmt.Fprintf(&ical, "END:VCALENDAR\r\n")

		return ical.Bytes()
	}, nil
}

type filterer struct {
	Patterns []string
	Negation bool // support exclude patterns
	Wildcard bool // support * wildcards
	CaseFold bool // case-insensitive match
	Collapse bool // replace consecutive whitespace with a single space, trim leading/trailing

	compile sync.Once
	include []*regexp.Regexp
	exclude []*regexp.Regexp
}

func (f *filterer) Match(s string) bool {
	f.compile.Do(func() {
		for _, p := range f.Patterns {
			var negate bool
			if f.Negation {
				p, negate = strings.CutPrefix(p, "-")
			}

			var wildStart, wildEnd bool
			if f.Wildcard {
				p, wildStart = strings.CutPrefix(p, "*")
				p, wildEnd = strings.CutSuffix(p, "*")
			}

			if f.Collapse {
				p = strings.Join(strings.Fields(p), " ")
			}

			var re strings.Builder
			if f.CaseFold {
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
	})

	var match bool
	if len(f.include) == 0 {
		match = true
	}
	if !match || len(f.exclude) != 0 {
		if f.Collapse {
			s = strings.Join(strings.Fields(s), " ")
		}
		for _, c := range f.include {
			if c.MatchString(s) {
				match = true
				break
			}
		}
		for _, c := range f.exclude {
			if c.MatchString(s) {
				match = false
				break
			}
		}
	}
	return match
}
