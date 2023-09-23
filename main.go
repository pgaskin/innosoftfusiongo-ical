package main

import (
	"context"
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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/pgaskin/innosoftfusiongo-ical/fusiongo"
	"github.com/pgaskin/innosoftfusiongo-ical/ifgical"
	"github.com/pgaskin/innosoftfusiongo-ical/vtimezone"
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
	Timezone          = flag_TimezoneMap("timezone", MustParseTimezoneMap("UTC,110=America/Toronto"), "Default timezone name, plus optional comma-separated schoolID=timezone pairs.")
)

func flag_Level(name string, value slog.Level, usage string) *slog.Level {
	v := new(slog.Level)
	flag.TextVar(v, name, value, usage)
	return v
}

func flag_TimezoneMap(name string, value TimezoneMap, usage string) *TimezoneMap {
	v := new(TimezoneMap)
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

	// ensure timezones are usable
	for _, tz := range *Timezone {
		if _, err := vtimezone.AppendRRULE(nil, tz); err != nil {
			fmt.Fprintf(flag.CommandLine.Output(), "cannot use timezone %s: %v\n", tz, err)
			os.Exit(2)
		}
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

	// setup testdata
	if *Testdata != "" {
		fusiongo.DefaultCMS = fusiongo.MockCMS(os.DirFS(*Testdata))
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

func handleCalendar(w http.ResponseWriter, r *http.Request, schoolID int) {
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	// limit query length
	if cur, lim := len(r.URL.RawQuery), 512; cur > lim {
		slog.Info("rejected request with long query string", "remote", r.RemoteAddr, "instance", schoolID)
		http.Error(w, fmt.Sprintf("Request query too long (%d > %d)", cur, lim), http.StatusRequestURITooLong)
		return
	}

	// get cached prepared calendar
	cal, err := prepareCalendarCached(schoolID)
	if err != nil {
		if cal == nil {
			http.Error(w, fmt.Sprintf("Failed to fetch data: %v", err), http.StatusInternalServerError)
			return
		}
		w.Header().Set("X-Refresh-Error", err.Error())
	}
	if cal == nil {
		http.Error(w, "No calendar data available", http.StatusInternalServerError)
		return
	}

	// parse options
	q := r.URL.Query()
	opt := ifgical.Options{
		Category:           queryFilter(q, "category", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		CategoryID:         queryFilter(q, "category_id", filterOpts{Negation: true, Wildcard: false, CaseFold: true, Collapse: true}),
		Activity:           queryFilter(q, "activity", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		ActivityID:         queryFilter(q, "activity_id", filterOpts{Negation: true, Wildcard: false, CaseFold: true, Collapse: true}),
		Location:           queryFilter(q, "location", filterOpts{Negation: true, Wildcard: true, CaseFold: true, Collapse: true}),
		NoNotifications:    queryBool(q, "no_notifications"),
		FakeCancelled:      queryBool(q, "fake_cancelled"),
		DeleteCancelled:    queryBool(q, "delete_cancelled"),
		DescribeRecurrence: queryBool(q, "describe_recurrence"),
	}

	// apply overrides
	switch schoolID {
	case 110:
		opt.CalendarName = "ARC Schedule"
		opt.CalendarColor = "firebrick" // note: closest CSS extended color keyword to red #b90e31 (=b22222 firebrick) blue #002452 (=000080 navy)
	}

	// render calendar
	generateStart := time.Now()
	buf := cal.Render(opt)
	w.Header().Set("X-Generate-Time", strconv.FormatFloat(time.Since(generateStart).Seconds(), 'f', -1, 64))

	// serve calendar
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

type cacheEntry struct {
	Mu sync.Mutex

	// error info (will be cleared on success)
	Failure time.Time // when the error happened
	Error   error     // error details
	ErrorN  int       // error count

	// success info (will be cleared on invalidation)
	Success  time.Time         // last time the calendar data was successfully updated
	Calendar *ifgical.Calendar // prepared calendar
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

// prepareCalendarCached gets the latest generate function for schoolID. Even if
// it returns an error, it may still return an old generateCalendarFunc if it
// isn't too stale.
func prepareCalendarCached(schoolID int) (*ifgical.Calendar, error) {
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

func prepareCalendar(ctx context.Context, schoolID int) (*ifgical.Calendar, error) {
	tz, _ := Timezone.Get(schoolID)

	schedule, err := fusiongo.FetchSchedule(ctx, schoolID)
	if err != nil {
		return nil, err
	}

	notifications, err := fusiongo.FetchNotifications(ctx, schoolID)
	if err != nil {
		return nil, err
	}

	return ifgical.Prepare(tz, ifgical.Data{
		SchoolID:      schoolID,
		Schedule:      schedule,
		Notifications: notifications,
	})
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

// TimezoneMap maps school IDs to timezones.
type TimezoneMap map[int]*time.Location

func MustParseTimezoneMap(s string) TimezoneMap {
	m := make(TimezoneMap)
	if err := m.UnmarshalText([]byte(s)); err != nil {
		panic(err)
	}
	return m
}

// Default gets the default timezone, or time.UTC if none was set. It will
// never return nil.
func (t TimezoneMap) Default() *time.Location {
	if t != nil {
		if loc := t[0]; loc != nil {
			return loc
		}
	}
	return time.UTC
}

// Get gets the timezone for id. It will never return nil.
func (t TimezoneMap) Get(id int) (*time.Location, bool) {
	if t == nil {
		return t.Default(), false
	}
	loc, ok := t[id]
	if !ok {
		loc = t.Default()
	}
	return loc, ok
}

// UnmarshalText parses the default timezone followed by zero or more
// comma-separated id=timezone pairs.
func (t TimezoneMap) UnmarshalText(b []byte) error {
	def, rest, _ := strings.Cut(string(b), ",")

	loc, err := time.LoadLocation(def)
	if err != nil {
		return fmt.Errorf("load default timezone %q: %w", def, err)
	}
	t[0] = loc

	var val string
	for rest != "" {
		val, rest, _ = strings.Cut(rest, ",")
		idS, tz, ok := strings.Cut(val, "=")
		if !ok {
			return fmt.Errorf("parse id=timezone pair %q: missing =", val)
		}
		id, err := strconv.ParseInt(idS, 10, 64)
		if err != nil {
			return fmt.Errorf("parse id=timezone pair %q: invalid id %q: %w", val, idS, err)
		}
		loc, err := time.LoadLocation(tz)
		if err != nil {
			return fmt.Errorf("parse id=timezone pair %q: load timezone %q: %w", val, tz, err)
		}
		t[int(id)] = loc
	}
	return nil
}

// MarshalText is the opposite of UnmarshalText, but will sort the values.
func (t TimezoneMap) MarshalText() ([]byte, error) {
	if t == nil {
		return []byte(t.Default().String()), nil
	}

	ids := make([]int, 0, len(t))
	for id := range t {
		if id != 0 {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	var b []byte
	b = append(b, t.Default().String()...)
	for _, id := range ids {
		b = append(b, ',')
		b = strconv.AppendInt(b, int64(id), 10)
		b = append(b, '=')
		b = append(b, t[id].String()...)
	}
	return b, nil
}
