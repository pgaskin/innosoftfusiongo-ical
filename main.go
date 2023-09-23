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
	"github.com/ringsaturn/tzf"
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
	InstanceWhitelist = flag_SchoolWhitelist("instance-whitelist", make(SchoolWhitelist), "Comma-separated whitelist of Innosoft Fusion Go instances to get data from")
	Testdata          = flag.String("testdata", "", "Path to directory containing school%d/*.json files to test with")
	Timezone          = flag_TimezoneMap("timezone", make(TimezoneMap), "Default timezone name (used when it can't be detected from facilities), plus optional comma-separated schoolID=timezone override pairs.")
	NoTimezoneFinder  = flag.Bool("no-timezone-finder", false, "Disable automatic timezone detection")
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

func flag_SchoolWhitelist(name string, value SchoolWhitelist, usage string) *SchoolWhitelist {
	v := new(SchoolWhitelist)
	flag.TextVar(v, name, value, usage)
	return v
}

var tzfinder tzf.F

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

	// setup timezone finder
	if !*NoTimezoneFinder {
		if f, err := tzf.NewDefaultFinder(); err != nil {
			fmt.Fprintf(flag.CommandLine.Output(), "failed to load timezone finder: %v\n", err)
			os.Exit(2)
		} else {
			tzfinder = f
		}
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

var calendarUserAgentRe = regexp.MustCompile(strings.Join([]string{
	`^Google-Calendar-Importer`, // Google Calendar
	`^Microsoft\.Exchange`,      // OWA
	`(?:^| )CalendarAgent/`,     // macOS
	`(?:^| )dataaccessd/`,       // iOS
	`^ICSx5/`,                   // ICSx5 (Android)
}, "|"))

func handle(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimLeft(path.Clean(r.URL.Path), "/")

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		handleError(w, r, http.StatusMethodNotAllowed, "%s", http.StatusText(http.StatusMethodNotAllowed))
		return
	}

	if p == "favicon.ico" {
		handleFavicon(w, r)
		return
	}

	if !strings.ContainsRune(p, '/') || !strings.HasPrefix(p, ".") {
		if instance, ext, _ := strings.Cut(strings.ToLower(p), "."); instance != "" {
			if schoolID, _ := strconv.ParseInt(strings.TrimPrefix(instance, "school"), 10, 64); schoolID != 0 {
				if !InstanceWhitelist.Has(int(schoolID)) {
					handleError(w, r, http.StatusForbidden, "Instance %q not on whitelist", instance)
					return
				}
				if ext != "ics" && calendarUserAgentRe.MatchString(r.Header.Get("User-Agent")) {
					w.Header().Set("Cache-Control", "private, no-cache, no-store")
					w.Header().Set("Pragma", "no-cache")
					w.Header().Set("Expires", "0")
					http.Redirect(w, r, (&url.URL{
						Path:     "/" + strconv.FormatInt(schoolID, 10) + ".ics",
						RawQuery: r.URL.RawQuery,
					}).String(), http.StatusFound)
					return
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
				handleError(w, r, http.StatusNotFound, "No handler for extension %q", ext)
				return
			}
		}
	}

	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

//go:embed favicon.ico
var favicon []byte

func handleFavicon(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "image/x-icon")
	w.Header().Set("Content-Length", strconv.Itoa(len(favicon)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		w.Write(favicon)
	}
}

//go:embed calendar.html
var calendarHTML []byte

func handleWeb(w http.ResponseWriter, r *http.Request, _ int) {
	w.Header().Set("Cache-Control", "private, no-cache, no-store")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(calendarHTML)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		w.Write(calendarHTML)
	}
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
	if r.Method != http.MethodHead {
		w.Write(buf)
	}
}

func handleError(w http.ResponseWriter, r *http.Request, status int, format string, a ...any) {
	b := append(fmt.Appendf(nil, format, a...), '\n')
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		w.Write(b)
	}
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
	facilities, err := fusiongo.FetchFacilities(ctx, schoolID)
	if err != nil {
		return nil, err
	}

	tz, override := Timezone.Get(schoolID)
	if !override && tzfinder != nil {
	ftz: // find the first valid timezone
		for _, facility := range facilities.Facilities {
			if lng, lat := facility.Longitude, facility.Latitude; lng != 0 && lat != 0 {
				if ns, err := tzfinder.GetTimezoneNames(lng, lat); err == nil {
					for _, n := range ns {
						if tmp, err := time.LoadLocation(n); err == nil {
							tz = tmp
							break ftz
						}
					}
				}
			}
		}
	}

	schedule, err := fusiongo.FetchSchedule(ctx, schoolID)
	if err != nil {
		return nil, err
	}

	notifications, err := fusiongo.FetchNotifications(ctx, schoolID)
	if err != nil {
		return nil, err
	}

	calendar, err := ifgical.Prepare(tz, ifgical.Data{
		SchoolID:      schoolID,
		Schedule:      schedule,
		Notifications: notifications,
	})
	if err != nil {
		return nil, err
	}

	return calendar, nil
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

// SchoolWhitelist is a set of whitelisted school IDs.
type SchoolWhitelist map[int]struct{}

// Has checks if the whitelist is empty or contains id.
func (w SchoolWhitelist) Has(id int) bool {
	if w == nil || len(w) == 0 {
		return true
	}
	if id != 0 {
		if _, ok := w[id]; ok {
			return true
		}
	}
	return false
}

// UnmarshalText parses zero or more comma-separated school IDs.
func (w SchoolWhitelist) UnmarshalText(b []byte) error {
	rest := string(b)

	var val string
	for rest != "" {
		val, rest, _ = strings.Cut(rest, ",")
		idS, _ := strings.CutPrefix(val, "school")
		id, _ := strconv.ParseInt(idS, 10, 64)
		if id == 0 {
			return fmt.Errorf("parse id %q: invalid id %q", val, idS)
		}
		w[int(id)] = struct{}{}
	}
	return nil
}

// MarshalText is the opposite of UnmarshalText, but will sort the values.
func (w SchoolWhitelist) MarshalText() ([]byte, error) {
	var b []byte
	if w != nil {
		ids := make([]int, 0, len(w))
		for id := range w {
			if id != 0 {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)

		for i, id := range ids {
			if i != 0 {
				b = append(b, ',')
			}
			b = strconv.AppendInt(b, int64(id), 10)
		}
	}
	return b, nil
}

// TimezoneMap maps school IDs to timezones.
type TimezoneMap map[int]*time.Location

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
		id, _ := strconv.ParseInt(idS, 10, 64)
		if id == 0 {
			return fmt.Errorf("parse id=timezone pair %q: invalid id %q", val, idS)
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
