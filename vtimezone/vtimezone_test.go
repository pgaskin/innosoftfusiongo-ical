package vtimezone

import (
	"strconv"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"
)

func TestAppend(t *testing.T) {
	for _, tc := range []struct {
		Lbl string
		Loc *time.Location
	}{
		{"", nil},
		{"", time.UTC},
		{"", time.Local},
		{"", mustLoadLocation("America/Toronto")},
		{"", mustLoadLocation("America/Whitehorse")},
		{"", mustLoadLocation("Antarctica/Casey")},
		{"", mustLoadLocation("Africa/Cairo")},
	} {
		if tc.Loc == nil {
			tc.Lbl = "nil"
		} else {
			tc.Lbl = tc.Loc.String()
		}
		t.Run(tc.Lbl, func(t *testing.T) {
			if b, err := Append(nil, tc.Loc); err != nil {
				t.Errorf("unexpected error: %v", err)
			} else {
				t.Log("\n" + string(b))
			}
		})
	}
}

func TestAppendTZ(t *testing.T) {
	for _, tc := range []struct {
		OK     bool
		TZID   string
		TZ     string
		Result string
	}{
		{false, "Empty", "", ""},
		{false, "Invalid", "12345", ""},
		{true, "Simple", "EST5EDT", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-0500
			TZOFFSETTO:-0400
			TZNAME:EDT
			DTSTART:19700308T020000
			RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0400
			TZOFFSETTO:-0500
			TZNAME:EST
			DTSTART:19701101T020000
			RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST-1", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:STANDARD
			TZOFFSETFROM:+0100
			TZOFFSETTO:+0100
			TZNAME:TST
			DTSTART:19700101T010000
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST12TDT,J1,J60", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-1200
			TZOFFSETTO:-1100
			TZNAME:TDT
			DTSTART:19700101T020000
			RRULE:FREQ=YEARLY;BYMONTH=1;BYMONTHDAY=1
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-1100
			TZOFFSETTO:-1200
			TZNAME:TST
			DTSTART:19700301T020000
			RRULE:FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=1
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST-1:30TDT+2:45,0,365", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:DAYLIGHT
			TZOFFSETFROM:+0130
			TZOFFSETTO:-0245
			TZNAME:TDT
			DTSTART:19700101T020000
			RRULE:FREQ=YEARLY;BYYEARDAY=1
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0245
			TZOFFSETTO:+0130
			TZNAME:TST
			DTSTART:19710101T020000
			RRULE:FREQ=YEARLY;BYYEARDAY=366
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST+6TDT,M1.2.3,M4.5.6", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-0600
			TZOFFSETTO:-0500
			TZNAME:TDT
			DTSTART:19700114T020000
			RRULE:FREQ=YEARLY;BYMONTH=1;BYDAY=2WE
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0500
			TZOFFSETTO:-0600
			TZNAME:TST
			DTSTART:19700425T020000
			RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=-1SA
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Complex", "TST+6TDT,M1.2.3/12:34:56,M4.5.6/07:08", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Complex
			X-LIC-LOCATION:Complex
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-0600
			TZOFFSETTO:-0500
			TZNAME:TDT
			DTSTART:19700114T123456
			RRULE:FREQ=YEARLY;BYMONTH=1;BYDAY=2WE
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0500
			TZOFFSETTO:-0600
			TZNAME:TST
			DTSTART:19700425T070800
			RRULE:FREQ=YEARLY;BYMONTH=4;BYDAY=-1SA
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Complex", "TEST+6TESTDT,J1,2/00", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Complex
			X-LIC-LOCATION:Complex
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-0600
			TZOFFSETTO:-0500
			TZNAME:TESTDT
			DTSTART:19700101T020000
			RRULE:FREQ=YEARLY;BYMONTH=1;BYMONTHDAY=1
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0500
			TZOFFSETTO:-0600
			TZNAME:TEST
			DTSTART:19700103T000000
			RRULE:FREQ=YEARLY;BYYEARDAY=3
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Complex", "TST+6TDT,J60,3/23:59:12", unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Complex
			X-LIC-LOCATION:Complex
			BEGIN:DAYLIGHT
			TZOFFSETFROM:-0600
			TZOFFSETTO:-0500
			TZNAME:TDT
			DTSTART:19700301T020000
			RRULE:FREQ=YEARLY;BYMONTH=3;BYMONTHDAY=1
			END:DAYLIGHT
			BEGIN:STANDARD
			TZOFFSETFROM:-0500
			TZOFFSETTO:-0600
			TZNAME:TST
			DTSTART:19700104T235912
			RRULE:FREQ=YEARLY;BYYEARDAY=4
			END:STANDARD
			END:VTIMEZONE
		`)},
	} {
		t.Run(tc.TZID, func(t *testing.T) {
			if b, ok := AppendTZ(nil, tc.TZID, tc.TZ); ok != tc.OK {
				if tc.OK {
					t.Errorf("unexpected error")
				} else {
					t.Errorf("expected error")
				}
			} else if ok {
				if tc.Result != "" && string(b) != tc.Result {
					t.Errorf("incorrect result:\n\t%q\n\t%q", string(b), tc.Result)
				}
				t.Log(tc.TZ + "\n" + string(b))
			}
		})
	}
}

func TestAppendFixed(t *testing.T) {
	for _, tc := range []struct {
		OK     bool
		TZID   string
		TZName string
		Offset int
		Result string
	}{
		{false, "Invalid", "", 0, ""},
		{false, "Invalid", "TST", 24 * 60 * 60, ""},
		{false, "Invalid", "TST", -24 * 60 * 60, ""},
		{true, "Zero", "UTC", 0, unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Zero
			X-LIC-LOCATION:Zero
			BEGIN:STANDARD
			TZOFFSETFROM:+0000
			TZOFFSETTO:+0000
			TZNAME:UTC
			DTSTART:19700101T000000
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST", 165 * 60, unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:STANDARD
			TZOFFSETFROM:+0245
			TZOFFSETTO:+0245
			TZNAME:TST
			DTSTART:19700101T024500
			END:STANDARD
			END:VTIMEZONE
		`)},
		{true, "Simple", "TST", -1145 * 60, unindent(true, `
			BEGIN:VTIMEZONE
			TZID:Simple
			X-LIC-LOCATION:Simple
			BEGIN:STANDARD
			TZOFFSETFROM:-1905
			TZOFFSETTO:-1905
			TZNAME:TST
			DTSTART:19691231T045500
			END:STANDARD
			END:VTIMEZONE
		`)},
	} {
		t.Run(tc.TZID, func(t *testing.T) {
			if b, ok := AppendFixed(nil, tc.TZID, tc.TZName, tc.Offset); ok != tc.OK {
				if tc.OK {
					t.Errorf("unexpected error")
				} else {
					t.Errorf("expected error")
				}
			} else if ok {
				if tc.Result != "" && string(b) != tc.Result {
					t.Errorf("incorrect result:\n\t%q\n\t%q", string(b), tc.Result)
				}
				t.Log(tc.TZName + " " + strconv.Itoa(tc.Offset) + "\n" + string(b))
			}
		})
	}
}

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return loc
}

func unindent(crlf bool, s string) string {
	if lf := "\n"; strings.Contains(s, lf) {
		if !strings.HasPrefix(s, lf) {
			if lf = "\r\n"; !strings.HasPrefix(s, lf) {
				panic("unindent: starts with junk before newline and indent")
			}
		}
		if tmp := strings.TrimRight(s, "\t"); !strings.HasSuffix(tmp, lf) {
			panic("unindent: incorrect trailing indentation")
		} else if ident := lf + "\t" + s[len(tmp):]; !strings.HasPrefix(s, ident) {
			panic("unindent: incorrect leading indentation")
		} else {
			s = strings.ReplaceAll(strings.TrimPrefix(tmp, ident), ident, lf)
		}
		var lfe string
		if crlf {
			lfe = "\r\n"
		} else {
			lfe = "\n"
		}
		if lf != lfe {
			s = strings.ReplaceAll(s, lf, lfe)
		}
	}
	return s
}
