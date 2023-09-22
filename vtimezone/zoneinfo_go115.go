//go:build go1.15 && !go1.22

package vtimezone

import (
	"reflect"
	"time"
	_ "unsafe"
)

// it's silly that we have to duplicate code from time/zoneinfo.go and access
// private fields, but it hasn't changed since support for extend was added in
// go1.15, and is easier than manually doing everything
//
// even so, I've hardcoded the required Go version so it needs to be updated for
// each release

type ruleKind int

const (
	ruleJulian ruleKind = iota
	ruleDOY
	ruleMonthWeekDay
)

// if this changes, we'll probably segfault
type rule struct {
	kind ruleKind
	day  int
	week int
	mon  int
	time int // transition time
}

//go:linkname tzsetName time.tzsetName
func tzsetName(s string) (string, string, bool)

//go:linkname tzsetOffset time.tzsetOffset
func tzsetOffset(s string) (offset int, rest string, ok bool)

//go:linkname tzsetRule time.tzsetRule
func tzsetRule(s string) (rule, string, bool)

//go:linkname tzruleTime time.tzruleTime
func tzruleTime(year int, r rule, off int) int

// getter for time.Location.extend
func locExtend(loc *time.Location) string {
	return reflect.ValueOf(loc).Elem().FieldByName("extend").String()
}

// partial copy of time.tzset, but stopping after parsing the rules
func tzset(s string) (
	stdName string,
	stdOffset int,
	dstName string,
	dstOffset int,
	startRule rule,
	endRule rule,
	hasDST bool,
	ok bool,
) {
	stdName, s, ok = tzsetName(s)
	if ok {
		stdOffset, s, ok = tzsetOffset(s)
	}
	if !ok {
		ok = false
		return
	}
	stdOffset = -stdOffset

	if len(s) == 0 || s[0] == ',' {
		return // no dst
	}
	hasDST = true

	dstName, s, ok = tzsetName(s)
	if ok {
		if len(s) == 0 || s[0] == ',' {
			dstOffset = stdOffset + 60*60
		} else {
			dstOffset, s, ok = tzsetOffset(s)
			dstOffset = -dstOffset
		}
	}
	if !ok {
		ok = false
		return
	}

	if len(s) == 0 {
		s = ",M3.2.0,M11.1.0"
	}
	if s[0] != ',' && s[0] != ';' {
		ok = false
		return
	}
	s = s[1:]

	startRule, s, ok = tzsetRule(s)
	if !ok || len(s) == 0 || s[0] != ',' {
		ok = false
		return
	}
	s = s[1:]
	endRule, s, ok = tzsetRule(s)
	if !ok || len(s) > 0 {
		ok = false
		return
	}

	return
}
