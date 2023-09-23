//go:build go1.15

package vtimezone

import (
	"reflect"
	"time"
	"unsafe"
)

//go:linkname timeLocationGet time.(*Location).get
func timeLocationGet(*time.Location) *time.Location

func init() {
	// definitely works from Go 1.15.0 to at least Go 1.21.0
	if t := reflect.TypeOf(new(time.Location)); t.Elem().Kind() != reflect.Struct {
		panic("vtimezone: unsupported Go version: " + t.String() + " is not a struct!?!")
	} else if f, _ := t.Elem().FieldByName("extend"); f.Type.Kind() != reflect.String {
		panic("vtimezone: unsupported Go version: " + t.String() + " does not contain extended timezone information (extend string)")
	} else if unsafe.Pointer(timeLocationGet(nil)) != unsafe.Pointer(time.UTC) {
		panic("vtimezone: unsupported Go version: (" + t.String() + ").get is broken")
	} else {
		timeLocationExtend = func(loc *time.Location) string {
			return reflect.ValueOf(timeLocationGet(loc)).Elem().FieldByName("extend").String()
		}
	}
}
