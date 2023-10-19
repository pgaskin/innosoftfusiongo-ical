package testdata

import (
	"embed"
	"io/fs"
	"sync"
	"testing"

	"github.com/pgaskin/innosoftfusiongo-ical/fusiongo"
)

//go:generate go run download.go

//go:embed */*
var testdata embed.FS

// CMS returns a [fusiongo.CMS] using the specified testdata. If none exists,
// nil is returned.
func CMS(d string) fusiongo.CMS {
	if fi, err := fs.Stat(testdata, d); err == nil && fi.IsDir() {
		if sf, err := fs.Sub(testdata, d); err == nil {
			return fusiongo.MockCMS(sf)
		}
	}
	return nil
}

var useMu sync.Mutex

// Use is a convenience method for tests which sets the default CMS to the
// provided testdata (panicking if it isn't found) until the returned function
// is called. Calls to Use must not overlap.
//
//	func(t *testing.T) {
//		defer testdata.Use("20231015")()
//		...
//	}
func Use(d string) func() {
	if cms := CMS(d); cms != nil {
		if !useMu.TryLock() {
			panic("testdata: overlapping use calls")
		}
		old := fusiongo.DefaultCMS
		fusiongo.DefaultCMS = cms
		return func() {
			fusiongo.DefaultCMS = old
			useMu.Unlock()
		}
	}
	panic("testdata: nothing for " + d)
}

// Run runs tests using all stored testdata using [Use] internally. It must not
// be run in parallel.
func Run(t *testing.T, fn func(t *testing.T, d string)) {
	fis, err := fs.ReadDir(testdata, ".")
	if err != nil {
		panic(err)
	}

	for _, fi := range fis {
		if fi.IsDir() {
			t.Run(fi.Name(), func(t *testing.T) {
				defer Use(fi.Name())()

				fn(t, fi.Name())
			})
		}
	}
}
