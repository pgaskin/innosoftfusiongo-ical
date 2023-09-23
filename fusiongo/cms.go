package fusiongo

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/tidwall/gjson"
)

// DefaultCMS is the Innosoft Fusion Go CMS used by default.
var DefaultCMS CMS = ProductionCMS

// CMS fetches Innosoft Fusion Go data.
type CMS interface {
	FetchJSON(ctx context.Context, schoolID int, dataType string) ([]byte, error)
}

func fetchAndParse[T any](ctx context.Context, schoolID int, dataType string, parse func([]byte) (*T, error)) (*T, error) {
	buf, err := DefaultCMS.FetchJSON(ctx, schoolID, dataType)
	if err != nil {
		return nil, fmt.Errorf("fetch %s: %w", dataType, err)
	}

	res, err := parse(buf)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", dataType, err)
	}

	return res, nil
}

// ProductionCMS is the default Innosoft Fusion Go CMS.
const ProductionCMS = FusionCMS("https://innosoftfusiongo.com/")

// FusionCMS fetches data from a Innosoft Fusion Go HTTP server with the
// provided URL.
//
// See config.json in a com.innosoftfusiongo.* package on Google Play for this
// and the school ID (note: all instances are considered schools, presumably for
// legacy reasons).
type FusionCMS string

func (c FusionCMS) FetchJSON(ctx context.Context, schoolID int, dataType string) ([]byte, error) {
	return c.With(http.DefaultClient, nil).FetchJSON(ctx, schoolID, dataType)
}

func (c FusionCMS) With(cl *http.Client, hdr http.Header) CMS {
	return fusionCMS{
		url: string(c),
		cl:  cl,
		hdr: hdr,
	}
}

type fusionCMS struct {
	url string
	cl  *http.Client
	hdr http.Header
}

func (c fusionCMS) FetchJSON(ctx context.Context, schoolID int, dataType string) ([]byte, error) {
	if c.cl == nil {
		c.cl = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return nil, err
	}
	req.URL.Path = path.Join("/", req.URL.Path, "schools", "school"+strconv.Itoa(schoolID), url.PathEscape(dataType)+".json")

	if c.hdr != nil {
		for k, v := range c.hdr {
			req.Header[k] = v
		}
	}

	resp, err := c.cl.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response status %d (%s)", resp.StatusCode, resp.Status)
	}

	if !gjson.ValidBytes(buf) {
		return nil, fmt.Errorf("invalid json returned")
	}
	return buf, nil
}

// MockCMS fetches data from school%d/%s.json in the provided filesystem.
func MockCMS(fsys fs.FS) CMS {
	return fsysCMS{fsys: fsys}
}

type fsysCMS struct {
	fsys fs.FS
}

func (c fsysCMS) FetchJSON(ctx context.Context, schoolID int, dataType string) ([]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		errCh = make(chan error)
		bufCh = make(chan []byte)
	)
	go func() {
		if buf, err := fs.ReadFile(c.fsys, fmt.Sprintf("school%d/%s.json", schoolID, dataType)); err != nil {
			errCh <- err
		} else {
			bufCh <- buf
		}
		<-ctx.Done()
		close(errCh)
		close(bufCh)
	}()

	select {
	case buf := <-bufCh:
		return buf, nil
	case err := <-errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
