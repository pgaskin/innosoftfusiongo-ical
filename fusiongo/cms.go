package fusiongo

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"

	"github.com/tidwall/gjson"
)

// CMS is the URL of an Innosoft Fusion Go CMS.
//
// See config.json in a com.innosoftfusiongo.* package on Google Play for this
// and the school ID (note: all instances are considered schools, presumably for
// legacy reasons).
type CMS string

// ProductionCMS is the default production instance for Innosoft Fusion Go.
const ProductionCMS CMS = "https://innosoftfusiongo.com/"

// FetchJSON gets the JSON for the provided school ID and type.
func (c CMS) FetchJSON(ctx context.Context, cl *http.Client, schoolID int, dataType string) ([]byte, error) {
	if cl == nil {
		cl = http.DefaultClient
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, string(ProductionCMS), nil)
	if err != nil {
		return nil, err
	}
	req.URL.Path = path.Join("/", req.URL.Path, "schools", "school"+strconv.Itoa(schoolID), url.PathEscape(dataType)+".json")

	resp, err := cl.Do(req)
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
