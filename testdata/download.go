//go:build ignore

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/pgaskin/innosoftfusiongo-ical/fusiongo"
)

func main() {
	d := time.Now().UTC().Format("20060102")
	slog.Info("downloading testdata", "name", d)

	var (
		ns []string
		bs [][]byte
	)
	for _, schoolID := range []int{20, 110} {
		for _, dataType := range []string{"schedule", "facilities", "notifications"} {
			slog.Info("fetching", "cms", string(fusiongo.ProductionCMS), "school_id", schoolID, "data_type", dataType)

			buf, err := fusiongo.ProductionCMS.FetchJSON(context.Background(), schoolID, dataType)
			if err != nil {
				slog.Error("failed to fetch data", "error", err)
				os.Exit(1)
			}

			ns = append(ns, filepath.FromSlash(fmt.Sprintf("%s/school%d/%s.json", d, schoolID, dataType)))
			bs = append(bs, buf)
		}
	}
	for i, n := range ns {
		slog.Info("writing", "name", n)
		if err := os.MkdirAll(filepath.Dir(n), 0755); err != nil {
			slog.Error("failed to create directories", "error", err)
			os.RemoveAll(d)
			os.Exit(1)
		}
		if err := os.WriteFile(n, bs[i], 0644); err != nil {
			slog.Error("failed to write file", "error", err)
			os.RemoveAll(d)
			os.Exit(1)
		}
	}
}
