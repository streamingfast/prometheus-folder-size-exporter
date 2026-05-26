package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	flag "github.com/spf13/pflag"
)

type target struct {
	name string
	path string
}

func main() {
	var interval time.Duration
	var label string
	var listen string
	var metricName string
	var duTimeout time.Duration

	flag.DurationVarP(&interval, "frequency", "f", time.Minute, "How often to scan the folders (e.g. 1m, 30s)")
	flag.StringVar(&label, "label", "name", "Prometheus label key used to identify each folder")
	flag.StringVar(&listen, "listen-addr", ":9101", "Address to expose Prometheus metrics on")
	flag.StringVar(&metricName, "metric-name", "folder_size_bytes", "Prometheus metric name")
	flag.DurationVar(&duTimeout, "du-timeout", 5*time.Minute, "Per-folder `du` timeout")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] NAME=PATH [NAME=PATH ...]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Periodically runs `du -sk` on each given folder and exposes the size in bytes as a Prometheus gauge.\n\n")
		fmt.Fprintf(os.Stderr, "Example:\n  %s -f 1m --label=network my-app-1=/data/myapp1 my-app-2=/data/app2\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	targets, err := parseTargets(flag.Args())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		flag.Usage()
		os.Exit(2)
	}
	if len(targets) == 0 {
		fmt.Fprintf(os.Stderr, "error: at least one NAME=PATH argument is required\n\n")
		flag.Usage()
		os.Exit(2)
	}

	registry := prometheus.NewRegistry()
	sizeGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricName,
			Help: "Size in bytes of the watched folder, as reported by `du -sk`.",
		},
		[]string{label},
	)
	scrapeDuration := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricName + "_scrape_duration_seconds",
			Help: "Time spent running `du` for this folder, in seconds.",
		},
		[]string{label},
	)
	scrapeSuccess := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricName + "_scrape_success",
			Help: "1 if the last `du` run for this folder succeeded, 0 otherwise.",
		},
		[]string{label},
	)
	lastScrape := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: metricName + "_last_scrape_timestamp_seconds",
			Help: "Unix timestamp of the last completed `du` run for this folder.",
		},
		[]string{label},
	)
	registry.MustRegister(sizeGauge, scrapeDuration, scrapeSuccess, lastScrape)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	log.Printf("starting export-folder-sizes: interval=%s label=%q listen=%s targets=%d", interval, label, listen, len(targets))
	for _, t := range targets {
		log.Printf("  watching %s=%s", t.name, t.path)
	}

	go scanLoop(ctx, interval, duTimeout, targets, label, sizeGauge, scrapeDuration, scrapeSuccess, lastScrape)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "export-folder-sizes\nmetrics at /metrics\n")
	})

	srv := &http.Server{Addr: listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}

func parseTargets(args []string) ([]target, error) {
	seen := map[string]bool{}
	out := make([]target, 0, len(args))
	for _, arg := range args {
		name, path, ok := strings.Cut(arg, "=")
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("invalid target %q: expected NAME=PATH", arg)
		}
		if seen[name] {
			return nil, fmt.Errorf("duplicate target name %q", name)
		}
		seen[name] = true
		out = append(out, target{name: name, path: path})
	}
	return out, nil
}

func scanLoop(
	ctx context.Context,
	interval, duTimeout time.Duration,
	targets []target,
	label string,
	size, dur, success, last *prometheus.GaugeVec,
) {
	scan := func() {
		var wg sync.WaitGroup
		for _, t := range targets {
			wg.Add(1)
			go func(t target) {
				defer wg.Done()
				start := time.Now()
				bytes, err := duBytes(ctx, t.path, duTimeout)
				elapsed := time.Since(start).Seconds()
				dur.With(prometheus.Labels{label: t.name}).Set(elapsed)
				last.With(prometheus.Labels{label: t.name}).Set(float64(time.Now().Unix()))
				if err != nil {
					log.Printf("du failed for %s=%s: %v", t.name, t.path, err)
					success.With(prometheus.Labels{label: t.name}).Set(0)
					return
				}
				size.With(prometheus.Labels{label: t.name}).Set(float64(bytes))
				success.With(prometheus.Labels{label: t.name}).Set(1)
			}(t)
		}
		wg.Wait()
	}

	scan()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scan()
		}
	}
}

func duBytes(ctx context.Context, path string, timeout time.Duration) (int64, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(cctx, "du", "-sk", path)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()

	// `du` walks the tree concurrently with whatever the watched application
	// is doing, so files frequently disappear mid-scan. We treat those ENOENT
	// lines as benign (du still prints a running total) but surface anything
	// else (Permission denied, I/O errors, ...) as a real failure.
	var fatal []string
	for _, line := range strings.Split(strings.TrimRight(stderr.String(), "\n"), "\n") {
		if line == "" || strings.HasSuffix(line, ": No such file or directory") {
			continue
		}
		fatal = append(fatal, line)
	}
	if len(fatal) > 0 {
		return 0, fmt.Errorf("du: %s", strings.Join(fatal, "; "))
	}

	fields := strings.Fields(stdout.String())
	if len(fields) < 1 {
		if runErr != nil {
			return 0, fmt.Errorf("du failed with no output: %w", runErr)
		}
		return 0, fmt.Errorf("unexpected du output: %q", stdout.String())
	}
	kb, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing du output %q: %w", fields[0], err)
	}
	return kb * 1024, nil
}
