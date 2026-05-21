# prometheus-folder-size-exporter

A small Prometheus exporter that periodically runs `du -sk` on a list of folders and exposes their sizes as gauges.

## Usage

```
prometheus-folder-size-exporter -f 1m --label=network my-app-1=/data/myapp1 my-app-2=/data/app2
```

Metrics are exposed at `http://<listen-addr>/metrics` (default `:9101`).

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-f, --frequency` | `1m` | Scan interval |
| `--label` | `name` | Prometheus label key used to identify each folder |
| `--listen-addr` | `:9101` | Address to expose metrics on |
| `--metric-name` | `folder_size_bytes` | Gauge name |
| `--du-timeout` | `5m` | Per-folder `du` timeout |

### Exposed metrics

- `folder_size_bytes{<label>="..."}` — size in bytes
- `folder_size_bytes_scrape_duration_seconds{...}`
- `folder_size_bytes_scrape_success{...}` — `1` on success, `0` on failure
- `folder_size_bytes_last_scrape_timestamp_seconds{...}`

## Releases

Tagged releases (e.g. `0.1.0`) publish Linux amd64/arm64 binaries via GitHub Actions.
