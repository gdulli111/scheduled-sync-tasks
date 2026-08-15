# scheduled-sync-tasks

A small scheduled job that polls a public data source on a cron, compares it against
saved state, and posts a short summary to a notification endpoint when something new
crosses a configured threshold.

- Runs on GitHub Actions (see `.github/workflows/sync.yml`).
- State is kept in `state.json` and committed back after each run.
- The notification endpoint is provided via the `NTFY_TOPIC` repository secret.
- No API keys required; adjust the threshold in `monitor.py`.
