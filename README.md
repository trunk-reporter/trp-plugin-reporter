# Reporter

A [Trunk Recorder Pro](https://github.com/TrunkRecorder/trunk-recorder-pro)
plugin that keeps call, unit and talkgroup history in SQLite and serves
[tr-engine](https://github.com/trunk-reporter/tr-engine)'s API and pages.

Status: under construction (phase 1). The design is in
[docs/superpowers/specs](docs/superpowers/specs) and the plan in
[docs/superpowers/plans](docs/superpowers/plans).

## Settings

| Setting | Default | |
|---|---|---|
| Port | 8092 | |
| Bind address | 127.0.0.1 | |
| Access token | none | When set, clients send it as a bearer token. |
| Keep history for (days) | 0 | 0 keeps everything. |
| Backfill from recorded calls on start | on | |

## Development

```sh
cargo test
cargo run -- --describe > m.json && python .github/check_manifest.py m.json
```

## License

MIT
