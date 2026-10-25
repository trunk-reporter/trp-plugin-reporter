# Changelog

## [0.1.0]

First release (phase 1).

- A SQLite store in the plugin's data folder holds calls, units, talkgroups and
  affiliations, so history survives restarts.
- tr-engine's REST API for the phase 1 subset at `/api/v1/`, with an optional
  bearer access token. Endpoints that are not built yet answer 501
  `not_implemented`.
- A live event stream (SSE) of calls and unit activity.
- tr-engine's web pages at `/`, linked from the plugin's Website link in TRP.
- Backfill: on first start the capture folder's recorded calls are loaded;
  an interrupted backfill resumes on the next start.
- Retention: "Keep history for (days)" prunes old history; 0 keeps everything.
- Serve mode: `reporter serve <data-dir>` serves the stored history without the
  recorder.
- Targets Trunk Recorder Pro plugin SDK 0.5. SDK 0.7 will add what 0.5 lacks
  (system names and sites from `hello`; Task 21 in the plan).
