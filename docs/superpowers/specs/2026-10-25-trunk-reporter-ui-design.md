# Trunk Reporter UI — design

**Date:** 2026-10-25
**Status:** draft for review
**Companion:** `2026-10-24-tr-engine-rust-plugin-design.md` (the plugin, store and API this UI runs on)

## Intent

One web app for everything a person does with recorded and live trunked-radio traffic, served by the
Trunk Reporter binary itself in every mode it runs in (plugin beside a recorder, serve on a store, hub
across sites). It replaces two things at once: tr-dashboard, and tr-engine's set of standalone pages,
which the plugin ships today as 23 separate HTML files.

The same person uses it for three different jobs at different times, and the current state is what
happens when each job got its own page without anything joining them:

- **Live** — what the monitored systems are doing now: what is busy, whether traffic is up or down,
  what is new.
- **History** — go back to an incident and dig around it: trace a radio's conversations, line up the
  talkgroups that lit up together during a coordinated response, play it back in order.
- **Scanner** — listen to what is happening, simply or with a wall display.

The one capability none of the existing pieces has, and the reason History is the centre of this
design, is **several talkgroups on one shared timeline**.

## Goals

1. The three jobs are three modes of one app, and what you are looking at carries between them.
2. A shared timeline that works for five minutes and four talkgroups, and for four hours and
   twenty, without the drawing falling apart at either end.
3. Every view stays usable on a busy multi-site system. Nothing is "fine on a small system".
4. Every feature is reachable when needed and out of the way when not: expand, scroll, filter.
5. Data presentation that is good to look at and does not break on too little or too much data.
6. One visual system, light and dark, with density tuned per mode.
7. Works unchanged against a single-site plugin, a serve-mode store, and a hub, and shows the
   right subset of itself by asking the server what it can do. The Go tr-engine is not a target:
   the legacy stack is in maintenance mode and keeps the old tr-dashboard releases.

## Non-goals

- Recorder administration. Radios, sources, systems and plugins are TRP's job; the app links out.
- Transcription features as a requirement. They appear when the server reports them and are
  absent, not broken, when it does not.
- A standalone deployment story. The app is embedded in the binary; the TRP "interface" packaging
  is a possible later wrapper, not a target.
- Rebuilding tr-dashboard's call list, call detail, talkgroup and unit pages from scratch. They
  are kept and restyled.

## Anti-goals (the finished thing must not have these)

- Helper text that reads as machine-written. Labels, not blurbs. Tooltips of a few words. No
  paragraphs explaining what a page is for.
- Unformatted tables. Every tabular view has alignment, number formatting, sticky headers,
  column intent, and a defined behaviour at 0, 1, 50 and 5,000 rows.
- Jargon for the app's own parts. Modes are **Live**, **History**, **Scanner**. The thing that
  carries between them is **the selection**. Nothing is called a spine, a workspace or an
  investigation.
- A feature that is overwhelmed by a busy system: an unbounded list, a chart that turns to noise, a
  stream that falls behind. Every view names its ceiling and what happens past it.
- A feature buried on a large system: a talkgroup that cannot be found because the list was cut at
  the top N, a site hidden behind a tab nobody opens. Cut for display, never for reach.
- Skeuomorphic scanner hardware. No rubber keypads, LCD fonts or knob graphics.

## Constraints

- **Stack:** React 19, TypeScript strict, Vite 7, Tailwind 4, the shadcn/Radix primitives
  tr-dashboard already uses. Types generated from the OpenAPI document (`openapi-typescript`).
  Charts and the timeline in D3 or hand-drawn SVG/Canvas; no charting framework that owns layout.
- **Lives in** the tr-dashboard repository, which becomes this app (rename is the owner's call).
  The reporter's CI pulls the built `dist` from a pinned tag and embeds it; the Rust build never
  runs Node.
- **Served at `/`** by the reporter in plugin, serve and hub modes. The classic pages that survive
  (see "What is kept") are rebuilt as views; the HTML files retire from the embed once their views
  ship. Until then they stay under `/classic/`.
- **API:** tr-engine's OpenAPI contract as the plugin serves it, plus the additions in "API
  additions". Same origin, so no CORS is needed for the app itself (CORS is still added for
  third-party front ends, as a settings allowlist).
- **Capabilities:** `GET /health` gains a `features` object (`transcriptions`, `live_audio`, `hub`,
  `recorders`, `call_groups`, `directory`, `users`). The app gates whole sections on it. Nothing
  is shown that the server cannot answer.
- **Token:** when the server has a token set, the app asks for it once, keeps it in the browser,
  sends it as a bearer header on fetches and as the `token` query parameter on the event stream
  and the audio socket, which the plugin already accepts. There is no login page on a plugin.
- **Preferences on the server:** colour rules, favourites, monitored talkgroups, channel
  priorities and mutes are stored by the server (`GET/PUT /preferences/{key}`, a JSON document per
  key, no users on a plugin so one set per server), with the browser copy as a cache. A link
  opened on another machine, and Wall mode on a second screen, see the same colours and channels.
- **Tests:** Vitest for the API client, the selection logic and the timeline's layout maths;
  one Playwright smoke run against a real plugin store that opens every view at two data sizes
  (an empty store and the reference capture) and fails on console errors or blank panels.

## The selection

What you are looking at, carried across modes and encoded in the URL so a link reproduces it on
the server the link points at:

- a **time window** (start, end; or "live" meaning a trailing window that follows now),
- a set of **talkgroups**, each with its system,
- optionally a **radio** being followed,
- the **recorder** filter, when the server is a hub with several installations.

A talkgroup is keyed by the server's `system_id` and the talkgroup id. On a hub, a radio system
heard at two installations is one system when its P25 system id matches (else by short name), the
installation is a `recorder` attribute on its calls, and copies of one transmission heard at two
installations are merged by the hub before they reach the API. The app never de-duplicates. (The
hub is specified separately; this is the contract the app assumes of it.) "Site" keeps its API
meaning, an RF site of a system, and is never used for an installation.

Live sets the selection from what is busy now. History widens it into the past. Scanner narrows
it to the live window and plays it. Switching modes never clears it; clearing is an explicit
action with an undo.

## History

### The shared timeline

A horizontal time axis across the top; one **lane** per selected talkgroup; the lane shows that
talkgroup's calls in the window.

**Two drawings, chosen by pixel density, not by a mode switch.** Each lane computes pixels per
second from the window and the width:

- **Transmissions** (roughly ≥ 4 px per second): each transmission is a bar at its start and
  length, coloured by the radio that keyed up, so a dispatcher and three units read as four
  colours across lanes. Hovering names the radio; clicking opens the call.
- **Density** (below that): the lane is a band shaded by calls per minute, from the server's
  activity buckets, with emergencies, affiliation changes and first-heard radios as marks that
  survive any zoom. Dragging across a band zooms to that span.

Zoom is continuous (wheel, pinch, drag-select, keyboard), and the drawing switches when the
density crosses the threshold. Selection, playhead and the open call persist across zoom.

**Lanes come from three places:**

- added by hand (search by id or name, with the system shown);
- **active in this window** — one action adds the talkgroups that had calls in the window,
  busiest first, capped at the lane ceiling with the rest in a drawer;
- **follow a radio** — pick a unit and the lanes it talked on in the window appear, with its
  transmissions highlighted across all of them.

**Lane ceiling.** Twenty lanes drawn; beyond that, lanes fold into a "more" drawer sorted by
activity, each one promotable with a click, and the folded lanes still count in the merged
playback if enabled. The window ceiling is seven days; longer spans are asked for in pages.

**Marks on lanes:** emergency (always visible), affiliation join/leave of a followed radio (from
the radio's events), first time a radio was heard on this talkgroup (a transmission whose radio's
`first_seen` on that talkgroup, from the talkgroup's units, falls inside the window), encrypted
calls as hollow bars.

**Playhead and merged playback.** Play runs the calls of all drawn lanes in time order, skipping
gaps longer than a set threshold (default two seconds), with the current bar highlighted and the
lane name and radio shown in the player. Space pauses, arrows step calls, numbers set speed.

**The call panel.** Clicking a bar opens the call beside the timeline: transmissions with radio
names, length, frequency, signal, the radio's recent events, and transcription text when the
server reports it. It does not navigate away. Esc closes it.

**Limits and behaviour under load.** The transmissions drawing is capped at 5,000 bars in view;
past that it degrades to density for the lanes that exceed it, lane by lane, rather than all at
once. Data loads per lane and per visible span: the transmissions drawing from the call list
filtered to the lane and span, the density drawing from the activity endpoint's buckets. A
twenty-lane, four-hour window is one bucket request, not a call list of ten thousand rows. The
app never bins calls itself, and the first release does not run against a server without the
activity endpoint: the plugin ships it first.

### Kept views

From tr-dashboard, restyled and wired to the selection: **Calls** (list with filters, inline
playback), **Call detail**, **Talkgroups** (directory with stats, colour rules, monitor and
favourite), **Talkgroup detail** (now the single-lane case of the timeline plus its stats),
**Units** and **Unit detail** (with "follow in timeline"). From the classic set: **Unit tracker**'s
affiliation view becomes a panel in Unit detail; **Talkgroup research**'s unit cross-reference
becomes a panel in Talkgroup detail; **Call history** is replaced by Calls.

## Live

A single screen meant to be left open and read from across the room.

- **Systems and sites** as cards with a health strip: control channel, decode rate, recorder
  state, from `/systems` and `/recorders`, refreshed on the event stream.
- **Busy now:** talkgroups active in the trailing window (default ten minutes), each a tile
  sized or shaded by calls, with the colour rule the user set. Tiles say up or down against the
  **baseline**: the same hour on the previous N days (default seven), from a server endpoint,
  shown as a small arrow and a muted number, never a bare percentage without context.
- **First heard:** talkgroups and radios seen for the first time in the last 24 hours, newest
  first, each one click from its detail. Capped at display, scrollable, never truncated at the
  data level.
- **Activity over the last hours** per system as a small chart with the baseline band behind it.

**On a large system:** tiles show the top N by activity with a search box always visible; a
category filter (from talkgroup tags) folds the grid; nothing is unreachable. Event-stream
updates are coalesced to one repaint per second so a busy system does not make the screen
flicker.

Everything on this screen is one click into History with that window and that lane, or into
Scanner with that talkgroup selected.

## Scanner

Listening, driven by the selection.

- **Channels** are the selected talkgroups; a channel list with search, tag filter, and the
  colour rule, with per-channel mute and priority.
- **Live audio** over the plugin's WebSocket when `features.live_audio` is true; otherwise plays
  finished calls as they conclude, with a clear label that this is delayed by the call length.
  The socket's frames are a 14-byte header (system id, talkgroup, milliseconds since connect,
  sequence, sample rate) and 16-bit PCM, 8 kHz from the recorder; the plugin sends PCM only, so
  decoding is an AudioWorklet with no codec. Frames carry no call or radio id: the radio shown in
  Now playing comes from the event stream's call start and update events for that talkgroup, as
  OmniTrunker does today. Priority and the busy policy apply per talkgroup on live audio and per
  call on delayed playback.
- **Now playing** shows talkgroup, radio, elapsed, signal; a **recent** list below it; clicking an
  entry replays it.
- **Busy policy:** calls queue by channel priority; the queue depth is visible; past a threshold
  the app skips to live and says how many it skipped, instead of falling behind.
- **Wall mode:** a full-screen layout of the same selection for a second monitor (the OmniTrunker
  successor): active channels large, recent activity scrolling, no controls until the mouse moves.
- Keyboard-first: space, number keys for channels, M to mute, arrows to move.

## Visual system

- **Type:** one UI face, one monospace for ids, frequencies and times. Three sizes per mode.
- **Colour:** a neutral surface palette, light and dark; talkgroup colour comes from the user's
  rules (kept from tr-dashboard); radio colours in the timeline from a fixed categorical palette
  assigned per window, with a legend. Status colours (emergency, encrypted, error) reserved and
  never reused for decoration.
- **Density:** Live is calm and large; History is dense and compact; Scanner sits between, with
  Wall mode large again.
- **Data presentation rules:** every number has a unit or a context; durations as `m:ss`; counts
  with thin-space grouping; times in the user's zone with the date only when it changes; charts
  with baselines where a baseline exists; empty states are one line and an action, not a graphic.
- **Copy rules:** labels, not sentences; no "This page shows…"; tooltips under eight words; error
  messages say what happened and what to do, in one line.
- **Tables:** sticky header, right-aligned numbers, monospace ids, hover row, row action menu,
  virtualised past 200 rows, server pagination past 1,000, column visibility toggles, and a
  defined empty row.

## API additions (reporter side)

Already in the plugin's plan as phase 2 or named in this design; all additive to the OpenAPI
contract:

| Endpoint | For | Notes |
|---|---|---|
| `GET /health` `features` | capability gating | `transcriptions`, `live_audio`, `hub`, `sites` |
| `GET /stats/talkgroup-activity` | density lanes, Live's activity charts | extended additively: `start`, `end`, `interval` (10, 30, 60, 300, 900, 3600 s), optional `tgids` and `system_id`; returns one series per talkgroup, `[bucket_start, calls, seconds]`, capped at 10,000 buckets per request. Without `interval` it answers as the spec does today |
| `GET /talkgroups/active?start&end` | "active in this window" | ordered by calls, with counts |
| `GET /units/{id}/talkgroups?start&end` | "follow a radio" | talkgroups the unit talked on, with counts |
| `GET /talkgroups/{id}/units` `first_seen` | first-heard marks | per unit, first time heard on that talkgroup (additive field) |
| `GET /first-heard?since` | Live's first-heard list | talkgroups and units with first-seen in range |
| `GET /stats/baseline?window&days&system_id` | up/down indicator | one request: for every talkgroup active in the trailing `window` seconds, the mean calls in the same clock window on the previous `days` days; equal widths, never a minute against an hour |
| `GET/PUT /preferences/{key}` | colour rules, favourites, monitors, priorities | JSON document per key, in the store |
| `WS /audio/live` | Scanner | phase 2 live audio; frame format above |
| CORS allowlist setting | third-party front ends | not used by the app itself |

The hub carries the same endpoints with a `site` filter.

## Behaviour at the edges

Every view defines, and the smoke test checks, what it shows at:

- **nothing** (empty store): one-line empty states, no spinners that never end;
- **one** (one system, one talkgroup, one call): no layout that assumes a grid;
- **many** (reference capture, 17k calls, 180 talkgroups, 1,600 units): fast and complete;
- **too many** (a hub of several county systems): the named ceilings apply, with the "more"
  path visible.

## What is kept, what retires

| Today | Becomes |
|---|---|
| tr-dashboard Dashboard | Live |
| tr-dashboard Calls, Call detail, Talkgroups, Units and details | History views, restyled |
| classic timeline ("Drift"), call-history | the shared timeline, Calls |
| classic unit-tracker, talkgroup-research | panels in Unit detail and Talkgroup detail |
| classic scanner, scanner-classic, OmniTrunker, IRC | Scanner and Wall mode |
| classic events page | a debug view under Settings |
| classic docs, playground | docs link to the served OpenAPI; playground retires |
| classic exports | export actions on the Talkgroups and Units tables |
| classic charts (bubble, heatmap, sunburst, stream graph, daily overview, gauges) | not carried; Live's activity charts cover the need, and any that are missed can return as History panels |
| tr-dashboard Investigate | replaced by the shared timeline |
| tr-dashboard Talkgroup analytics | panels in Talkgroup detail |
| tr-dashboard Affiliations | a History table view, wired to the selection |
| tr-dashboard Systems, System detail, Recorders | Live's system cards, plus System detail kept in History |
| tr-dashboard alert engine and emergency notifications | kept in Live as browser notifications |
| tr-dashboard Transcriptions, Call groups, Talkgroup directory, Unit tag suggestions | gated on `features`; absent on a plugin. Call groups is the one to revive first if incident grouping is wanted in History |
| tr-dashboard Admin, Users, Login | gated on `features`; absent on a plugin |

## Left to the builder

- Exact lane heights, zoom curve and the pixel-density threshold, tuned on the reference capture.
- The radio colour assignment order and legend placement.
- Whether the call panel is a side sheet or a bottom drawer at narrow widths.
- Bucket interval choice per zoom level (the server accepts an interval parameter).
- Keyboard map details beyond those named.

## Open questions (for the owner)

Settled by the conversation, listed so they can be checked while reading:

- Mode names are Live, History, Scanner; the carried state is "the selection". My call on the
  latter; change it if a better plain word exists.
- The app lives in the tr-dashboard repository and the reporter embeds its build. My call.
- Classic chart pages are not carried. My call, reversible per page.
- One system across installations is keyed by P25 system id, and the hub merges duplicate
  transmissions before the API. My call; the hub's own design will restate it.
- Preferences live on the server. My call, from the owner's stated preference for durable
  storage.
- The fates of the unmentioned tr-dashboard pages are in "What is kept"; Call groups is the one
  worth a second look.
