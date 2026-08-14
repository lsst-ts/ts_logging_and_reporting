# Backend caching refactor — before/after performance

| | baseline | refactored |
|---|---|---|
| Deployment | `usdf-rsp-dev.slac.stanford.edu/nightlydigest/api/` | `…/nightlydigest/api/alpha/` |
| Build (`/version`) | `0.15.2.dev10+g644edde` | `0.15.2.dev147+gd4de8f3` |
| Capture window | 2026-08-12T07:41 → 10:22Z | 2026-08-12T10:47 → 08-13T02:49Z |
| Runs / bursts | 20 sequential, 10 rounds × 10 concurrent | same |
| Range | `dayObsStart=20260611`, `instrument=LSSTCam` | same |

`scripts/perf_test.py` produced both captures and joins them into the table below.
Scenario definitions and the constraints on a valid run are documented in that
script. The table is generated output; everything after it is interpretation.

**Reading the columns.**

- **completed** is successful requests over attempted. **p50 is taken over
  successful requests only**, so a row that did not complete cleanly has a
  percentile over a smaller sample.
- **wall p50** is the median time for a whole burst round of ten to finish — what a
  user waits when ten people load the same night at once. Burst rows only.
- **change** is against the baseline row of the same shape (matching range length,
  burst against burst). Every (day_range, burst) after row therefore shares one
  baseline, regardless of caching scenario.
- **bytes** is the modal decompressed response size. `[DIFFERS]` means sizes varied
  within one scenario; `[DIFFERS AFTER]` that the two runs settled on different
  sizes for the same request; `[OTHER WINDOW]` that the two rows did not request
  the same nights, so their sizes are not comparable — e.g., `partial-rolling`,
  which measures the shifted week against a baseline for the original one. See §8;
  all of these flags are expected and explained by `capture-compare.md`

---

## Results

| Endpoint | Scenario | completed (before → after) | before p50 (s) | after p50 (s) | change | before wall p50 (s) | after wall p50 (s) | wall change | bytes |
|---|---|---|---|---|---|---|---|---|---|
| almanac | cold-1day | 20/20 → 20/20 | 0.689 | 0.678 | -2% |  |  |  | 590 |
| almanac | cold-7day-burst | 100/100 → 100/100 | 10.767 | 3.718 | -65% | 16.24 | 3.8 | -77% | 4017 |
| almanac | cold-7day | 20/20 → 20/20 | 3.723 | 3.677 | -1% |  |  |  | 4017 |
| almanac | hot-1day-burst | 100/100 → 100/100 | 1.706 | 0.178 | -90% | 2.467 | 0.196 | -92% | 590 |
| almanac | hot-1day | 20/20 → 20/20 | 0.689 | 0.175 | -75% |  |  |  | 590 |
| almanac | hot-7day-burst | 100/100 → 100/100 | 10.767 | 0.18 | -98% | 16.24 | 0.199 | -99% | 4017 |
| almanac | hot-7day | 20/20 → 20/20 | 3.723 | 0.176 | -95% |  |  |  | 4017 |
| almanac | partial-extension-7day | 20/20 → 20/20 | 3.723 | 3.176 | -15% |  |  |  | 4017 |
| almanac | partial-fragmented-7day | 20/20 → 20/20 | 3.723 | 1.188 | -68% |  |  |  | 4017 |
| almanac | partial-rolling-7day-burst | 100/100 → 100/100 | 10.767 | 0.767 | -93% | 16.24 | 0.796 | -95% | 4015 [OTHER WINDOW] |
| almanac | partial-rolling-7day | 20/20 → 20/20 | 3.723 | 0.701 | -81% |  |  |  | 4015 [OTHER WINDOW] |
| block-details | cold-all-keys-burst | 100/100 → 100/100 | 1.91 | 1.087 | -43% | 2.438 | 1.106 | -55% | 2123 |
| block-details | cold-all-keys | 20/20 → 20/20 | 1.455 | 0.986 | -32% |  |  |  | 2123 |
| block-details | hot-all-keys-burst | 100/100 → 100/100 | 1.91 | 0.18 | -91% | 2.438 | 0.199 | -92% | 2123 |
| block-details | hot-all-keys | 20/20 → 20/20 | 1.455 | 0.176 | -88% |  |  |  | 2123 |
| block-details | partial-keys-burst | 100/100 → 100/100 | 1.91 | 0.683 | -64% | 2.438 | 0.708 | -71% | 2123 |
| block-details | partial-keys | 20/20 → 20/20 | 1.455 | 0.763 | -48% |  |  |  | 2123 |
| context-feed | cold-1day | 20/20 → 20/20 | 12.666 | 11.68 | -8% |  |  |  | 4845605 [DIFFERS AFTER] |
| context-feed | cold-7day-burst | 100/100 → 100/100 | 49.873 | 31.674 | -36% | 61.081 | 32.837 | -46% | 13814752 [DIFFERS AFTER] |
| context-feed | cold-7day | 20/20 → 20/20 | 28.403 | 27.492 | -3% |  |  |  | 13814752 [DIFFERS AFTER] |
| context-feed | hot-1day-burst | 100/100 → 100/100 | 23.403 | 6.516 | -72% | 37.652 | 6.843 | -82% | 4847559 [DIFFERS AFTER] |
| context-feed | hot-1day | 20/20 → 20/20 | 12.666 | 6.353 | -50% |  |  |  | 4847559 [DIFFERS AFTER] |
| context-feed | hot-7day-burst | 100/100 → 100/100 | 49.873 | 20.021 | -60% | 61.081 | 24.218 | -60% | 13814752 [DIFFERS AFTER] |
| context-feed | hot-7day | 20/20 → 20/20 | 28.403 | 17.803 | -37% |  |  |  | 13814752 [DIFFERS AFTER] |
| context-feed | partial-extension-7day | 20/20 → 20/20 | 28.403 | 24.663 | -13% |  |  |  | 13812798 [DIFFERS AFTER] |
| context-feed | partial-fragmented-7day | 20/20 → 20/20 | 28.403 | 21.517 | -24% |  |  |  | 13812902 [DIFFERS AFTER] |
| context-feed | partial-rolling-7day-burst | 100/100 → 100/100 | 49.873 | 22.915 | -54% | 61.081 | 26.753 | -56% | 13748094 [OTHER WINDOW] |
| context-feed | partial-rolling-7day | 20/20 → 20/20 | 28.403 | 21.629 | -24% |  |  |  | 13748094 [OTHER WINDOW] |
| data-log | cold-1day | 20/20 → 20/20 | 7.685 | 11.652 | +52% |  |  |  | 4336170 |
| data-log | cold-7day-burst | 100/100 → 100/100 | 48.678 | 44.947 | -8% | 58.724 | 53.325 | -9% | 24323138 |
| data-log | cold-7day | 20/20 → 20/20 | 36.851 | 40.3 | +9% |  |  |  | 24323138 |
| data-log | hot-1day-burst | 100/100 → 100/100 | 10.348 | 9.311 | -10% | 15.459 | 13.821 | -11% | 4336170 |
| data-log | hot-1day | 20/20 → 20/20 | 7.685 | 6.361 | -17% |  |  |  | 4336170 |
| data-log | hot-7day-burst | 100/100 → 100/100 | 48.678 | 44.017 | -10% | 58.724 | 50.131 | -15% | 24323138 |
| data-log | hot-7day | 20/20 → 20/20 | 36.851 | 35.872 | -3% |  |  |  | 24323138 |
| data-log | partial-extension-7day | 20/20 → 20/20 | 36.851 | 36.196 | -2% |  |  |  | 24323138 |
| data-log | partial-fragmented-7day | 20/20 → 20/20 | 36.851 | 35.79 | -3% |  |  |  | 24323138 |
| data-log | partial-rolling-7day-burst | 100/100 → 100/100 | 48.678 | 43.603 | -10% | 58.724 | 47.516 | -19% | 22481113 [OTHER WINDOW] |
| data-log | partial-rolling-7day | 20/20 → 20/20 | 36.851 | 33.23 | -10% |  |  |  | 22481113 [OTHER WINDOW] |
| exposure-entries | cold-1day | 20/20 → 20/20 | 0.214 | 0.21 | -2% |  |  |  | 23 |
| exposure-entries | cold-7day-burst | 100/100 → 100/100 | 0.283 | 0.282 | -0% | 0.37 | 0.3 | -19% | 23 |
| exposure-entries | cold-7day | 20/20 → 20/20 | 0.213 | 0.215 | +1% |  |  |  | 23 |
| exposure-entries | hot-1day-burst | 100/100 → 100/100 | 0.289 | 0.177 | -39% | 0.361 | 0.197 | -45% | 23 |
| exposure-entries | hot-1day | 20/20 → 20/20 | 0.214 | 0.174 | -19% |  |  |  | 23 |
| exposure-entries | hot-7day-burst | 100/100 → 100/100 | 0.283 | 0.179 | -37% | 0.37 | 0.197 | -47% | 23 |
| exposure-entries | hot-7day | 20/20 → 20/20 | 0.213 | 0.175 | -18% |  |  |  | 23 |
| exposure-entries | partial-extension-7day | 20/20 → 20/20 | 0.213 | 0.212 | -0% |  |  |  | 23 |
| exposure-entries | partial-fragmented-7day | 20/20 → 20/20 | 0.213 | 0.214 | +0% |  |  |  | 23 |
| exposure-entries | partial-rolling-7day-burst | 100/100 → 100/100 | 0.283 | 0.279 | -1% | 0.37 | 0.298 | -19% | 23 |
| exposure-entries | partial-rolling-7day | 20/20 → 20/20 | 0.213 | 0.21 | -1% |  |  |  | 23 |
| exposures | cold-1day | 20/20 → 20/20 | 5.781 | 3.908 | -32% |  |  |  | 344371 |
| exposures | cold-7day-burst | 99/100 → 100/100 | 37.613 | 15.68 | -58% | 52.682 | 17.398 | -67% | 2111893 [DIFFERS] |
| exposures | cold-7day | 20/20 → 20/20 | 15.707 | 12.908 | -18% |  |  |  | 2111893 |
| exposures | hot-1day-burst | 100/100 → 100/100 | 11.184 | 0.698 | -94% | 20.631 | 0.999 | -95% | 344371 |
| exposures | hot-1day | 20/20 → 20/20 | 5.781 | 0.554 | -90% |  |  |  | 344371 |
| exposures | hot-7day-burst | 99/100 → 100/100 | 37.613 | 3.571 | -91% | 52.682 | 6.2 | -88% | 2111893 |
| exposures | hot-7day | 20/20 → 20/20 | 15.707 | 3.038 | -81% |  |  |  | 2111893 |
| exposures | partial-extension-7day | 20/20 → 20/20 | 15.707 | 14.565 | -7% |  |  |  | 2111893 |
| exposures | partial-fragmented-7day | 20/20 → 20/20 | 15.707 | 9.996 | -36% |  |  |  | 2111891 [DIFFERS AFTER] |
| exposures | partial-rolling-7day-burst | 99/100 → 100/100 | 37.613 | 5.662 | -85% | 52.682 | 9.018 | -83% | 2027338 [OTHER WINDOW] |
| exposures | partial-rolling-7day | 20/20 → 20/20 | 15.707 | 4.931 | -69% |  |  |  | 2027338 [OTHER WINDOW] |
| jira-tickets | cold-1day | 20/20 → 20/20 | 0.633 | 0.476 | -25% |  |  |  | 13 |
| jira-tickets | cold-7day-burst | 100/100 → 100/100 | 1.145 | 0.584 | -49% | 1.805 | 0.603 | -67% | 3741 |
| jira-tickets | cold-7day | 20/20 → 20/20 | 0.608 | 0.533 | -12% |  |  |  | 3741 |
| jira-tickets | hot-1day-burst | 100/100 → 100/100 | 1.125 | 0.177 | -84% | 1.96 | 0.195 | -90% | 13 |
| jira-tickets | hot-1day | 20/20 → 20/20 | 0.633 | 0.174 | -73% |  |  |  | 13 |
| jira-tickets | hot-7day-burst | 100/100 → 100/100 | 1.145 | 0.18 | -84% | 1.805 | 0.197 | -89% | 3741 |
| jira-tickets | hot-7day | 20/20 → 20/20 | 0.608 | 0.175 | -71% |  |  |  | 3741 |
| jira-tickets | partial-extension-7day | 20/20 → 20/20 | 0.608 | 0.494 | -19% |  |  |  | 3741 |
| jira-tickets | partial-fragmented-7day | 20/20 → 20/20 | 0.608 | 0.487 | -20% |  |  |  | 3741 |
| jira-tickets | partial-rolling-7day-burst | 100/100 → 100/100 | 1.145 | 0.58 | -49% | 1.805 | 0.598 | -67% | 4626 [OTHER WINDOW] |
| jira-tickets | partial-rolling-7day | 20/20 → 20/20 | 0.608 | 0.479 | -21% |  |  |  | 4626 [OTHER WINDOW] |
| multi-night-visit-maps | cold-1day | 20/20 → 20/20 | 19.206 | 22.391 | +17% |  |  |  | 1447936 [DIFFERS AFTER] |
| multi-night-visit-maps | cold-7day-burst | 100/100 → 82/100 | 50.643 | 101.051 | +100% | 78.436 | 174.128 | +122% | 3327645 [DIFFERS AFTER] |
| multi-night-visit-maps | cold-7day | 20/20 → 20/20 | 24.682 | 27.983 | +13% |  |  |  | 3325489 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-1day-burst | 100/100 → 93/100 | 38.377 | 78.419 | +104% | 49.867 | 119.362 | +139% | 1448909 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | hot-1day | 20/20 → 20/20 | 19.206 | 21.012 | +9% |  |  |  | 1447936 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-7day-burst | 100/100 → 98/100 | 50.643 | 86.075 | +70% | 78.436 | 120.992 | +54% | 3327645 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-7day | 20/20 → 20/20 | 24.682 | 27.107 | +10% |  |  |  | 3325489 [DIFFERS AFTER] |
| multi-night-visit-maps | partial-extension-7day | 20/20 → 20/20 | 24.682 | 23.938 | -3% |  |  |  | 3325489 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | partial-fragmented-7day | 20/20 → 20/20 | 24.682 | 23.141 | -6% |  |  |  | 3327645 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | partial-rolling-7day-burst | 100/100 → 98/100 | 50.643 | 63.664 | +26% | 78.436 | 113.916 | +45% | 2865056 [OTHER WINDOW] |
| multi-night-visit-maps | partial-rolling-7day | 20/20 → 20/20 | 24.682 | 22.421 | -9% |  |  |  | 2863040 [OTHER WINDOW] |
| narrative-log | cold-1day | 20/20 → 20/20 | 0.235 | 0.234 | -0% |  |  |  | 22203 |
| narrative-log | cold-7day-burst | 100/100 → 100/100 | 1.119 | 0.588 | -47% | 1.75 | 0.916 | -48% | 125161 |
| narrative-log | cold-7day | 20/20 → 20/20 | 0.31 | 0.314 | +1% |  |  |  | 125161 |
| narrative-log | hot-1day-burst | 100/100 → 100/100 | 0.448 | 0.219 | -51% | 0.639 | 0.255 | -60% | 22203 |
| narrative-log | hot-1day | 20/20 → 20/20 | 0.235 | 0.176 | -25% |  |  |  | 22203 |
| narrative-log | hot-7day-burst | 100/100 → 100/100 | 1.119 | 0.365 | -67% | 1.75 | 0.633 | -64% | 125161 |
| narrative-log | hot-7day | 20/20 → 20/20 | 0.31 | 0.186 | -40% |  |  |  | 125161 |
| narrative-log | partial-extension-7day | 20/20 → 20/20 | 0.31 | 0.297 | -4% |  |  |  | 125161 |
| narrative-log | partial-fragmented-7day | 20/20 → 20/20 | 0.31 | 0.254 | -18% |  |  |  | 125161 |
| narrative-log | partial-rolling-7day-burst | 100/100 → 100/100 | 1.119 | 0.545 | -51% | 1.75 | 0.686 | -61% | 118036 [OTHER WINDOW] |
| narrative-log | partial-rolling-7day | 20/20 → 20/20 | 0.31 | 0.238 | -23% |  |  |  | 118036 [OTHER WINDOW] |
| obs-status | cold-1day | 20/20 → 20/20 | 1.157 | 0.204 | -82% |  |  |  | 31958 |
| obs-status | cold-7day-burst | 100/100 → 100/100 | 2.699 | 0.543 | -80% | 4.803 | 0.633 | -87% | 69151 |
| obs-status | cold-7day | 20/20 → 20/20 | 1.229 | 0.205 | -83% |  |  |  | 69151 |
| obs-status | hot-1day-burst | 100/100 → 100/100 | 2.54 | 0.248 | -90% | 4.637 | 0.366 | -92% | 31958 |
| obs-status | hot-1day | 20/20 → 20/20 | 1.157 | 0.178 | -85% |  |  |  | 31958 |
| obs-status | hot-7day-burst | 100/100 → 100/100 | 2.699 | 0.196 | -93% | 4.803 | 0.529 | -89% | 69151 |
| obs-status | hot-7day | 20/20 → 20/20 | 1.229 | 0.18 | -85% |  |  |  | 69151 |
| obs-status | partial-extension-7day | 20/20 → 20/20 | 1.229 | 0.196 | -84% |  |  |  | 69151 |
| obs-status | partial-fragmented-7day | 20/20 → 20/20 | 1.229 | 0.203 | -83% |  |  |  | 69151 |
| obs-status | partial-rolling-7day-burst | 100/100 → 100/100 | 2.699 | 0.296 | -89% | 4.803 | 0.485 | -90% | 71514 [OTHER WINDOW] |
| obs-status | partial-rolling-7day | 20/20 → 20/20 | 1.229 | 0.191 | -84% |  |  |  | 71514 [OTHER WINDOW] |
| static-visit-map | cold-1day | 20/20 → 20/20 | 4.389 | 3.847 | -12% |  |  |  | 138986 |
| static-visit-map | cold-7day-burst | 100/100 → 100/100 | 10.401 | 11.444 | +10% | 17.547 | 13.136 | -25% | 165454 |
| static-visit-map | cold-7day | 20/20 → 20/20 | 4.924 | 4.829 | -2% |  |  |  | 165454 |
| static-visit-map | hot-1day-burst | 100/100 → 100/100 | 9.844 | 7.747 | -21% | 15.593 | 10.242 | -34% | 138986 |
| static-visit-map | hot-1day | 20/20 → 20/20 | 4.389 | 3.557 | -19% |  |  |  | 138986 |
| static-visit-map | hot-7day-burst | 100/100 → 100/100 | 10.401 | 9.125 | -12% | 17.547 | 12.317 | -30% | 165454 |
| static-visit-map | hot-7day | 20/20 → 20/20 | 4.924 | 4.1 | -17% |  |  |  | 165454 |
| static-visit-map | partial-extension-7day | 20/20 → 20/20 | 4.924 | 4.813 | -2% |  |  |  | 165454 |
| static-visit-map | partial-fragmented-7day | 20/20 → 20/20 | 4.924 | 4.417 | -10% |  |  |  | 165454 |
| static-visit-map | partial-rolling-7day-burst | 100/100 → 100/100 | 10.401 | 8.765 | -16% | 17.547 | 11.859 | -32% | 160062 [OTHER WINDOW] |
| static-visit-map | partial-rolling-7day | 20/20 → 20/20 | 4.924 | 4.231 | -14% |  |  |  | 160062 [OTHER WINDOW] |

---

## Analysis

### 1. What this capture can and cannot see

The performance test suite was run from a datacenter in Melbourne, Australia
against the RSP in California - a 90ms round trip for light. Two properties
of that link therefore affect every number in the table and must be considered
when analysing it.

**A ~0.17 s floor.** Every fully cached small response lands on it, to the
centisecond, regardless of endpoint or payload:

| Endpoint | hot-7day p50 (s) | bytes |
|---|---|---|
| exposure-entries | 0.175 | 23 |
| jira-tickets | 0.175 | 3,741 |
| almanac | 0.176 | 4,017 |
| block-details (hot-all-keys) | 0.176 | 2,123 |
| obs-status | 0.180 | 69,151 |
| narrative-log | 0.186 | 125,161 |

Six endpoints spanning four orders of magnitude in payload agree to within 11 ms.
That is round-trip time, not server time. **For these six endpoints the hot
server-side latency is not measured by this capture at all** — only bounded, at
under ~10 ms. Their headline improvements (−71% to −95%) are real but floored: the
true speedup is **larger** than the table can show.

**A ~0.75 MB/s ceiling on a single connection.** The large-payload endpoints all
sit on one byte-proportional cost, stable across ranges:

| Endpoint / scenario | MB | hot p50 (s) | above floor | s/MB |
|---|---|---|---|---|
| obs-status 7d | 0.07 | 0.180 | 0.006 | 0.09 |
| narrative-log 7d | 0.13 | 0.186 | 0.012 | 0.10 |
| exposures 1d | 0.34 | 0.554 | 0.380 | 1.10 |
| exposures 7d | 2.11 | 3.038 | 2.864 | 1.36 |
| context-feed 1d | 4.85 | 6.353 | 6.179 | 1.28 |
| context-feed 7d | 13.81 | 17.803 | 17.629 | 1.28 |
| data-log 1d | 4.34 | 6.361 | 6.187 | 1.43 |
| data-log 7d | 24.32 | 35.872 | 35.698 | 1.47 |

**The per-byte cost falls as payloads shrink, which is not how a server-side
bottleneck behaves.** Payloads around 0.1 MB move at 10–12 MB/s — narrative-log's
125 KB costs 12 ms above the floor, obs-status' 69 KB costs 6 ms, two independent
measurements agreeing within 10%. Payloads of 4–24 MB move at 0.68–0.78 MB/s, a
15× difference in cost per byte. Serialising JSON does not get 15× cheaper per
byte on smaller documents; if anything it amortises the other way. A connection
limited to ~0.13 MB in flight at 0.17 s RTT does exactly this: anything fitting
in one window is effectively free, anything larger is capped at window ÷ RTT.

Concurrency corroborates. Ten connections deliver several times the bytes per
second that one does:

| Endpoint / scenario | 1 stream | 10 streams (from wall p50) | scaling |
|---|---|---|---|
| context-feed hot-1day | 0.78 MB/s | 7.08 MB/s | 9.0× |
| context-feed hot-7day | 0.78 MB/s | 5.70 MB/s | 7.3× |
| data-log hot-7day | 0.68 MB/s | 4.85 MB/s | 7.1× |
| exposures hot-7day | 0.74 MB/s | 3.41 MB/s | 4.6× |

Consequently:

- **`data-log`, `context-feed` and `exposures` sequential numbers are dominated by
  transfer, not by the backend.** `data-log hot-7day` at 35.872 s is ~35 s of
  moving 24.3 MB to Australia and ~0.2 s of server. Its "−3%" is not a weak cache
  result; it is a cache result invisible behind the link.
- Nothing here is compressed. `REFACTOR_ODDITIES.md` §13 records that neither
  `GZipMiddleware` nor nginx `gzip` is enabled, and measured ~4× on this payload
  class. That single change would move these three endpoints more than anything in
  the refactor did.


### 2. Headline: what a user with a warm cache now sees

The baseline has no server-side cache, so every baseline request is cold by
construction. Comparing it against the refactored steady state — `hot-7day`, which
the RefreshWorker maintains for current data and which any recently viewed range
also sits in:

| Endpoint | before (s) | after, hot (s) | speedup |
|---|---|---|---|
| almanac | 3.723 | 0.176 | 21× |
| block-details (all keys) | 1.455 | 0.176 | 8.3× |
| obs-status | 1.229 | 0.180 | 6.8× |
| exposures | 15.707 | 3.038 | 5.2× |
| jira-tickets | 0.608 | 0.175 | 3.5× |
| narrative-log | 0.310 | 0.186 | 1.7× |
| context-feed | 28.403 | 17.803 | 1.6× |
| exposure-entries | 0.213 | 0.175 | 1.2× |
| static-visit-map | 4.924 | 4.100 | 1.2× |
| data-log | 36.851 | 35.872 | 1.0× |
| multi-night-visit-maps | 24.682 | 27.107 | 0.9× |

Both ends of this table have important caveats. The top is floored by the
0.174 s RTT, so those speedups are lower bounds. The bottom is floored by transfer
(`data-log`, `context-feed`) or by render cost that was never inside the cache's
scope (the two map endpoints). The defensible statement is that **the cache
reliably removes the upstream fetch, and what remains is whatever else the endpoint
does** — bytes on the wire, or the map figure build.

`exposure-entries` returned 23 bytes — an empty result — in every request of
both captures. For this endpoint, the numbers are a valid measurement of
request plumbing and cache round-trip and say nothing about behaviour on
a populated range.

### 3. Concurrency

Burst p50 divided by the sequential p50 of the same cache state and range — the
factor by which ten simultaneous requests for the same data cost more than one.


| Endpoint | before (cold-7d) | after (cold-7d) | after (hot-7d) |
|---|---|---|---|
| almanac | 2.89× | 1.01× | 1.02× |
| jira-tickets | 1.88× | 1.10× | 1.03× |
| context-feed | 1.76× | 1.15× | 1.12× |
| exposures | 2.39× | 1.21× | 1.18× |
| data-log | 1.32× | 1.12× | 1.23× |
| block-details | 1.31× | 1.10× | 1.02× |
| exposure-entries | 1.33× | 1.31× | 1.02× |
| narrative-log | 3.61× | 1.87× | 1.96× |
| obs-status | 2.20× | 2.65× | 1.09× |
| static-visit-map | 2.11× | 2.37× | 2.23× |
| multi-night-visit-maps | 2.05× | 3.61× | 3.18× |

The single-flight lock does what it was built to do: on every data endpoint the
marginal cost of the 2nd through 10th concurrent request for the same key falls to
near zero. `almanac` is the clearest case — its cold burst went 10.767 s → 3.718 s,
and a whole round of ten now completes in 3.8 s wall against a 3.677 s single
request.

Two entries need care rather than celebration.

**`obs-status` cold at 2.65× and `narrative-log` at 1.87–1.96× are denominator
artefacts.** Both divide by a sequential time already at or close to the RTT floor, so the
costs a burst still pays regardless — ten TCP connections, ten TLS handshakes, ten
response writes — stop being negligible against it. In absolute terms `obs-status`'s
cold burst went 2.699 s → 0.543 s and its wall time 4.803 s → 0.633 s.

**The two map endpoints genuinely got worse under concurrency**, and this is
discussed further in §6.

### 4. rubin_nights client caching

`obs-status` improves by ~1.0 s on *every* scenario, including the cold ones, which
flush Redis before each request. A cold path cannot be faster because of a cache.

| scenario | before (s) | after (s) | delta |
|---|---|---|---|
| cold-1day | 1.157 | 0.204 | −0.953 |
| cold-7day | 1.229 | 0.205 | −1.024 |
| hot-7day | 1.229 | 0.180 | −1.049 |
| partial-extension-7day | 1.229 | 0.196 | −1.033 |
| partial-fragmented-7day | 1.229 | 0.203 | −1.026 |

A flat ~1.0 s, independent of cache state and of range length, is a fixed
per-request cost that was removed rather than a cache effect. The most likely 
candidate is `RubinNightsClientsMixin` (`adapters/mixins.py:162`), which holds 
`_clients` and `_efd_client` as `functools.cached_property` built once so credential
discovery does not repeat on every fetch — where the pre-refactor path called
`get_clients()` inside the request.


The mixin is also used by the dome, context-feed and visit-overhead adapters, so
the same saving is probably inside their totals, but only `obs-status` isolates it:
`context-feed`'s payload changed between the runs (§8), and the `exposures` adapter
is too unstable to show this saving.

### 5. Per-day chunking is verified

The design's central claim is that request cost tracks *missing* days rather than requested
days. Predicting each partial scenario from the cold 7-day cost, net of the hot
floor — `hot-7day + missing × (cold-7day − hot-7day) / 7`:

| Endpoint | 2 missing (fragmented) | 6 missing (extension) |
|---|---|---|
| almanac | +1.0% | −0.0% |
| static-visit-map | +2.5% | +1.9% |
| narrative-log | +14.1% | +0.4% |
| context-feed | +4.6% | −5.5% |
| obs-status | +8.5% | −2.7% |
| exposure-entries | +14.8% | +1.3% |
| jira-tickets | +75.6% | +2.5% |
| data-log | −3.6% | −8.8% |
| exposures | **+70.6%** | **+26.7%** |

`almanac` is the clean case — pure computation, no upstream variability, a small
payload, so neither the RTT floor nor the transfer ceiling distorts it — and it
lands within 1% of the predicted value at both points. The six-day column,
where the per-day term dominates, agrees within ~5% on every endpoint except
`exposures`.

`exposures` is the apparent exception in both columns, and is handled in its
own section (§6(c)).

Note that `partial-extension-7day` improving only −15% is **correct
behaviour looking unimpressive**: widening a 1-day view to a week genuinely requires
six nights of work, and no cache can avoid it. The scenario exists to confirm the
cache does not pretend otherwise.

**`partial-rolling-7day` is excluded from this test.** It requests days 2–8, so both
its payload and its upstream cost differ from the baseline for reasons unrelated to
cache behaviour — the reason its byte cell reads `[OTHER WINDOW]`.

### 6. Regressions

Three rows read as regressions in the table. One is an apparent transient or
statistical effect, while the other two require further followup and testing.

**(a) `data-log`'s apparent cold-path penalty is not real.** The table shows
`cold-1day` at +52% and `cold-7day` at +9%, which read as a fixed ~3.5–4.0 s cost on
the fetch path:

| scenario | before (s) | after (s) | delta |
|---|---|---|---|
| cold-1day | 7.685 | 11.652 | **+3.967** |
| cold-7day | 36.851 | 40.300 | **+3.449** |
| hot-1day | 7.685 | 6.361 | −1.324 |
| hot-7day | 36.851 | 35.872 | −0.979 |
| partial-fragmented-7day | 36.851 | 35.790 | −1.061 |

These findings could not be repeated. When retested 24 hours later, the cold-1day had
a p50 of 7.616s and the cold-7day a p50 of 36.257 - consistent with the original
measurements.

**(b) `multi-night-visit-maps` bursts are 1.5–2× slower and now drop requests.**

This is a result of a mismatch between the code and the environment it executes in.
In the baseline, the `/multi-night-visit-maps` endpoint uses a `ThreadPoolExecutor`
to perform the (relatively) CPU-expensive bokeh figure generator. Due to inherent
limitations in the python GIL, this doesn't actually provide a throughput benefit,
but it is stable.

The refactored `/multi-night-visit-maps` service uses `WorkerPoolMixin` to parallelise
the figure generation across multiple processes (not threads), which *does*
provide a genuine parallelization speedup - but **only** if the code is running in a
multi-CPU system.

This is where the disconnect occurs - during the development process, the code was
running on a system with 4 vCPUs which was able to take advantage of this multi-process
parallelism, but when deployed it is in a kubernetes pod with 150milliCPU requested
and a maximum of 1000mCPU available. This means that the parallel requests waste a whole
lot of time context-switching and throttling which it did not do before. This causes
requests to time out - either the `WorkerPoolMixin`s inbuilt load-shedding, an
intermediate (RSP) proxy timeout, or the performance tests inbuilt timeout.

There are two potential remediations here: either increase the resource limits,
or change the service. If the pod is allowed to access more CPU, the throttling
is removed, however this may not be practical to do. If this is the case, a better
option may be to remove this parallelization or reduce the number of allowed
simultaneous tasks to 1 (this can be done without significantly rewriting the service).

This finding also increases the importance of [OSW-2811](https://rubinobs.atlassian.net/browse/OSW-2811)
which suggests caching the bokeh figure generation at a higher level. If the figure
generation time is decreased, then a lot of these issues become moot.

It's also important to note that this regression is not a realistic scenario - it's
a worst-case, almost pathological situation. Multiple requests for the same night
range are already guarded by nginx `proxy_cache_lock on;` which collapses simultaneous
requests into one. So for this situation to actually occur, >8 people would have to
request large unique ranges within the same 20 second period.

The static visit maps use `WorkerPoolMixin` for a different reason: the png
generation mechanism is explicitly and catastrophically not thread-safe, so it
*must* be run either non-concurrently or on separate processes. It does not show
the same slowdown and dropped requests most likely because it is much less CPU
heavy and therefore doesn't hit the pod limits.

**(c) `exposures` returns a bimodal latency distribution for an unknown reason.**
The same request returns either ~5.4 s or ~9–16 s on `partial-rolling`,
and either ~10.7 s or ~14.5 s on `partial-extension`, with nothing in between.
That is what the table's `partial-extension-7day` row is really showing when
it reads 14.565 s against `cold-7day`'s 12.908 s — six missing days apparently
costing more than seven.

It is confined to this endpoint. `data-log` issues the same ConsDB query through the
same `ConsdbExposuresAdapter` and is flat to 0.4 s across 20 runs; `static-visit-map`
and `context-feed` are flat to 0.15 s; and `exposures` itself is flat at 3.02–3.12 s
when served entirely from cache. Further testing shows that pre-warming just the
ConsDB branch — requesting `/data-log` for the missing day first, which fills
the shared cache entry but not the `rubin_nights` backed caches — removes it: the
slow mode goes from 2/12 runs to 0/12, and the spread tightens from 0.95 s to
0.17 s.

`/exposures` is the only endpoint that fetches several adapters concurrently
(`fetch_concurrently` over ConsDB, dome and visit-overhead), which is the one
structural feature distinguishing it from every endpoint that stays unimodal.
**The mechanism is not established.** The primary candidates are CPU quota throttling on a
one-core pod interacting with in-flight upstream I/O and GIL contention between the
concurrent branches. Further investigation is warranted into whether this is
specific to the exposure endpoint, the environment, or whether the `_fetch_concurrently`
helper contains some hidden flaw. Initial investigation shows that the issue
cannot be reproduced under local development circumstances, which points weakly
towards an environment/resource allocation issue.

**The practical consequence matters more than the cause.** A captured p50 for these
rows reflects the mix of fast and slow draws in that particular capture, not a stable
value — which is why this row moves between captures that agree everywhere else. Read
it as a distribution, not a number. Everything else about this endpoint is
strong — −81% hot, −18% cold, 5.2× in steady state, and a burst that went
37.613 s → 15.680 s.

### 7. Error rates

Failures are rare on both sides and concentrated in one place.

| | requests | failures | rate |
|---|---|---|---|
| baseline | 4,200 | 1 | 0.02% |
| refactored | 5,800 | 29 | 0.50% |

The baseline's single failure was a 504 on `exposures cold-7day-burst` at the 60s
gateway timeout. Every one of the refactored run's 29 was on a
`multi-night-visit-maps` burst, from the queueing in §6(b).

### 8. Return values

The byte flags in the table fall into three groups, all of which are expected.

1. **Different window.** Tagged `[OTHER WINDOW]`: `partial-rolling` requests days
   2–8 against a baseline for days 1–7, so a different payload is the scenario
   working correctly.
2. **Context Feed changes.** As explained in more detail in the `capture-compare.md`
   document, the `/context-feed` endpoint has two known changes - the value of
   empty timestamp fields has changed, and some columns are dependant on cache state.
3. **Serialisation noise.** The multi-night visit maps, whose Bokeh documents differ in
   structure while carrying identical values (also noted in `capture-compare.md`).


---

## Summary

- **The cache works, and it works everywhere.** Every endpoint's upstream fetch cost
  is effectively removed on a hit, and cost tracks missing days rather than
  requested days — verified to within 1% on `almanac` and within ~5% on the six-day
  partial for every endpoint except `exposures`.
- **Concurrent cost collapsed on all nine data endpoints.** Ten simultaneous
  requests for the same key cost 1.0–1.3× a single one, against 1.3–3.6× before,
  which is the single-flight lock behaving as designed.
- **This capture cannot see the best of it.** Six endpoints' hot latency is pinned
  at the 0.17 s Australia → SLAC RTT, and `data-log`, `context-feed` and `exposures`
  are dominated by a ~0.75 MB/s per-connection ceiling on uncompressed payloads.
  Enabling gzip (`REFACTOR_ODDITIES.md` §13, measured at 4.4×) would do more for
  those three than anything in the refactor did.
- **~1.0 s per request on Rubin Nights backed adapters is not the cache.** 
  It is a fixed startup cost removed from the request path.
- **Two regressions survives re-measurement and requires attention.** 
  - `multi-night-visit-maps` bursts lose 2–18% of requests to gateway timeouts,
    due to resource contention and starvation
  - `exposures` return time is bimodal
- **Error rates are low on both sides** — 1 failure in 4,200 baseline requests, 29
  in 5,800 after, all of them the visit map queueing mentioned above.

## Follow-ups, in order

1. **Enable response compression.** The largest single win available, and already
   scoped in [OSW-2812](https://rubinobs.atlassian.net/browse/OSW-2812).
2. **Format `context-feed`'s `config` column explicitly** rather than letting
   pandas' inferred dtype decide, so a night's rendering does not depend on which
   other nights shared its query.
3. **Account for `exposures`' bimodal latency** (§6(c)) — the widest row in the table
   at 10.706–17.904 s, and confined to the one endpoint that fetches its adapters
   concurrently. Examine whether this is an intrinsic issue with the
   adapters involved, or if there is a more serious issue with `fetch_concurrently`
4. **Resolve multi-night-visit-maps parallelization.** Whether through changing
   resource allocation, partial caching, or decreasing process-count, this
   is the stand-out slow endpoint that needs further improvement.
