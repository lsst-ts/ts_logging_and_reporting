# Backend caching refactor — before/after performance

Both captures were taken against the usdf-rsp-dev deployment.

| | baseline | refactored |
|---|---|---|
| Deployment | `usdf-rsp-dev.slac.stanford.edu/nightlydigest/api/` | `…/nightlydigest/api/alpha/` |
| Capture window | 2026-08-12T07:41 → 10:22Z | 2026-08-12T10:47 → 08-13T02:49Z |
| Runs / bursts | 20 sequential, 10 rounds × 10 concurrent | same |
| Range | `dayObsStart=20260611`, `instrument=LSSTCam` | same |

The `git_commit` field in both JSON files (`c8cb12f`, `528e289`) records the
**harness** checkout at capture time, not the code deployed behind either URL. The
baseline build is identified by its deployment path alone.

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
  burst against burst). Every 7-day non-burst after-row therefore shares one
  `cold-7day` baseline.
- **bytes** is the modal decompressed response size. `[DIFFERS]` means sizes varied
  within one scenario; `[DIFFERS AFTER]` that the two runs settled on different
  sizes for the same request; `[OTHER WINDOW]` that the two rows did not request
  the same nights, so their sizes are not comparable — this is `partial-rolling`,
  which measures the shifted week against a baseline for the original one. See §8.

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

The harness ran from Australia against SLAC. Two properties of that link are
imposed on every number in the table, and both must be subtracted before any row
is read as a statement about the backend.

**A 0.174 s floor.** Every fully cached small response lands on it, to the
millisecond, regardless of endpoint or payload:

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
true speedup is larger than the table can show.

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
limited to ~0.13 MB in flight at 0.174 s RTT does exactly this: anything fitting
in one window is effectively free, anything larger is capped at window ÷ RTT. And
0.75 MB/s × 0.174 s = 0.13 MB puts the knee where it is observed.

Concurrency corroborates. Ten connections deliver several times the bytes per
second that one does:

| Endpoint / scenario | 1 stream | 10 streams (from wall p50) | scaling |
|---|---|---|---|
| context-feed hot-1day | 0.78 MB/s | 7.08 MB/s | 9.0× |
| context-feed hot-7day | 0.78 MB/s | 5.70 MB/s | 7.3× |
| data-log hot-7day | 0.68 MB/s | 4.85 MB/s | 7.1× |
| exposures hot-7day | 0.74 MB/s | 3.41 MB/s | 4.6× |

Each container runs a single uvicorn process (`docker/startup.sh` calls
`run_logging_and_reporting`, which passes no `workers` argument), so explaining 9×
server-side would need roughly nine pods each independently capped at 0.78 MB/s.
Replica count is set outside this repo and is unconfirmed here, so read this table
as consistent with the window explanation rather than as proof of it — the
per-byte discontinuity above is the argument that does not depend on it.

Consequently:

- **`data-log`, `context-feed` and `exposures` sequential numbers are dominated by
  transfer, not by the backend.** `data-log hot-7day` at 35.872 s is ~35.7 s of
  moving 24.3 MB to Australia and ~0.2 s of server. Its "−3%" is not a weak cache
  result; it is a cache result invisible behind the link.
- Nothing here is compressed. `REFACTOR_ODDITIES.md` §13 records that neither
  `GZipMiddleware` nor nginx `gzip` is enabled, and measured 4.4× on this payload
  class. That single change would move these three endpoints more than anything in
  the refactor did.
- **Where before and after returned byte-identical payloads, the transfer term
  cancels exactly in the difference.** Those deltas are link-rate independent and
  are the most trustworthy figures in this document; §4 and §6 rely on them.

**Caveats not resolved by the data.** Deployment parity between `/api/` and
`/api/alpha/` (replicas, CPU/memory limits, node class, Redis instance) is
unconfirmed — this is the first thing to check before accepting any regression
below as a code regression. The two captures are also ~16 h apart in wall clock
and the refactored capture spans 16 h, so link conditions were not held constant.

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

Neither end of this table means what it appears to mean. The top is floored by the
0.174 s RTT, so those speedups are lower bounds. The bottom is floored by transfer
(`data-log`, `context-feed`) or by render cost that was never inside the cache's
scope (the two map endpoints). The defensible statement is that **the cache
reliably removes the upstream fetch, and what remains is whatever else the endpoint
does** — bytes on the wire, or a figure build.

Two rows measure less than they appear to. `exposure-entries` returned 23 bytes —
an empty result — in every request of both captures, and `jira-tickets` returned
13 bytes on every 1-day request. For those, the numbers are a valid measurement of
request plumbing and cache round-trip and say nothing about behaviour on a
populated range. `jira-tickets` does carry data at 7 days (3,741 bytes).

### 3. Concurrency

Burst p50 divided by the sequential p50 of the same cache state and range — the
factor by which ten simultaneous requests for the same data cost more than one:

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
request. A shift change where several people open the same night at once used to be
the worst case and is now close to free.

Two entries need care rather than celebration.

**`obs-status` cold at 2.65× and `narrative-log` at 1.87–1.96× are denominator
artefacts.** Both divide by a sequential time already at the RTT floor, so the
costs a burst still pays regardless — ten TCP connections, ten TLS handshakes, ten
response writes — stop being negligible against it. In absolute terms `obs-status`'s
cold burst went 2.699 s → 0.543 s and its wall time 4.803 s → 0.633 s. Below about
a second of baseline, read absolute numbers rather than ratios.

**The two map endpoints genuinely got worse under concurrency**, and that is §6.

### 4. A per-request constant that is not the cache

`obs-status` improves by ~1.0 s on *every* scenario, including the cold ones, which
flush Redis before each request. A cold path cannot be faster because of a cache.
Its payloads are byte-identical between the runs, so these deltas are pure server
time:

| scenario | before (s) | after (s) | delta |
|---|---|---|---|
| cold-1day | 1.157 | 0.204 | −0.953 |
| cold-7day | 1.229 | 0.205 | −1.024 |
| hot-7day | 1.229 | 0.180 | −1.049 |
| partial-extension-7day | 1.229 | 0.196 | −1.033 |
| partial-fragmented-7day | 1.229 | 0.203 | −1.026 |

A flat ~1.0 s, independent of cache state and of range length, is a fixed
per-request cost that was removed rather than a cache effect. The candidate in the
code is `RubinNightsClientsMixin` (`adapters/mixins.py:162`), which holds `_clients`
and `_efd_client` as `functools.cached_property` — "built once so credential
discovery does not repeat on every fetch" — where the pre-refactor path called
`get_clients()` inside the request.

The corollary matters for how the refactor is credited: after the change
`obs-status` is 0.204 s cold and 0.180 s hot, so **the difference between querying
the EFD and not querying it at all is 24 ms.** Essentially all of `obs-status`'s
gain is this fixed cost, not caching.

The mixin is also used by the dome, context-feed and visit-overhead adapters, so
the same saving is probably inside their totals, but only `obs-status` isolates it:
`context-feed`'s payload changed between the runs (§8), and the others are not
range-independent enough to separate a constant. The ConsDB visit and exposure
adapters do not use the mixin — they build a `SqlClient` — so `exposures` and the
map endpoints are not covered by this explanation.

### 5. Per-day chunking is verified

The design's central claim is that cost tracks *missing* days rather than requested
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

`almanac` is the clean case — pure computation, no upstream variability, a 4 KB
payload, so neither the RTT floor nor the transfer ceiling distorts it — and it
lands within 1% at both points. The six-day column, where the per-day term
dominates, agrees within ~5% on every endpoint except `exposures`.

The two-day column is unreliable wherever the per-day term is small next to the
floor: `jira-tickets`' +76% is +0.21 s on a 0.28 s prediction, and `narrative-log`'s
+14% is +31 ms. Those are floor effects, not chunking failures. `exposures` is a
real outlier and is covered in §6.

Note that `almanac partial-extension-7day` improving only −15% is **correct
behaviour looking unimpressive**: widening a 1-day view to a week genuinely requires
six nights of work, and no cache can avoid it. The scenario exists to confirm the
cache does not pretend otherwise.

**`partial-rolling-7day` is excluded from this test.** It requests days 2–8, so both
its payload and its upstream cost differ from the baseline for reasons unrelated to
cache behaviour — the reason its byte cell reads `[OTHER WINDOW]`.

### 6. Regressions

Three, of different kinds.

**(a) `data-log`'s cold path costs ~3.5–4.0 s more per request.** Its payloads are
byte-identical between the runs, so the transfer term cancels and these deltas are
pure server time:

| scenario | before (s) | after (s) | delta |
|---|---|---|---|
| cold-1day | 7.685 | 11.652 | **+3.967** |
| cold-7day | 36.851 | 40.300 | **+3.449** |
| hot-1day | 7.685 | 6.361 | −1.324 |
| hot-7day | 36.851 | 35.872 | −0.979 |
| partial-fragmented-7day | 36.851 | 35.790 | −1.061 |

The penalty is roughly constant rather than proportional to days, so it is a fixed
per-request cost on the fetch path, not the cost of writing 24 MB into Redis. The
cache itself is working underneath it — the hot path does ~5.3 s less server work
than the cold one — so the penalty is repaid on the first hit.

It is worth being explicit about why this endpoint looks so flat in the table:
`data-log`'s entire range-scaling is transfer in both runs. Its marginal cost is
4.86 s/day before and 4.78 s/day after, and 3.33 MB/day at 0.68 MB/s is 4.9 s/day.
Nothing about the range scaling is server work at all. Finding the +3.7 s needs
server-side profiling, not another end-to-end run.

**(b) `multi-night-visit-maps` bursts are 1.5–2× slower and now drop requests.**
Both sides have honest medians here — the baseline completed 100/100 on every burst
row — so this is a real regression rather than a sampling artefact.

The mechanism is the worker pool. `WorkerPoolMixin` sets `pool_workers = 4`, neither
map service overrides it, and both route their render through `run_in_worker`. Ten
concurrent renders are therefore served four at a time: a floor of
`ceil(10/4) = 3` sequential waves. `static-visit-map` matches that model almost
exactly:

| static-visit-map burst | 3 × sequential p50 | observed wall p50 | ratio |
|---|---|---|---|
| hot-1day | 10.67 s | 10.24 s | 0.96 |
| hot-7day | 12.30 s | 12.32 s | **1.00** |
| cold-7day | 14.49 s | 13.14 s | 0.91 |
| partial-rolling-7day | 12.69 s | 11.86 s | 0.93 |

**The pool is not itself the problem.** On `static-visit-map`, whose render is
~4 s, three waves come to ~12 s and every burst row improved (wall −25% to −34%).
The problem is that the same fixed floor applied to a render costing 21–28 s is
63–84 s before any contention, and `multi-night-visit-maps` then runs 1.5–2.1× worse
than even that:

| MNVM burst | 3 × sequential p50 | observed wall p50 | ratio |
|---|---|---|---|
| hot-1day | 63.0 s | 119.4 s | 1.89 |
| hot-7day | 81.3 s | 121.0 s | 1.49 |
| cold-7day | 84.0 s | 174.1 s | 2.07 |
| partial-rolling-7day | 67.3 s | 113.9 s | 1.69 |

That excess is consistent with four concurrent Bokeh renders contending for a pod
without four spare cores, though this capture cannot confirm that from outside.

The failures follow directly from the queueing. All 29 are gateway timeouts at
60/120/180 s boundaries with 160- or 167-byte bodies — proxy error pages, not the
application's `{"detail": …}` JSON, which would be 34 bytes and which no request
received. **The pool's own 503 and 504 never fired**, so it never shed load and
never reached `pool_timeout`; requests simply queued behind four workers until a
proxy gave up:

| MNVM burst | completed | timeouts |
|---|---|---|
| cold-7day | 82/100 | 17 × 504, 1 client-side abort |
| hot-1day | 93/100 | 7 × 504 |
| hot-7day | 98/100 | 2 × 504 |
| partial-rolling-7day | 98/100 | 2 × 504 |

The other half of this trade is in §8: the pool is what made concurrent map output
deterministic. The knobs are `pool_workers`, the render cost itself, and the proxy
timeout — and with a 60 s gateway limit against a 21–28 s render, `pool_workers = 4`
cannot serve ten concurrent users whatever the cache does.

**(c) `exposures partial-extension-7day` costs more than a fully cold fetch.**
14.565 s to fetch six missing days against 12.908 s to fetch all seven — more time
for strictly less work. It is also the least stable row in the capture (min 10.706,
max 17.904, against 12.462–13.630 for `cold-7day`). `partial-fragmented-7day` shows
the same shape: 9.996 s for two missing days where the linear model predicts 5.86 s.

`exposures` is the only endpoint where merging cached chunks with freshly fetched
ones costs more than not having the cached chunks at all, which points at the merge
of the 2.1 MB frame rather than at the fetch. Everything else about this endpoint is
excellent — −81% hot, −18% cold, 5.2× in steady state, and its burst went 37.613 s
→ 15.680 s — so this is a targeted fix, not a design problem.

### 7. Error rates

Failures are rare on both sides and concentrated in one place.

| | requests | failures | rate |
|---|---|---|---|
| baseline | 4,200 | 1 | 0.02% |
| refactored | 5,800 | 29 | 0.50% |

The baseline's single failure was a 504 on `exposures cold-7day-burst` at the 60 s
gateway timeout. Every one of the refactored run's 29 was on a
`multi-night-visit-maps` burst, from the queueing in §6(b). **Every other endpoint
completed 100% of requests on both sides**, including all 400 `static-visit-map`
burst requests.

No request in either capture received a 5xx generated by the application itself —
no 500 from a handler, and none of the pool's 503 or 504. The refactored run's
higher rate is entirely one endpoint's concurrency ceiling, not a broad reliability
change.

### 8. Return values

The byte flags in the table fall into three groups.

1. **Different window.** Tagged `[OTHER WINDOW]`: `partial-rolling` requests days
   2–8 against a baseline for days 1–7, so a different payload is the scenario
   working correctly. Sixteen rows across seven endpoints.
2. **Intended content change.** `context-feed` returns 19,608 fewer bytes at 1 day
   and 41,472 fewer at 7 days — the `"NaT"` → `null` timestamp change.
3. **Unexplained.** Two remain, both worth a body-level diff.

**Unexplained (i): `context-feed`'s payload depends on cache state.** Within the
refactored build alone, the same URL returns different sizes according to how it was
served:

| scenario | bytes | vs cold of same range |
|---|---|---|
| cold-1day | 4,845,605 | — |
| hot-1day | 4,847,559 | **+1,954** |
| cold-7day / hot-7day | 13,814,752 | 0 |
| partial-extension-7day | 13,812,798 | **−1,954** |
| partial-fragmented-7day | 13,812,902 | −1,850 |

The same 1,954-byte quantum appears twice with opposite sign. Given that `"NaT"`
(five characters) against `null` (four) is a one-byte difference per field, this has
the shape of **both representations coexisting depending on whether the value
round-tripped through Redis** — the conversion applying on one path and not the
other. A fresh response and a cached response for the same request should be
byte-identical; here they are not. Two bodies diffed would settle it.

**Unexplained (ii): `multi-night-visit-maps` output varies when the cache is
partially filled.** Its cold, hot and rolling scenarios are perfectly stable — 20/20
identical sizes each. The two partial scenarios are not:

```
partial-extension-7day : 3325489 3325489 3325763 3326719 3325489 3327645 …  (5 distinct sizes / 20 runs)
partial-fragmented-7day: 3327645 3327645 3327645 3325489 3327645 3325489 …  (4 distinct sizes / 20 runs)
```

The two dominant values are exactly the stable sequential value (3,325,489) and the
stable burst value (3,327,645). What the varying scenarios have in common, and the
stable ones lack, is that **more than one contiguous run is fetched at once**
(`8cc9308`, "Fetch a request's contiguous runs in parallel"). The natural reading is
that merging parallel-fetched runs does not produce a deterministic row order, which
changes the Bokeh document. `exposures` shows a smaller version of the same thing:
2,111,891 against 2,111,893 on `partial-fragmented`, and 2,111,889 on 20 of its 100
`cold-7day-burst` responses.

This has a consequence beyond the maps. **A byte count only catches reordering when
it changes the serialised length**, so an endpoint could be returning
correctly-sized but differently-ordered rows and this harness would not see it. A
deterministic-order assertion in the merge path would close that gap.

One point in the refactor's favour that the flags obscure: the baseline has a worse
version of the same problem. `multi-night-visit-maps cold-1day-burst` returns up to
four distinct sizes *within a single round of ten* for its first four rounds before
settling, while every refactored burst round returns exactly one size. The worker
pool of §6(b) is what buys that.

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
  at the 0.174 s Australia→SLAC RTT, and `data-log`, `context-feed` and `exposures`
  are dominated by a ~0.75 MB/s per-connection ceiling on uncompressed payloads.
  Enabling gzip (`REFACTOR_ODDITIES.md` §13, measured at 4.4×) would do more for
  those three than anything in the refactor did.
- **~1.0 s per request on `obs-status` is not the cache.** It is a fixed cost
  removed from the request path, and it accounts for essentially all of that
  endpoint's gain — the difference between querying the EFD and not querying it is
  24 ms.
- **Three regressions.** `data-log`'s cold path is ~3.7 s slower per request, fixed
  rather than per-day; `multi-night-visit-maps` bursts are 1.5–2× slower and lose
  2–18% of requests to gateway timeouts, because a 4-worker pool imposes three
  sequential waves on a 21–28 s render; and `exposures partial-extension` costs more
  than a fully cold fetch.
- **Error rates are low on both sides** — 1 failure in 4,200 baseline requests, 29
  in 5,800 after, all of them the map queueing above, and none generated by the
  application itself.
- **Two open return-value questions.** `context-feed` responses differ by a repeated
  1,954-byte quantum depending on cache state, and `multi-night-visit-maps` output
  varies run to run when the cache is partially filled. Both sit on paths where
  cached and freshly fetched data are merged.

## Follow-ups, in order

1. **Confirm `/api/` and `/api/alpha/` are resourced identically** — replicas, CPU
   and memory limits, node class, Redis. Every regression above is provisional until
   they are.
2. **Diff two `context-feed` bodies** (cold against hot, same 1-day range) and two
   `multi-night-visit-maps` bodies from `partial-extension-7day`. Minutes of work
   each, and both are correctness questions rather than performance ones.
3. **Enable response compression.** The largest single win available, and already
   scoped.
4. **Re-run the harness from inside the cluster.** That removes both the RTT floor
   and the transfer ceiling, and would for the first time measure the backend rather
   than the link.
5. **Profile `data-log`'s cold path server-side** for the +3.7 s fixed cost.
6. **Revisit `pool_workers` for the map services** against the gateway's 60 s
   timeout, and re-measure `exposures`' partial-range merge.
