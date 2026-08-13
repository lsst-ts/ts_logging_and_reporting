
Before: c8cb12f (2026-08-12T07:41 to 2026-08-12T10:22)
After:  528e289 (2026-08-12T10:47 to 2026-08-13T02:49)

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
| almanac | partial-rolling-7day-burst | 100/100 → 100/100 | 10.767 | 0.767 | -93% | 16.24 | 0.796 | -95% | 4015 [DIFFERS AFTER] |
| almanac | partial-rolling-7day | 20/20 → 20/20 | 3.723 | 0.701 | -81% |  |  |  | 4015 [DIFFERS AFTER] |
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
| context-feed | partial-rolling-7day-burst | 100/100 → 100/100 | 49.873 | 22.915 | -54% | 61.081 | 26.753 | -56% | 13748094 [DIFFERS AFTER] |
| context-feed | partial-rolling-7day | 20/20 → 20/20 | 28.403 | 21.629 | -24% |  |  |  | 13748094 [DIFFERS AFTER] |
| data-log | cold-1day | 20/20 → 20/20 | 7.685 | 11.652 | +52% |  |  |  | 4336170 |
| data-log | cold-7day-burst | 100/100 → 100/100 | 48.678 | 44.947 | -8% | 58.724 | 53.325 | -9% | 24323138 |
| data-log | cold-7day | 20/20 → 20/20 | 36.851 | 40.3 | +9% |  |  |  | 24323138 |
| data-log | hot-1day-burst | 100/100 → 100/100 | 10.348 | 9.311 | -10% | 15.459 | 13.821 | -11% | 4336170 |
| data-log | hot-1day | 20/20 → 20/20 | 7.685 | 6.361 | -17% |  |  |  | 4336170 |
| data-log | hot-7day-burst | 100/100 → 100/100 | 48.678 | 44.017 | -10% | 58.724 | 50.131 | -15% | 24323138 |
| data-log | hot-7day | 20/20 → 20/20 | 36.851 | 35.872 | -3% |  |  |  | 24323138 |
| data-log | partial-extension-7day | 20/20 → 20/20 | 36.851 | 36.196 | -2% |  |  |  | 24323138 |
| data-log | partial-fragmented-7day | 20/20 → 20/20 | 36.851 | 35.79 | -3% |  |  |  | 24323138 |
| data-log | partial-rolling-7day-burst | 100/100 → 100/100 | 48.678 | 43.603 | -10% | 58.724 | 47.516 | -19% | 22481113 [DIFFERS AFTER] |
| data-log | partial-rolling-7day | 20/20 → 20/20 | 36.851 | 33.23 | -10% |  |  |  | 22481113 [DIFFERS AFTER] |
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
| exposures | partial-rolling-7day-burst | 99/100 → 100/100 | 37.613 | 5.662 | -85% | 52.682 | 9.018 | -83% | 2027338 [DIFFERS AFTER] |
| exposures | partial-rolling-7day | 20/20 → 20/20 | 15.707 | 4.931 | -69% |  |  |  | 2027338 [DIFFERS AFTER] |
| jira-tickets | cold-1day | 20/20 → 20/20 | 0.633 | 0.476 | -25% |  |  |  | 13 |
| jira-tickets | cold-7day-burst | 100/100 → 100/100 | 1.145 | 0.584 | -49% | 1.805 | 0.603 | -67% | 3741 |
| jira-tickets | cold-7day | 20/20 → 20/20 | 0.608 | 0.533 | -12% |  |  |  | 3741 |
| jira-tickets | hot-1day-burst | 100/100 → 100/100 | 1.125 | 0.177 | -84% | 1.96 | 0.195 | -90% | 13 |
| jira-tickets | hot-1day | 20/20 → 20/20 | 0.633 | 0.174 | -73% |  |  |  | 13 |
| jira-tickets | hot-7day-burst | 100/100 → 100/100 | 1.145 | 0.18 | -84% | 1.805 | 0.197 | -89% | 3741 |
| jira-tickets | hot-7day | 20/20 → 20/20 | 0.608 | 0.175 | -71% |  |  |  | 3741 |
| jira-tickets | partial-extension-7day | 20/20 → 20/20 | 0.608 | 0.494 | -19% |  |  |  | 3741 |
| jira-tickets | partial-fragmented-7day | 20/20 → 20/20 | 0.608 | 0.487 | -20% |  |  |  | 3741 |
| jira-tickets | partial-rolling-7day-burst | 100/100 → 100/100 | 1.145 | 0.58 | -49% | 1.805 | 0.598 | -67% | 4626 [DIFFERS AFTER] |
| jira-tickets | partial-rolling-7day | 20/20 → 20/20 | 0.608 | 0.479 | -21% |  |  |  | 4626 [DIFFERS AFTER] |
| multi-night-visit-maps | cold-1day | 20/20 → 20/20 | 19.206 | 22.391 | +17% |  |  |  | 1447936 [DIFFERS AFTER] |
| multi-night-visit-maps | cold-7day-burst | 100/100 → 82/100 | 50.643 | 101.051 | +100% | 78.436 | 174.128 | +122% | 3327645 [DIFFERS AFTER] |
| multi-night-visit-maps | cold-7day | 20/20 → 20/20 | 24.682 | 27.983 | +13% |  |  |  | 3325489 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-1day-burst | 100/100 → 93/100 | 38.377 | 78.419 | +104% | 49.867 | 119.362 | +139% | 1448909 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | hot-1day | 20/20 → 20/20 | 19.206 | 21.012 | +9% |  |  |  | 1447936 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-7day-burst | 100/100 → 98/100 | 50.643 | 86.075 | +70% | 78.436 | 120.992 | +54% | 3327645 [DIFFERS AFTER] |
| multi-night-visit-maps | hot-7day | 20/20 → 20/20 | 24.682 | 27.107 | +10% |  |  |  | 3325489 [DIFFERS AFTER] |
| multi-night-visit-maps | partial-extension-7day | 20/20 → 20/20 | 24.682 | 23.938 | -3% |  |  |  | 3325489 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | partial-fragmented-7day | 20/20 → 20/20 | 24.682 | 23.141 | -6% |  |  |  | 3327645 [DIFFERS] [DIFFERS AFTER] |
| multi-night-visit-maps | partial-rolling-7day-burst | 100/100 → 98/100 | 50.643 | 63.664 | +26% | 78.436 | 113.916 | +45% | 2865056 [DIFFERS AFTER] |
| multi-night-visit-maps | partial-rolling-7day | 20/20 → 20/20 | 24.682 | 22.421 | -9% |  |  |  | 2863040 [DIFFERS AFTER] |
| narrative-log | cold-1day | 20/20 → 20/20 | 0.235 | 0.234 | -0% |  |  |  | 22203 |
| narrative-log | cold-7day-burst | 100/100 → 100/100 | 1.119 | 0.588 | -47% | 1.75 | 0.916 | -48% | 125161 |
| narrative-log | cold-7day | 20/20 → 20/20 | 0.31 | 0.314 | +1% |  |  |  | 125161 |
| narrative-log | hot-1day-burst | 100/100 → 100/100 | 0.448 | 0.219 | -51% | 0.639 | 0.255 | -60% | 22203 |
| narrative-log | hot-1day | 20/20 → 20/20 | 0.235 | 0.176 | -25% |  |  |  | 22203 |
| narrative-log | hot-7day-burst | 100/100 → 100/100 | 1.119 | 0.365 | -67% | 1.75 | 0.633 | -64% | 125161 |
| narrative-log | hot-7day | 20/20 → 20/20 | 0.31 | 0.186 | -40% |  |  |  | 125161 |
| narrative-log | partial-extension-7day | 20/20 → 20/20 | 0.31 | 0.297 | -4% |  |  |  | 125161 |
| narrative-log | partial-fragmented-7day | 20/20 → 20/20 | 0.31 | 0.254 | -18% |  |  |  | 125161 |
| narrative-log | partial-rolling-7day-burst | 100/100 → 100/100 | 1.119 | 0.545 | -51% | 1.75 | 0.686 | -61% | 118036 [DIFFERS AFTER] |
| narrative-log | partial-rolling-7day | 20/20 → 20/20 | 0.31 | 0.238 | -23% |  |  |  | 118036 [DIFFERS AFTER] |
| obs-status | cold-1day | 20/20 → 20/20 | 1.157 | 0.204 | -82% |  |  |  | 31958 |
| obs-status | cold-7day-burst | 100/100 → 100/100 | 2.699 | 0.543 | -80% | 4.803 | 0.633 | -87% | 69151 |
| obs-status | cold-7day | 20/20 → 20/20 | 1.229 | 0.205 | -83% |  |  |  | 69151 |
| obs-status | hot-1day-burst | 100/100 → 100/100 | 2.54 | 0.248 | -90% | 4.637 | 0.366 | -92% | 31958 |
| obs-status | hot-1day | 20/20 → 20/20 | 1.157 | 0.178 | -85% |  |  |  | 31958 |
| obs-status | hot-7day-burst | 100/100 → 100/100 | 2.699 | 0.196 | -93% | 4.803 | 0.529 | -89% | 69151 |
| obs-status | hot-7day | 20/20 → 20/20 | 1.229 | 0.18 | -85% |  |  |  | 69151 |
| obs-status | partial-extension-7day | 20/20 → 20/20 | 1.229 | 0.196 | -84% |  |  |  | 69151 |
| obs-status | partial-fragmented-7day | 20/20 → 20/20 | 1.229 | 0.203 | -83% |  |  |  | 69151 |
| obs-status | partial-rolling-7day-burst | 100/100 → 100/100 | 2.699 | 0.296 | -89% | 4.803 | 0.485 | -90% | 71514 [DIFFERS AFTER] |
| obs-status | partial-rolling-7day | 20/20 → 20/20 | 1.229 | 0.191 | -84% |  |  |  | 71514 [DIFFERS AFTER] |
| static-visit-map | cold-1day | 20/20 → 20/20 | 4.389 | 3.847 | -12% |  |  |  | 138986 |
| static-visit-map | cold-7day-burst | 100/100 → 100/100 | 10.401 | 11.444 | +10% | 17.547 | 13.136 | -25% | 165454 |
| static-visit-map | cold-7day | 20/20 → 20/20 | 4.924 | 4.829 | -2% |  |  |  | 165454 |
| static-visit-map | hot-1day-burst | 100/100 → 100/100 | 9.844 | 7.747 | -21% | 15.593 | 10.242 | -34% | 138986 |
| static-visit-map | hot-1day | 20/20 → 20/20 | 4.389 | 3.557 | -19% |  |  |  | 138986 |
| static-visit-map | hot-7day-burst | 100/100 → 100/100 | 10.401 | 9.125 | -12% | 17.547 | 12.317 | -30% | 165454 |
| static-visit-map | hot-7day | 20/20 → 20/20 | 4.924 | 4.1 | -17% |  |  |  | 165454 |
| static-visit-map | partial-extension-7day | 20/20 → 20/20 | 4.924 | 4.813 | -2% |  |  |  | 165454 |
| static-visit-map | partial-fragmented-7day | 20/20 → 20/20 | 4.924 | 4.417 | -10% |  |  |  | 165454 |
| static-visit-map | partial-rolling-7day-burst | 100/100 → 100/100 | 10.401 | 8.765 | -16% | 17.547 | 11.859 | -32% | 160062 [DIFFERS AFTER] |
| static-visit-map | partial-rolling-7day | 20/20 → 20/20 | 4.924 | 4.231 | -14% |  |  |  | 160062 [DIFFERS AFTER] |
