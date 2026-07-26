# Performance Review

This note tracks repository-level performance and validation findings from the
July 2026 review.

## Findings

- GitHub Actions sanitizer jobs used lowercase matrix values to build CMake
  options, so `ENABLE_ASAN` and `ENABLE_UBSAN` were not actually set.
- `t_map` used open addressing with tombstones but did not track tombstone
  pressure. Repeated remove-heavy workloads could accumulate dead slots and
  lengthen future probe chains.
- TTL expiry removes entries in batches and is a remove-heavy caller of `t_map`,
  so it benefits directly from explicit map compaction after successful expiry.

## Applied Plan

- Make the sanitizer matrix carry exact CMake option names.
- Track `t_map` tombstones, clean them during resize/clear/destroy, and expose
  `t_map_compact()` for callers that know a remove-heavy phase just completed.
- Compact the TTL map after expiry batches while preserving the lazy stale-heap
  strategy that avoids per-update heap deletion.
- Add unit coverage for map replacement, removal, tombstone compaction, and
  post-compaction insertion.
- Keep CI on currently portable Linux/macOS targets; Windows backend sources
  remain present, but CI is disabled until POSIX and assembly fallbacks exist.

## Verification

Run these commands before publishing changes:

```bash
cmake --build build -j
ctest --test-dir build --output-on-failure -j
cmake -B /tmp/opencode/transit-verify -S . -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-verify -j
ctest --test-dir /tmp/opencode/transit-verify --output-on-failure -j
cmake -B /tmp/opencode/transit-asan -S . -DENABLE_ASAN=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-asan -j
ctest --test-dir /tmp/opencode/transit-asan --output-on-failure -j
cmake -B /tmp/opencode/transit-ubsan -S . -DENABLE_UBSAN=ON -DBUILD_EXAMPLES=OFF
cmake --build /tmp/opencode/transit-ubsan -j
ctest --test-dir /tmp/opencode/transit-ubsan --output-on-failure -j
```
