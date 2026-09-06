# Roadmap

Transit already has in-process queues, broker/domain routing, framed TCP,
admin HTTP, consumer groups, and a Raft log for clustered durable queues.
The gaps below are what still separate that library from a production
message bus.

## Implemented in this change

- Windows leftovers that blocked CI: `t_config` reads through `t_file`,
  `t_log` / `t_ratelimit` / `t_flowcontrol` use `t_mutex`, and tests speak
  `t_thread` + `t_socket_pair` instead of POSIX `pthread` / `socketpair`.
- Portable `t_thread` (CreateThread / pthread) for tests and benches.
- `t_socket_pair`: AF_UNIX on POSIX; loopback TCP plus a self-token on
  Windows so a stolen accept is rejected. Both ends non-blocking.
- Windows evloop is WSAPoll readiness (same `T_EV_READ`/`WRITE` contract
  as epoll/kqueue). Wakeup is a loopback pair. Incomplete IOCP completions
  are not used for socket I/O.
- Coroutine SysV assembly is not compiled on Windows; `t_coro_create`
  returns NULL (`T_HAVE_CORO_ASM == 0`).
- GitHub Actions `windows-latest` (default VS generator, x64) builds
  and runs ctest.
  CMake enables ASM only off Windows so MSVC configure does not require
  an assembler (CMake 4 / CMP0194). The job does not pin
  `Visual Studio 17 2022` because `windows-latest` now ships VS 2026.
  MSVC `cl` uses Interlocked (not stdatomic). Log macros avoid GNU
  `##__VA_ARGS__`. `/Zc:preprocessor` stays off so `windows.h` C5105
  is not `/WX`.
- `t_client_ack_seq`: tests wait for a decoded `ACK` instead of treating
  `last_status == 0` as success. That was the exclusive-consumer /
  autodelete flake on WSAPoll (OPEN had not been processed yet).
- `T_MSG_JOIN`: consumer groups on the client port. One group per
  FIFO/priority queue, O(1) `t_cgroup_pick`, one `PUSH`. Fail closed
  without consumer `OPEN`, on duplicate ids, and when the group is
  empty. See `docs/Consumer_Groups.md`.
- Raft queue log: clustered `POST` / `CONFIRM` append `PUT` / `ACK`
  commands, majority-commit (Figure 8), and apply on every node via
  `t_queue_restore` / `t_queue_drop`. The Raft log is the WAL when
  `t_broker_set_raft` is set. `OPEN` / delete append `CREATE` / `DELETE`.
  See `docs/Raft.md`.
- Static cluster membership: `transit-server -n` / `[cluster] id=` and
  `[cluster] peers=id@host:peer[/client],...`. Admin `/stats` reports the
  live Raft role, client-port leader hint, live server/broker counters,
  and `/health` plus `/ready` (200 only on a standalone or Raft leader).
- Client leader redirect: follower `OPEN`/`POST`/`JOIN` return `T_ERR_AGAIN`
  with `host_clientport` only when the leader's client port is known.
  `t_client_parse_leader_hint` / `t_client_redial_leader` follow that hint.
  `t_client_open_follow` waits for the ACK and redials once when the hint
  names a different client port; no hint or a same-peer hint stays put.
  A second `open_follow` is a no-op only when those mode bits are
  already acked; extra bits send a merged `OPEN`.
  `t_client_join_follow` / `t_client_post_follow` do the same one hop
  for `JOIN` and `POST`.
- Raft `NACK`: clustered `REJECT` appends the same-shaped command as
  `ACK`. Apply requeues inflight on every node; fail closed without a
  majority. Disconnect nacks stay local.
- Raft replicate is async on the evloop (same path as heartbeats).
  Clustered client ACKs wait for apply instead of blocking `peer_rpc_once`.
  The wait is fail-closed: `T_ERR_AGAIN` after one election timeout (or
  sooner if the node is no longer leader). Apply reports the entry
  index so a successful majority still ACKs `T_OK`.
- Client `HEARTBEAT` (default 10s) keeps idle TCP sessions alive under
  the server 30s idle timeout. Keepalive ACKs skip `ack_seq` so
  `wait_ack` is not spoofed. `HEARTBEAT`/`NOP` skip the token bucket.
- Raft snapshot: `raft.log.snap` holds applied queue state. Restart
  replays the tail only. Prefix compact waits until every peer's
  `match_index` covers `last_applied`. A lagging peer is caught up
  with `InstallSnapshot`. See `docs/Raft.md`.
- Sticky consumer-group name on the queue: first `JOIN` binds
  `t_queue_set_group` (Raft `JOIN` type 6 + snapshot v2). The live
  group object may drop with the last `OPEN`; the name does not,
  so an OPEN-only consumer cannot steal after disconnect or
  failover. See `docs/Consumer_Groups.md`.
- PSK `t_client_dial` waits for the `AUTH` ACK before returning
  connected. A wrong key or timeout drops the socket (`-1`);
  `is_connected` stays false. Heartbeat starts only after `T_OK`.
- Clustered `AUTODELETE` `CLOSE` waits for Raft `DELETE` apply
  before `T_OK`. A majority that never arrives is `T_ERR_AGAIN`
  and the queue stays; disconnect still proposes without waiting.
- Unclustered durable WAL records `JOIN` so a sticky group name
  survives process restart (same exclusivity as Raft snapshot).
- `t_client_close_follow` waits for the `CLOSE` ACK (needed for
  clustered `AUTODELETE`). A send failure no longer pretends success.
  `T_ERR_AGAIN` with a different client-port hint redials once.
- `t_client_reject` / `t_client_confirm` settle the last `PUSH`.
  Auto-confirm stays the default; `set_auto_confirm(0)` is fail-closed
  (no silent ack). `reject_follow` / `confirm_follow` wait for the
  clustered `NACK`/`ACK` apply. A redirect redials once and returns
  `-1` so the old `msg_id` is not settled as unknown-id `T_OK`.
- Auto-`CONFIRM` runs only when a subscriber callback received the
  `PUSH`. Unsubscribe no longer acks messages nobody handled.
- `t_client_subscribe` tracks the consumer `OPEN` so `close_follow`
  can release exclusive / autodelete. `t_client_subscribe_follow`
  waits (and applies `T_CLIENT_QFLAG_*` at create). A failed wait
  drops the callback just added.
- Client `OPEN` can create `PRIORITY` and `BROADCAST` queues
  (`T_CLIENT_QTYPE_*` in bits 16–23). FIFO stays the default.
  Unknown types fail before send.
- Last `unsubscribe` of a consumer-only open sends `CLOSE`, so
  exclusive / autodelete are released and a later consumer can
  take the backlog. A producer bit keeps the open.
- `t_client_last_push_priority` exposes the last `PUSH` priority
  (stub `post` included) so priority-queue consumers can see it.
- Unknown `CONFIRM`/`REJECT` on a FIFO/priority queue is
  `T_ERR_NOTFOUND` and does not release PUSH credit. Broadcast still
  returns a credit (`PUSH` is not inflight).
- Leader `redial` keeps subscriber callbacks. `subscribe_follow` to a
  follower still delivers after the hop (opens are session-local).
- `open_follow` merges missing mode bits instead of no-op on any ACK.
  Subscribe then `post_follow` (or produce then `subscribe_follow`)
  actually `OPEN`s the other half. `unsubscribe` of a mixed open
  `CLOSE`s and re-`OPEN`s producer so leftover consumer is dropped.
- Leader `redial` keeps OPEN flags and `JOIN` triples. A consumer
  `open_follow` replays `JOIN` so a group member is not dropped after
  the hop (empty group would hold messages forever).

## Remaining (priority order)

(none for the current production-gap list)

GitHub `windows` (`cl` `/W4 /WX`, `ctest -C Release`) is green as of
`d4521ec`.

Consumer groups over TCP (`T_MSG_JOIN`) are implemented. See
`docs/Consumer_Groups.md` and `docs/Wire_Protocol.md`.

Raft replicated durable-queue log is implemented. See `docs/Raft.md`.

TLS is still deferred (PSK AUTH covers the loopback-default client
port).

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
- TLS on the client or peer port
