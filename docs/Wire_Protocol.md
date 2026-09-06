# Wire Protocol

Binary, big-endian payloads inside the existing 16-byte Transit frame
(`magic=TRNT`, CRC32C Castagnoli over header-with-zero-CRC + payload).

Design goals: **tight encode/decode**, **fail closed**, **no extra copies**
on decode (fields alias the frame payload until the callback returns).

## Frame

See `t_proto.h`. Max payload 16 MiB. Unknown `type`, bad magic/version, or
CRC mismatch closes the connection.

## Queue names

1–255 bytes, charset `[A-Za-z0-9._-]`. Trailing bytes after a payload are
rejected. Names are copied to a NUL-terminated stack buffer before any
broker call so C string APIs cannot read past the frame.

## Payloads

### `OPEN_QUEUE` (client → server)

```
u8  qtype      /* T_QUEUE_FIFO / PRIORITY / BROADCAST */
u8  qflags     /* T_QUEUE_FLAG_* */
u8  mode       /* T_WIRE_MODE_PRODUCER | T_WIRE_MODE_CONSUMER */
u16 name_len
u8  name[name_len]
```

Idempotent per connection. Consumer mode subscribes that connection for
`PUSH`. Producer mode is required for `POST`. Queue is created in the
`default` domain if it does not exist.

`qflags` are applied only at create:

- `T_QUEUE_FLAG_DURABLE` requires a broker datadir (`transit-server -d` or
  `[storage] datadir=`) when Raft is not attached. Missing datadir returns
  `T_ERR_IO`. Combined with `BROADCAST` returns `T_ERR_INVALID`. WAL path is
  `{datadir}/{domain}.{queue}.wal` (file mode `0600`, cap 256 MiB, default
  fsync every 32 records). Records are `PUT` / `DEL` / `JOIN` (group name).
  With Raft, create is a log `CREATE` (no per-queue WAL).
- A Raft follower `OPEN` of a missing queue returns `T_ERR_AGAIN`. The
  leader waits for majority apply before ACKing create.
- `T_QUEUE_FLAG_EXCLUSIVE` refuses a second consumer with `T_ERR_BUSY`.
- `T_QUEUE_FLAG_AUTODELETE` deletes the queue when the last network session
  closes it (or disconnects). A Raft-attached leader `CLOSE` of that last
  session proposes `DELETE` and ACKs only after majority apply (same
  election-timeout `T_ERR_AGAIN` as `POST`). Disconnect still proposes
  without waiting (session is already gone).

Client helper: `t_client_open_queue` packs mode in the low 8 bits,
`T_CLIENT_QFLAG_*` in bits 8–15, and `T_CLIENT_QTYPE_*` (`FIFO` /
`PRIORITY` / `BROADCAST`) in bits 16–23 so they do not collide.
Type is applied only at create (same as `qflags`). An unknown type
is rejected on the client before send.

### `CLOSE_QUEUE`

```
u16 name_len
u8  name[name_len]
```

Drops this connection’s producer/consumer bits and unsubscribes.

### `POST`

```
u8  priority
u16 name_len
u8  name[name_len]
u32 data_len
u8  data[data_len]
```

### `PUSH` (server → client)

```
u64 msg_id     /* queue id for FIFO/priority pull; per-connection for broadcast */
u8  priority
u16 name_len
u8  name[name_len]
u32 data_len
u8  data[data_len]
```

FIFO and priority queues are consumed into inflight before `PUSH`. A `PUSH`
is sent only when a session credit can be acquired; otherwise the message
stays pending. Broadcast still fans out fire-and-forget (per-connection
`msg_id`) and skipped deliveries are not requeued.

Each connection has a PUSH credit window (`t_server_config.push_credits`,
default 64, `0` = unlimited). Slow consumers cannot grow `t_conn` send
buffers beyond the window (the buffer itself is still capped at 64 MiB).

### `ACK` (server → client)

```
u16 req_type
i32 status     /* t_error_code, two’s complement */
u16 name_len   /* 0 if none */
u8  name[name_len]
```

### `CONFIRM` / `REJECT`

```
u64 msg_id
u16 name_len
u8  name[name_len]
```

`CONFIRM` acks a matching inflight `PUSH` on this session. Without Raft
that is a local `t_queue_ack` (WAL `DEL` for durable queues). With Raft
attached it proposes an `ACK` command and succeeds only after majority
commit and apply (`docs/Raft.md`). `REJECT` proposes `NACK` the same
way (local `t_queue_nack` when Raft is not attached) and the server
then tries to `PUSH` again. A Raft `REJECT` that cannot majority-commit
returns `T_ERR_AGAIN` and keeps session inflight. Unknown ids still
return a credit (capped at the window) and `ACK` `T_OK`. Disconnect
nacks leftover inflight locally. These frames skip the per-connection token
bucket. Broadcast `PUSH` is not inflight — confirm only returns a
credit. The TCP client (`t_client_dial`) sends `CONFIRM` automatically
after a decoded `PUSH` that a subscriber callback received, unless
`t_client_set_auto_confirm(client, 0)`. A `PUSH` with no matching
callback is left unsettled.
`t_client_reject` / `t_client_confirm` settle the last `PUSH` on that
queue; a second settle is `-1`. `t_client_reject_follow` /
`t_client_confirm_follow` wait for the ACK. `T_ERR_AGAIN` with a
different client-port hint redials once and then returns `-1` (a new
session must wait for redelivery; rejecting the old id would be
unknown-id `T_OK`).

### `HEARTBEAT` / `NOP`

Empty payload. Server replies `ACK` and refreshes the session timestamp.
These frames skip the per-connection token bucket so a busy data path
cannot starve keepalive. The TCP client sends `HEARTBEAT` every
`T_CLIENT_HEARTBEAT_DEFAULT_MS` (10s) unless `t_client_set_heartbeat`
disables it (`0`). Those ACKs do not advance `ack_seq` or overwrite
`last_status` — they are not a substitute for `OPEN`/`POST`/`JOIN`
completion.

### `AUTH` (client → server)

First frame when a PSK is configured (`t_server_config.psk` /
`transit-server -k` / `[auth] psk=`). Payload is exactly 32 bytes:

```
u8 mac[32]   /* HMAC-SHA256(psk, "transit.auth.v1") */
```

Trailing junk is rejected. Success: `ACK` `T_OK`. Wrong MAC or any other
frame first: `ACK` `T_ERR_PERMISSION` and the connection is closed.
No PSK on loopback: AUTH is not required (an unexpected `AUTH` frame
closes like any unknown type). Binding a non-loopback address without a
PSK fails at `t_server_start`.

### `CLUSTER`

Not accepted on the client port (connection closed). The cluster peer
listener (`t_peer`, default `127.0.0.1:4223`, `transit-server -C` /
`[cluster] port=`) accepts only `T_MSG_CLUSTER` and replies in-kind.
Other frame types close the peer connection.

Peer payloads:

```
u8 rpc   /* 1 VoteReq, 2 VoteResp, 3 AppendReq, 4 AppendResp,
            5 SnapReq, 6 SnapResp */
```

`VoteReq`: `u64 term, candidate_id, last_log_index, last_log_term`  
`VoteResp`: `u64 term`, `u8 granted`  
`AppendReq`: `u64 term, leader_id, prev_log_index, prev_log_term, leader_commit`,
`u32 n`, then `n` entries of `u64 index, term`, `u8 type`, `u32 len`, `data`.  
`AppendResp`: `u64 term`, `u8 success`, `u64 match_index`.  
`SnapReq`: `u64 term, leader_id, last_index, last_term`, `u32 len`, `data`.  
`SnapResp`: `u64 term`, `u8 success`, `u64 match_index`.

Handle with `t_raft_rpc`. Durable term/vote/`commit_index`/entries:
`t_raft_open_log` (header v2). Optional `raft.log.snap` (`TRFS` v2) holds
applied queue state so restart skips the compacted prefix. Queue
`PUT`/`ACK` command layouts are in `docs/Raft.md`.

Follower `OPEN` / `POST` / `JOIN` on the client port return `T_ERR_AGAIN`
when a cluster is attached and this node is not leader. The ACK name is
`host_clientport` of the current leader (underscore because `:` is not a
valid name character). Empty name if the leader is unknown or its client
listen port was not configured (`id@host:peer` without `/client`). The
cluster peer port is never used as a hint. `t_client_parse_leader_hint`
and `t_client_redial_leader` follow a valid hint; the new session must
`OPEN` again. A same-peer hint (already dialed host/port) is not
followed: that is retry-later, not a redirect. `t_client_open_follow`,
`t_client_join_follow`, `t_client_post_follow`,
`t_client_close_follow`, `t_client_confirm_follow`,
`t_client_reject_follow`, and `t_client_subscribe_follow` wait with
`t_client_wait_ack` and redial at most once per call. A second
`open_follow` on an already-acked queue returns immediately.
`confirm_follow` / `reject_follow` do not resend the old `msg_id`
after a redial.
A Raft-attached leader `POST`/`CONFIRM`/`REJECT`/`OPEN` of a missing
queue, or last-`OPEN` `CLOSE` of an `AUTODELETE` queue, appends first
and ACKs only after majority apply (the evloop is not blocked).
`T_ERR_AGAIN` if the node is not leader, the append fails, or the
apply wait exceeds one election timeout. The uncommitted entry stays
in the log; client retry is at-least-once.

## Server safety switches

| Control | Default | Effect |
|---------|---------|--------|
| Bind host | `127.0.0.1` | Loopback unless the operator passes another IPv4 |
| `max_conns` | 1024 | Extra accepts are closed immediately |
| Token bucket | 128 burst, 50/s | Excess frames get `ACK` `T_ERR_BUSY` (`CONFIRM`/`REJECT` exempt) |
| Idle timeout | 30s | Session inactivity closes the socket |
| PSK AUTH | unset | First frame HMAC-SHA256; required off loopback |
| PUSH credits | 64 | Outstanding unacked `PUSH` per session; `0` = unlimited |

`0.0.0.0` is allowed only when set explicitly (CLI/config) **and** a PSK is
configured. Admin HTTP already binds `127.0.0.1`.

### `JOIN` (client → server)

```
u16 group_len
u8  group[group_len]
u16 consumer_len
u8  consumer[consumer_len]
u16 queue_len
u8  queue[queue_len]
```

All three names use the queue charset. Trailing bytes are rejected.
`OPEN` is unchanged: extra bytes after an `OPEN` payload still fail
decode, so a group cannot be smuggled onto an old server.

Fail closed:

- No consumer `OPEN` on `queue` → `T_ERR_PERMISSION`
- Broadcast queue → `T_ERR_INVALID`
- Duplicate `consumer` id in the group → `T_ERR_EXISTS`
- Same connection already joined that queue under another id, or a
  second group name on a queue that already has one → `T_ERR_BUSY`
- Disconnect / `CLOSE` removes the member. An empty group refuses
  dispatch; the message stays in the queue. PUSH credits still apply.
- The group name is sticky on the queue until delete. Last `OPEN`
  drop or failover does not release exclusivity; an OPEN-only
  consumer still cannot steal.

One group per FIFO/priority queue. Hot path is still one `consume`
plus one `PUSH` to the session `t_cgroup_pick` returns (O(1) RR).
Idempotent `JOIN` (same group, consumer, queue on the same
connection) is `ACK` `T_OK`. See `docs/Consumer_Groups.md`.

## Client API

- `t_client_connect` — in-process stub (tests and local fanout).
- `t_client_dial(client, loop, host, port)` — real TCP using `t_conn`.
  If `t_client_set_psk` was called, the first frame is `T_MSG_AUTH`.
  `dial` waits for that ACK (`T_CLIENT_AUTH_WAIT_DEFAULT_MS`) and
  returns 0 only on `T_OK`. Timeout or a non-OK ACK drops the
  socket. Does not pump the evloop (same contract as `wait_ack`).
- `t_client_open_queue(..., T_CLIENT_OPEN_PRODUCER \| T_CLIENT_OPEN_CONSUMER)`.
  Bits 8–15: `T_CLIENT_QFLAG_DURABLE` / `EXCLUSIVE` / `AUTODELETE`.
  Bits 16–23: `T_CLIENT_QTYPE_FIFO` (default) / `PRIORITY` / `BROADCAST`.
- `t_client_last_status()` — last decoded `ACK` status. Starts at `0`
  (same as `T_OK_CODE`); do not treat that as “an ACK arrived”.
- `t_client_join(client, group, consumer_id, queue)` — TCP only
  (`t_client_connect` stub returns -1). Wait on `ack_seq` like `OPEN`.
- `t_client_ack_seq()` — monotonic count of decoded `ACK` frames. Wait
  for this to change after `OPEN`/`POST`/`CLOSE`/`AUTH`/`JOIN`/
  `CONFIRM`/`REJECT`, then read
  `last_status`. Status and the name are published before the sequence
  increment (seq_cst), so a new seq is a happens-before for those fields.
- `t_client_heartbeat` — one `HEARTBEAT`. TCP only. The ACK is ignored
  for `ack_seq` / `last_status`.
- `t_client_set_heartbeat(ms)` — repeat interval; `0` off; default 10s
  so the server 30s idle timeout does not drop waiting consumers.
- `t_client_wait_ack(client, prev, timeout_ms)` — poll until `ack_seq`
  moves past `prev`. Does not run the evloop.
- `t_client_open_follow` — `OPEN` then wait. On `T_ERR_AGAIN` with a
  different client-port hint, redial once and `OPEN` again. Returns 0
  only after `T_OK`. Already-acked on this session is a no-op. No hint
  or a same-peer hint is fail-closed.
- `t_client_join_follow` — consumer `OPEN` (via `open_follow`) then
  `JOIN`. Follows a different client-port hint once. TCP only.
- `t_client_post_follow` — producer `OPEN` if needed, `POST`, follow
  a different client-port hint once. In-process stub opens locally
  and fans out (no ACK wait).
- `t_client_close_follow` — `CLOSE` then wait. On `T_ERR_AGAIN` with a
  different client-port hint, redial once, `OPEN` with the saved flags,
  and `CLOSE` again. Send failure keeps the local open (fail closed).
  Stub closes locally.
- `t_client_set_auto_confirm` — `1` (default) sends `CONFIRM` after
  each `PUSH` callback. `0` leaves the last `PUSH` unsettled.
- `t_client_last_push_id` — `msg_id` of the last decoded `PUSH`.
- `t_client_confirm` / `t_client_reject` — settle that `PUSH` on
  `queue`. TCP only. Stub returns `-1`.
- `t_client_confirm_follow` / `t_client_reject_follow` — settle then
  wait. A different client-port hint redials once and returns `-1`.
- `t_client_last_ack_name()` — last decoded `ACK` name (`host_port` on
  follower `POST` `T_ERR_AGAIN`).
- `t_client_subscribe` may be called before `open_queue`. It registers
  the callback, then a consumer `OPEN`, and tracks that open so
  `close_follow` can release exclusive / autodelete. On a dialed
  client a `PUSH` cannot arrive before the callback is registered.
- `t_client_subscribe_follow` — callback first, then consumer `OPEN`
  (plus `T_CLIENT_QFLAG_*`) and wait. Follows a different client-port
  hint once. A failed wait drops the callback just added.
- After a `PUSH` that a callback actually received, the dialed client
  sends `CONFIRM` so the server credit window can refill, unless
  auto-confirm is off or the callback already settled the `PUSH`.
  A `PUSH` with no matching subscriber is not confirmed (fail closed:
  unsubscribe must not silently ack).

Drive the same `t_evloop` that owns the server (or a dedicated client loop)
so `PUSH`/`ACK` are read.
