# Roadmap

Transit already has in-process queues, broker/domain routing, framed TCP,
admin HTTP, and a simplified Raft object model. The gaps below are what still
separate that library from a production message bus.

## Implemented in this change

- Typed wire payloads (`OPEN_QUEUE`, `POST`, `PUSH`, `ACK`, `CLOSE_QUEUE`,
  `CONFIRM`/`REJECT`, `HEARTBEAT`) on top of the 16-byte CRC32C frame.
- Protocol server (`t_server`): accept, session, per-connection token bucket,
  max-connection cap, idle timeout, default bind `127.0.0.1`.
- Network client (`t_client_dial`) that speaks the same protocol. The original
  `t_client_connect` in-process stub is unchanged.
- `transit-server` now listens on the client port and starts the broker.

## Remaining (priority order)

### 1. Durable queues (reliability)

`T_QUEUE_FLAG_DURABLE` is accepted and stored but not wired to `t_storage`.
Needed: write-ahead log of posts/acks, fsync policy, replay on start. Keep
hot-path posts in the existing queue; persist asynchronously with a bounded
loss window that the operator chooses.

### 2. Real Raft / cluster replication (reliability)

`t_raft` and `t_cluster` are in-memory, single-process helpers. Needed: Raft
RPCs over `T_MSG_CLUSTER`, persistent log, leader-only publish, follower
redirect. Do not replicate every payload through the client port.

### 3. Authentication (security)

Client connections are unauthenticated. Plan: first frame after TCP is a
`T_MSG_OPEN_QUEUE`-style handshake with HMAC of a pre-shared key (no extra
deps), reject and close on failure, bind remote traffic only after auth.
TLS is deferred (would break the zero-dependency rule unless optional).

### 4. Credit-based flow control on the wire

`t_flowcontrol` exists but is not attached to `t_server` connections. Attach
per-session credits to `PUSH` so a slow consumer cannot fill `t_conn` send
buffers (already capped at 64 MiB).

### 5. Pull consume + ack on the wire

Queue `ack`/`nack` is pull-inflight only. Network `PUSH` is fire-and-forget.
A later `T_MSG_CONFIRM` should map to inflight ack once a pull-mode consumer
API exists on the session.

### 6. Windows / non-x86_64

IOCP sources exist; POSIX modules and coroutine assembly do not. Keep CI on
Linux/macOS until those fallbacks exist.

### 7. Exclusive / autodelete queue flags

Flags are stored on `t_queue` and ignored. Exclusive should refuse a second
consumer connection; autodelete should drop the queue when the last network
session closes it.

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
