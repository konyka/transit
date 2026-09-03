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
- Durable queues: append-only WAL (`{datadir}/{domain}.{queue}.wal`), PUT
  before enqueue, DEL after push deliver or pull ack. Default fsync every 32
  records. Fail closed without a datadir. `DURABLE+BROADCAST` is rejected.
- Exclusive queues refuse a second consumer (`T_ERR_BUSY`). Autodelete drops
  the queue when the last network session closes it.
- Raft RPCs (`RequestVote` / `AppendEntries`) as `T_MSG_CLUSTER` payloads,
  persistent raft log (`t_raft_open_log`), and leader-only publish when a
  cluster is attached to the broker. The client port still closes `CLUSTER`
  frames; peer transport is the next slice.

## Remaining (priority order)

### 1. Cluster peer transport (reliability)

`t_raft_rpc` is in-process. Needed: a loopback-default cluster listen port that
exchanges `T_MSG_CLUSTER` frames, election timeouts, and follower redirect
hints for clients (`T_ERR_AGAIN` is already returned on follower `POST`).

### 2. Authentication (security)

Client connections are unauthenticated. Plan: first frame after TCP is a
`T_MSG_OPEN_QUEUE`-style handshake with HMAC of a pre-shared key (no extra
deps), reject and close on failure, bind remote traffic only after auth.
TLS is deferred (would break the zero-dependency rule unless optional).

### 3. Credit-based flow control on the wire

`t_flowcontrol` exists but is not attached to `t_server` connections. Attach
per-session credits to `PUSH` so a slow consumer cannot fill `t_conn` send
buffers (already capped at 64 MiB).

### 4. Pull consume + ack on the wire

Queue `ack`/`nack` is pull-inflight only. Network `PUSH` is fire-and-forget.
A later `T_MSG_CONFIRM` should map to inflight ack once a pull-mode consumer
API exists on the session.

### 5. Windows / non-x86_64

IOCP sources exist; POSIX modules and coroutine assembly do not. Keep CI on
Linux/macOS until those fallbacks exist.

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
