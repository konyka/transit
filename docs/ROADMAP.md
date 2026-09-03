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
  frames.
- Cluster peer transport (`t_peer`): loopback-default listen (port 4223,
  or ephemeral), election timeouts, short-lived `T_MSG_CLUSTER` dials,
  and follower `POST` ACK names of the form `host_port` (colon is not a
  valid queue-name character). `transit-server -C` / `[cluster] port=` is
  opt-in.
- Client AUTH: first frame is `T_MSG_AUTH` with HMAC-SHA256 of a pre-shared
  key over `transit.auth.v1`. Wrong or missing MAC closes the connection.
  Binding off loopback without a PSK fails closed. TLS is deferred.
- Per-session PUSH credits (`t_flowcontrol`): default 64 outstanding
  `PUSH` frames. `CONFIRM`/`REJECT` return a credit and are not
  rate-limited. The TCP client auto-sends `CONFIRM` after each `PUSH`.
- Network FIFO/priority delivery is pull-inflight: `PUSH` carries the
  queue `msg_id`, `CONFIRM` maps to `t_queue_ack`, `REJECT` to
  `t_queue_nack`. Disconnect nacks leftover inflight so another consumer
  can take the message. Broadcast still uses fire-and-forget push.

## Remaining (priority order)

### 1. Windows / non-x86_64

IOCP sources exist; POSIX modules and coroutine assembly do not. Keep CI on
Linux/macOS until those fallbacks exist.

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
