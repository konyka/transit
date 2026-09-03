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
  `[storage] datadir=`). Missing datadir returns `T_ERR_IO`. Combined with
  `BROADCAST` returns `T_ERR_INVALID`. WAL path is
  `{datadir}/{domain}.{queue}.wal` (file mode `0600`, cap 256 MiB, default
  fsync every 32 records).
- `T_QUEUE_FLAG_EXCLUSIVE` refuses a second consumer with `T_ERR_BUSY`.
- `T_QUEUE_FLAG_AUTODELETE` deletes the queue when the last network session
  closes it (or disconnects).

Client helper: `t_client_open_queue` packs mode in the low 8 bits and
`T_CLIENT_QFLAG_*` in the high 8 bits so they do not collide.

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

`CONFIRM` calls `t_queue_ack` for a matching inflight `PUSH` on this
session (WAL `DEL` for durable queues). `REJECT` calls `t_queue_nack` and
the server immediately tries to `PUSH` again. Unknown ids still return a
credit (capped at the window) and `ACK` `T_OK`. Disconnect nacks leftover
inflight. These frames skip the per-connection token bucket. Broadcast
`PUSH` is not inflight — confirm only returns a credit. The TCP client
(`t_client_dial`) sends `CONFIRM` automatically after a decoded `PUSH`.

### `HEARTBEAT` / `NOP`

Empty payload. Server replies `ACK` and refreshes the session timestamp.

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
u8 rpc   /* 1 VoteReq, 2 VoteResp, 3 AppendReq, 4 AppendResp */
```

`VoteReq`: `u64 term, candidate_id, last_log_index, last_log_term`  
`VoteResp`: `u64 term`, `u8 granted`  
`AppendReq`: `u64 term, leader_id, prev_log_index, prev_log_term, leader_commit`,
`u32 n`, then `n` entries of `u64 index, term`, `u8 type`, `u32 len`, `data`.  
`AppendResp`: `u64 term`, `u8 success`, `u64 match_index`.

Handle with `t_raft_rpc`. Durable term/vote/entries: `t_raft_open_log`.

Follower `POST` on the client port returns `T_ERR_AGAIN` when a cluster is
attached and this node is not leader. The ACK name is `host_port` of the
current leader cluster node (underscore because `:` is not a valid name
character). Empty name if no leader is known.

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

## Client API

- `t_client_connect` — in-process stub (tests and local fanout).
- `t_client_dial(client, loop, host, port)` — real TCP using `t_conn`.
  If `t_client_set_psk` was called, the first frame is `T_MSG_AUTH`.
- `t_client_open_queue(..., T_CLIENT_OPEN_PRODUCER \| T_CLIENT_OPEN_CONSUMER)`.
  High byte: `T_CLIENT_QFLAG_DURABLE` / `EXCLUSIVE` / `AUTODELETE`.
- `t_client_last_status()` — last decoded `ACK` status.
- `t_client_last_ack_name()` — last decoded `ACK` name (`host_port` on
  follower `POST` `T_ERR_AGAIN`).
- After each `PUSH`, the dialed client sends `CONFIRM` so the server
  credit window can refill.

Drive the same `t_evloop` that owns the server (or a dedicated client loop)
so `PUSH`/`ACK` are read.
