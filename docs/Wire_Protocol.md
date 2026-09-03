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
u64 msg_id     /* per-connection sequence, not a durable offset */
u8  priority
u16 name_len
u8  name[name_len]
u32 data_len
u8  data[data_len]
```

Push delivery is fire-and-forget (same as in-process consumer callbacks).

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

Accepted as liveness if the queue is open; not yet mapped to pull-inflight
`t_queue_ack`.

### `HEARTBEAT` / `NOP`

Empty payload. Server replies `ACK` and refreshes the session timestamp.

### `CLUSTER`

Not accepted on the client port (connection closed).

## Server safety switches

| Control | Default | Effect |
|---------|---------|--------|
| Bind host | `127.0.0.1` | Loopback unless the operator passes another IPv4 |
| `max_conns` | 1024 | Extra accepts are closed immediately |
| Token bucket | 128 burst, 50/s | Excess frames get `ACK` `T_ERR_BUSY` |
| Idle timeout | 30s | Session inactivity closes the socket |
| Send buffer | 64 MiB | Existing `t_conn` cap against slow peers |

`0.0.0.0` is allowed only when set explicitly (CLI/config). Admin HTTP
already binds `127.0.0.1`.

## Client API

- `t_client_connect` — in-process stub (tests and local fanout).
- `t_client_dial(client, loop, host, port)` — real TCP using `t_conn`.
- `t_client_open_queue(..., T_CLIENT_OPEN_PRODUCER \| T_CLIENT_OPEN_CONSUMER)`.
- `t_client_last_status()` — last decoded `ACK` status.

Drive the same `t_evloop` that owns the server (or a dedicated client loop)
so `PUSH`/`ACK` are read.
