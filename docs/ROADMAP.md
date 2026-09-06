# Roadmap

Transit already has in-process queues, broker/domain routing, framed TCP,
admin HTTP, and a simplified Raft object model. The gaps below are what still
separate that library from a production message bus.

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
- GitHub Actions `windows-latest` (VS 2022 x64) builds and runs ctest.
- `t_client_ack_seq`: tests wait for a decoded `ACK` instead of treating
  `last_status == 0` as success. That was the exclusive-consumer /
  autodelete flake on WSAPoll (OPEN had not been processed yet).

## Remaining (priority order)

None for the current library surface. TLS is still deferred (PSK AUTH
covers the loopback-default client port).

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
- TLS on the client or peer port
