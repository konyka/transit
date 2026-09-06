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

## Remaining (priority order)

1. **Windows `cl` `/W4 /WX`** — GitHub `windows` must compile and run
   ctest. `t_coro_create` on Windows is fail-closed (`NULL`); the
   create path must not leave unreachable code after
   `#if !T_HAVE_CORO_ASM` (MSVC C4702 / C2220).
2. **Consumer groups over TCP** — `t_cgroup` is in-process only
   (`test_cgroup`). There is no `JOIN` frame. Design: new `T_MSG_JOIN`
   (not trailing bytes on `OPEN` — those are rejected). Payload is
   group / consumer / queue names (same charset as queues). Hot path
   stays one `PUSH` to the chosen session; round-robin is O(1) in
   `t_cgroup`. Fail closed: unknown names, duplicate consumer id,
   JOIN without consumer `OPEN`, and disconnect removes the member.
   Empty group refuses dispatch. PUSH credits still apply.
3. **Raft** is still a simplified object model plus peer RPC, not a
   full replicated log for durable queues.

TLS is still deferred (PSK AUTH covers the loopback-default client
port).

## Non-goals for now

- New languages / client SDKs
- Compression
- Cross-datacenter WAN mesh
- TLS on the client or peer port
