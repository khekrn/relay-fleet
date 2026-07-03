# Fleet benchmark — ASYNC_STICKY vs ASYNC_DISTRIBUTED

**Setup:** 5 workers (own gRPC channels, `max_concurrency=16` each) alternating across **2 relay-server
instances** (`:9090`, `:9091`) on one shared Postgres. Workflow (identical in both modes, loan-approval-style):
`validate → PARALLEL[creditScore, fraudCheck, kycCheck] → decide` — 5 tasks/run, each simulating 8ms of work.
200 runs per mode, 10 concurrent submitters, 20-run warmup. Latency = submit → COMPLETED
(sticky: the `runAsync` round trip; distributed: fire-and-forget + 20ms status sampling, so distributed numbers
carry ≤20ms measurement noise). Machine: local dev laptop, 2026-07-03, schema V9.

## Work spreading (the point of distributed mode)

30 fire-and-forget runs = 150 task executions, pulled competitively by 5 pollers:

```
fleet-w1: 33   fleet-w2: 30   fleet-w3: 32   fleet-w4: 19   fleet-w5: 36
```

Benchmark phase (1000 executions): `w1=231 w2=191 w3=175 w4=183 w5=220` — evenly spread, no coordination,
purely competitive claims. In sticky mode each submitter executes all 5 tasks of its own runs.

## Latency / throughput

Poll cadence 100ms (conservative default):

| mode | runs | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| ASYNC_STICKY | 200 | 200 | 40.3 | 46.7 | 50.3 | 104.7 | 105.5 | 1.00s | **201.0** |
| ASYNC_DISTRIBUTED | 200 | 200 | 170.2 | 370.4 | 516.4 | 1676.7 | 1751.6 | 8.70s | 23.0 |

Poll cadence 25ms (`-Dfleet.poll-ms=25`):

| mode | runs | ok | min | p50 | p90 | p99 | max | makespan | runs/s |
|---|---|---|---|---|---|---|---|---|---|
| ASYNC_STICKY | 200 | 200 | 43.2 | 48.7 | 84.8 | 114.1 | 114.8 | 1.13s | 177.6 |
| ASYNC_DISTRIBUTED | 200 | 200 | 101.2 | 221.6 | 312.6 | 423.5 | 453.8 | 4.69s | **42.6** |

(all values ms unless noted; 0 failures in every phase — including the tx deadlock-retry machinery holding
under 10-way concurrent parallel-join completions against 2 competing servers)

## Reading the numbers

- **Sticky wins raw latency** — the submitter drives every wave itself with zero queue hops: p50 ≈ compute
  (3 sequential waves × 8ms) + gRPC round trips.
- **Distributed latency = queue cadence, not compute.** Each of the 3 waves waits for a poller pull, so the
  floor is ~3 × (pull wait) + compute; the p99 tail is queue depth: 1000 dispatches drained at
  ~(workers × batch 16) per poll tick. Halving the poll interval roughly halved p50 and cut p99 4×.
- **What distributed buys:** fire-and-forget submits (the submitter is free immediately), fleet-wide load
  spreading, worker interchangeability (any worker crash → others pull its redelivered work), and capacity
  capping per worker. The cost is pull-cadence latency — tune `poll-ms` per deployment, or move to
  server-streaming push delivery if sub-50ms distributed latency is ever required.

Reproduce: two servers up (`:9090` + `:9091`), then `java [-Dfleet.poll-ms=25] -jar target/relay-fleet-1.0.0.jar`.

---

# JVM / Netty / spring-gRPC tuning pass (2026-07-03)

**Changes measured** (all @25ms poll, 200 runs/mode, 10 submitters, 2 servers):
- **Server executor → virtual-thread-per-call** (`GrpcServerExecutorProvider`): Relay handlers block on JDBC
  (one event = one tx); gRPC's default shared executor serialized them under concurrency (everything ran on
  `grpc-default-executor-0`). This was the bottleneck.
- Netty: `TCP_NODELAY` child option, `SO_BACKLOG 256` (`ServerBuilderCustomizer<NettyServerBuilder>`).
- Server keepalive props (`spring.grpc.server.keepalive.permit.*`) + client channels keep-alive 30s +
  vthread callback executor (SDK auto-config + fleet).
- Hikari `maximum-pool-size 32` (with vthreads, the DB pool IS the concurrency limit — default 10 throttled).
- Fixed a silently-ignored property: `spring.grpc.server.shutdown-grace-period` → the real name is
  `spring.grpc.server.shutdown.grace-period`.

| variant | sticky p50 | p90 | p99 | runs/s | dist p50 | p99 | runs/s |
|---|---|---|---|---|---|---|---|
| pre-tuning baseline | 48.7 | 84.8 | 114.1 | 177.6 | 221.6 | 423.5 | 42.6 |
| **V1 code-tuned, default G1** | **44.8** | **50.5** | **95.4** | **211.9** | 218.9 | **388.5** | 42.7 |
| V2 = V1 + ZGC, 512m fixed, pretouch | 47.8 | 53.7 | 108.2 | 196.1 | 221.0 | 387.8 | 43.3 |
| V3 = V1 + G1, 512m fixed, compact headers | 45.4 | 57.9 | 100.2 | 195.8 | 235.3 | 415.0 | 40.7 |

**Reading:** the executor + pool fix is the real win — sticky **p90 −40% (84.8→50.5ms)**, p99 −16%,
throughput **+19%**, far outside run-to-run noise (±5–10%). GC/heap variants are noise at this scale: the
workload allocates little and the heap is small, so GC was never the bottleneck — the RPC executor and DB pool
were. ZGC / compact object headers are worth re-measuring under production heap sizes and allocation rates,
not here. Distributed mode barely moves in all variants — as established, its latency floor is poll cadence,
not server runtime.

**Kept as defaults:** V1 (code-level tuning, stock G1/heap). No JVM flags required.

## Benchmarking on Linux (checklist)

The macOS numbers above are valid for RELATIVE comparisons only. For capacity-planning numbers, rerun on
Ubuntu (or wherever production runs):
1. **epoll is now wired**: relay-server ships `netty-transport-native-epoll` (x86_64 + aarch_64) — on Linux the
   non-shaded grpc-netty server picks the native transport automatically (verify in the startup log; force with
   `spring.grpc.server.netty.transport` if needed). Workers get epoll free via grpc-netty-shaded.
2. Put Postgres on separate storage/host — one-event-one-transaction puts fsync latency inside handler latency.
3. Separate the bench client from the servers (everything shared one laptop here).
4. Re-measure the JVM variants (ZGC / compact object headers) at production heap sizes before trusting the
   "noise" verdict; disable CPU frequency scaling / use performance governor for stable tails.
