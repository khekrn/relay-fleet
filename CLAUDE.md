# CLAUDE.md

**relay-fleet** — 5 workers × 2 relay-server instances: ASYNC_DISTRIBUTED work-spreading demo + the
STICKY-vs-DISTRIBUTED benchmark (p50/p90/p99 + throughput). The engine repo is `khekrn/relay`; READ ITS
CLAUDE.md FIRST. All measured results + the JVM/Netty tuning tables + the **Linux benchmarking checklist**
live in `RESULTS.md` here — on Ubuntu, that checklist is the intended next step.

Run: bring up BOTH servers (:9090 and :9091 — second via
`SPRING_APPLICATION_JSON='{"spring":{"grpc":{"server":{"port":9091}}},"server":{"port":8081}}'`), then
`mvn clean package -DskipTests && java [-Dfleet.poll-ms=25] -jar target/relay-fleet-1.0.0.jar`.

Notes: the 5 workers are constructed MANUALLY (RelayWorkerAutoConfiguration is excluded in FleetApplication);
distributed latency floor = poll cadence × ~3 waves (the `-Dfleet.poll-ms` knob), not compute; macOS numbers
in RESULTS.md are relative-only — user decision: NO server-streaming push delivery.
