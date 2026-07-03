package com.example.fleet;

import com.example.fleet.Model.*;
import com.cs.relay.worker.Worker;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * Typed task implementations, registered onto EVERY fleet worker. Each execution is tagged with — and counted
 * against — the worker that ran it, so the report can show how work spread across the fleet. Each task sleeps
 * {@code WORK_MS} to model real task latency (that's what makes spreading matter).
 */
public final class FleetTasks {

    static final long WORK_MS = 8;

    /** taskName -> workerId -> executions (the spreading evidence). */
    static final ConcurrentMap<String, ConcurrentMap<String, LongAdder>> EXECUTIONS = new ConcurrentHashMap<>();

    private static void count(String task, String workerId) {
        EXECUTIONS.computeIfAbsent(task, t -> new ConcurrentHashMap<>())
                  .computeIfAbsent(workerId, w -> new LongAdder()).increment();
    }
    private static void work() {
        try { Thread.sleep(WORK_MS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    // ---- the typed handlers -----------------------------------------------------------------------
    static Validated validate(LoanIn in, String by)   { work(); return new Validated(in.applicant(), true, by); }
    static Score creditScore(LoanIn in, String by)    { work(); return new Score(in.applicant(), 640 + Math.abs(in.applicant().hashCode() % 200), by); }
    static Fraud fraudCheck(LoanIn in, String by)     { work(); return new Fraud(in.applicant(), true, by); }
    static Kyc kycCheck(LoanIn in, String by)         { work(); return new Kyc(in.applicant(), true, by); }
    static Decision decide(LoanIn in, String by)      { work(); return new Decision("APPROVED", in.amount(), by); }

    /** bind all tasks onto a worker, tagging outputs + counters with that worker's id. */
    static void registerOn(Worker w, String workerId, ObjectMapper json) {
        w.task("validate",    (in, info) -> tag(json, workerId, "validate",    validate(json.convertValue(in, LoanIn.class), workerId)));
        w.task("creditScore", (in, info) -> tag(json, workerId, "creditScore", creditScore(json.convertValue(in, LoanIn.class), workerId)));
        w.task("fraudCheck",  (in, info) -> tag(json, workerId, "fraudCheck",  fraudCheck(json.convertValue(in, LoanIn.class), workerId)));
        w.task("kycCheck",    (in, info) -> tag(json, workerId, "kycCheck",    kycCheck(json.convertValue(in, LoanIn.class), workerId)));
        w.task("decide",      (in, info) -> tag(json, workerId, "decide",      decide(json.convertValue(in, LoanIn.class), workerId)));
    }

    private static Object tag(ObjectMapper json, String workerId, String task, Object out) {
        count(task, workerId);
        return json.convertValue(out, Map.class);
    }

    static void resetCounters() { EXECUTIONS.clear(); }

    /** total executions per worker across all tasks (for the distribution table). */
    static Map<String, Long> perWorkerTotals() {
        Map<String, Long> totals = new java.util.TreeMap<>();
        EXECUTIONS.values().forEach(byWorker ->
                byWorker.forEach((w, n) -> totals.merge(w, n.sum(), Long::sum)));
        return totals;
    }

    private FleetTasks() {}
}
