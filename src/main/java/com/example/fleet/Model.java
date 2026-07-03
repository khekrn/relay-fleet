package com.example.fleet;

/** Typed task payloads for the loan-approval-style fleet workflow. */
public interface Model {
    record LoanIn(String applicant, int amount) {}
    record Validated(String applicant, boolean ok, String by) {}
    record Score(String applicant, int score, String by) {}
    record Fraud(String applicant, boolean clear, String by) {}
    record Kyc(String applicant, boolean verified, String by) {}
    record Decision(String outcome, int amount, String by) {}
}
