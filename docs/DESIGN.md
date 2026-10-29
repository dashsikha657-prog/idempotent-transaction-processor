# Design Note: Idempotent Background Transaction Processing Service

## 1. Purpose

This application processes transaction requests that may be duplicated, received out of order, or retried after a temporary failure.

The main requirement is that one business transaction must not update the ledger or account balance more than once. The application also needs to make failed and pending requests visible so they can be investigated.

The implementation uses Java, CSV input, and in-memory storage to keep the assessment manageable. The interfaces are separated so that a database or message queue could be added later.

## 2. Application flow

The application follows this flow:

1. Read a record from the input source.
2. Validate the transaction fields.
3. Check whether the request or business transaction has already been received.
4. Check whether the transaction can be processed in sequence.
5. Apply the business rule and update the ledger.
6. Update the processing status and record the outcome.
7. Retry temporary failures within the configured limit.
8. Print a summary report.

Invalid records are recorded as failed and do not stop the rest of the batch.

## 3. Main components

| Component | Responsibility |
|---|---|
| `TransactionSource` | Provides records to process. The current implementation supports CSV and in-memory sources. |
| `RequestValidator` | Checks required fields and validates transaction data before processing. |
| `TransactionStateStore` | Tracks request status and identifies repeated requests or transaction IDs. |
| `SequenceGate` | Holds transactions that arrive before an earlier sequence number. |
| `TransactionProcessor` | Applies the business rules for credits, debits, and reversals. |
| `Ledger` | Stores ledger entries and maintains account balances. |
| `ProcessingEngine` | Coordinates validation, idempotency, ordering, processing, retries, and reporting. |
| `ProcessingReport` | Summarizes final statuses and processing details. |

The input source and business processing logic are separate. This makes it easier to replace the CSV input with a queue or API adapter later.

## 4. Transaction identifiers and idempotency

The request contains two identifiers with different purposes:

- `requestId` identifies a request delivery. If the same request is delivered again, the replay is ignored.
- `transactionId` identifies the business transaction. If the same transaction is sent with a different request ID, it is still recognised as a duplicate.

The state store compares the transaction details for requests that use the same transaction ID. If the details match, the later request is marked as `DUPLICATE`. If the details differ, it is treated as an idempotency conflict and failed.

The ledger also checks `transactionId` before adding a new entry. This is important for retry handling. For example, the ledger may have committed a transaction but the worker may not have received the acknowledgement. If the worker retries, the existing ledger entry is returned instead of applying the amount again.

The implementation uses `BigDecimal` for monetary values rather than `double`.

## 5. Handling requests received out of order

This solution uses sequence-based processing.

- If a request has the next expected sequence number, it can be processed.
- If its sequence number is higher than expected, it is marked `PENDING` until the missing earlier sequence arrives.
- If the sequence has already been used or is no longer valid, the request is rejected as a sequence conflict.

The default configuration uses one global sequence because the sample assessment data uses sequence numbers across the supplied accounts. The application also supports per-account sequence handling.

This approach is appropriate for ledger movements because order can affect the result. For example, a debit may fail if it is processed before the credit that funds it.

A missing sequence can leave later requests pending. In a production implementation, this would need monitoring and an agreed policy for resolving long-running gaps.

## 6. Retry and failure handling

The application distinguishes between temporary failures and failures that will not be fixed by retrying.

| Situation | Handling |
|---|---|
| Temporary processing failure | Mark the request `RETRY_PENDING` and retry it. |
| Retry limit reached | Mark the request `FAILED` with a retry-exhausted reason. |
| Invalid input | Mark the request `FAILED`; continue with the next record. |
| Insufficient funds or invalid reversal | Mark the request `FAILED`; do not retry automatically. |
| Same transaction ID with different details | Mark the request as an idempotency conflict. |

The maximum number of attempts and retry delay can be configured. Temporary failures are simulated by a wrapper around the ledger, so the normal business logic does not need to contain test-only failure conditions.

The retry process is safe because the ledger uses the transaction ID to avoid posting the same transaction more than once.

## 7. Status tracking and reporting

The application records the processing status and status history for each request. Depending on the outcome, a request may finish as `PROCESSED`, `DUPLICATE`, or `FAILED`, or remain `PENDING` if an earlier sequence has not arrived.

The report includes counts by status, request details, attempts, failure reasons, and sequence gaps. Logs identify important events such as duplicate detection, validation failures, successful processing, and scheduled retries.

Account identifiers are masked in logs and reports. Amounts are not written to the processing logs.

## 8. Data consistency

In this in-memory version, the ledger entry and balance change are performed together under the ledger's synchronization mechanism. The ledger checks constraints such as insufficient funds before applying a debit. It also prevents the same transaction from being posted more than once.

In a production system, the equivalent operations should be performed in one database transaction. A unique constraint on the transaction ID would provide an additional safeguard against duplicate postings.

The current application uses in-memory state, so processing state and balances are lost when the process stops. This is an intentional simplification for the assessment.

## 9. Tests

The test suite covers the main expected scenarios, including:

- Valid transaction processing.
- Repeated requests and duplicate transaction IDs.
- Reuse of a transaction ID with different details.
- Out-of-order transactions and missing sequence numbers.
- Invalid transaction data.
- Temporary failures, successful retries, and retry exhaustion.
- Ledger idempotency when a commit succeeds but the acknowledgement is lost.
- Reporting and status transitions.

Run the tests with:

```bash
mvn test
```

The test results should be checked locally before submission. Sample reports are included in the `sample-output` folder.

## 10. Assumptions and limitations

- The application uses in-memory storage; it does not retain state after restart.
- Sequence numbers are expected to start at 1 within the selected sequence scope.
- A transaction ID must not be reused for a different business transaction.
- Only full reversals are supported.
- The application runs as a single background worker. It does not connect to a real queue or database.
- Authentication, authorisation, deployment, and production monitoring are outside the scope of this exercise.

## 11. Possible production improvements

If this service were to be used in a production environment, I would consider the following improvements:

1. Store transaction state, ledger entries, and balances in a relational database.
2. Add a real queue or API adapter and define how messages are acknowledged.
3. Use database transactions and unique constraints to protect ledger updates.
4. Add alerts for requests that remain pending because of missing sequence numbers.
5. Add a dead-letter and controlled redrive process for requests that exhaust retries.
6. Add structured application logging, metrics, tracing, and operational dashboards.
7. Add authentication, access controls, secret management, and retention rules.
8. Run load and recovery tests, including a restart between ledger commit and status update.

## 12. AI-assisted development

AI assistance was used during preparation of this assessment. The generated code and documentation should be reviewed against the requirements rather than accepted without checking.

Before submitting, I will verify the following locally:

- The project builds successfully.
- The automated tests pass.
- The sample input produces the expected statuses and balances.
- Duplicate requests and retries do not create duplicate ledger entries.
- I can explain the key classes, design choices, assumptions, and limitations.

**Note:** Please update this section to describe accurately which AI tools you used and which checks you personally completed. Do not claim a test or review was performed unless you have actually done it.
