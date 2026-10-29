# Idempotent Background Transaction Processing Service

## Overview

This project is a small Java application that processes transaction requests from a CSV file. It is based on the Mphasis FDE L2 assessment.

The main objective is to make sure a transaction is not applied more than once, even if the same request is received again or a temporary failure occurs. The application also handles requests that arrive out of sequence, validates input, records processing status, and creates a summary report.

## Technology

- Java 17+
- Maven
- JUnit 5 for tests
- CSV input
- In-memory transaction state and ledger

The application uses in-memory storage to keep the setup simple for this assessment. Data is not retained after the application stops.

## Main features

- **Input validation:** Checks required fields, transaction type, amount, and other input rules. Invalid rows are reported without stopping the remaining batch.
- **Duplicate handling:** Uses `requestId` to identify a repeated delivery and `transactionId` to identify the business transaction. A repeated transaction does not create another ledger entry.
- **Payload conflict handling:** If a transaction ID is reused with different transaction details, the request is reported as a conflict rather than silently accepted.
- **Sequence handling:** Uses sequence numbers to process transactions in order. A transaction that arrives before an earlier sequence is available is kept as `PENDING`.
- **Retry handling:** Simulates temporary processing failures and retries them up to a configured limit.
- **Ledger and balance updates:** Keeps the ledger entry and balance update together in the in-memory implementation.
- **Reporting:** Prints the final status of each request, counts by status, and processing details. A JSON report can also be generated.

## Project structure

```text
src/main/java/com/fde/txnprocessor/
  App.java                  Application entry point
  ingest/                   Reads CSV or in-memory input
  validation/               Validates transaction requests
  model/                    Transaction data and status models
  store/                    Tracks requests and idempotency state
  ledger/                   Stores ledger entries and balances
  processing/               Processing rules, ordering and retries
  simulation/               Simulates temporary failures
  observability/             Logs, metrics and processing reports

src/test/java/               Unit and scenario tests
data/                        Sample CSV input files
sample-output/               Example reports and logs
docs/DESIGN.md                Design decisions and assumptions
```

## Prerequisites

Install JDK 17 or later and Maven 3.8 or later.

Check the installations:

```bash
java -version
mvn -version
```

## Build and test

Run the unit tests:

```bash
mvn test
```

Build the application:

```bash
mvn package
```

Run the sample input with one simulated temporary failure for `TXN-2005`:

```bash
java -jar target/txn-processor-1.0.0.jar data/sample-transactions.csv --fail-before-commit TXN-2005:1
```

The application prints the processing report to the console. Logs are written separately to standard error.

On Windows, `run.bat` is also provided. On Linux or macOS, use `run.sh`. These scripts require a compatible JDK.

## Sample scenarios

### 1. Process the assessment sample data

```bash
java -jar target/txn-processor-1.0.0.jar data/sample-transactions.csv --fail-before-commit TXN-2005:1
```

This scenario includes an out-of-order transaction, a duplicate, an invalid amount, and a transaction that succeeds after a retry.

### 2. Check retry exhaustion

```bash
java -jar target/txn-processor-1.0.0.jar data/sample-transactions.csv --fail-before-commit TXN-2005:5
```

This simulates repeated temporary failures until the configured retry limit is reached.

### 3. Run additional edge cases

```bash
java -jar target/txn-processor-1.0.0.jar data/edge-cases.csv --fail-after-commit TXN-3003:1 --fail-before-commit TXN-3006:5
```

This input covers additional cases such as a retry after a commit, insufficient funds, reversals, malformed input, and unresolved sequence gaps.

The retry count, retry delay, ordering mode, and optional JSON report path can be configured through command-line options. Use `--help` to see the available options.

## Input format

The CSV file uses the following columns:

```text
transactionId,requestId,sequenceNumber,accountId,transactionType,amount,currency,eventTime,originalTransactionId
TXN-2001,REQ-1,1,ACC-201,CREDIT,100,INR,2026-10-29T09:00:01Z,
```

Supported transaction types are `CREDIT`, `DEBIT`, and `REVERSAL`. A reversal must refer to an original transaction. This version supports full reversals only.

## Design summary

- `transactionId` identifies the business transaction and prevents the same transaction from being posted twice.
- `requestId` identifies a particular request delivery.
- Sequence numbers are used to determine processing order. The default mode uses a global sequence; per-account ordering is also available.
- Temporary failures are retried up to the configured limit. Business validation failures are not retried automatically.
- Processing states and failure details are included in the report.
- The current implementation is in-memory and intended for the assessment, not as a production banking system.

More detail, including assumptions, trade-offs, and possible production improvements, is available in [`docs/DESIGN.md`](docs/DESIGN.md).

## Before submitting

1. Run `mvn test` and review any failures.
2. Run the sample scenarios and check that the output matches the expected behavior.
3. Read through the main classes so you can explain how validation, idempotency, ordering, and retry handling work.
4. Update the AI-assistance section in `docs/DESIGN.md` so it accurately reflects how you used AI and what you personally reviewed and tested.
5. Push the project to a public GitHub repository and confirm the repository can be opened without signing in.
