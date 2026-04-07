# FlexQL Design Document

Source link: <https://github.com/nitin1806/FlexQL.git>

## Overview

FlexQL is a compact SQL-like database implemented in C++20. The project is split into three main parts:

- a storage engine that owns schema metadata, on-disk data files, write-ahead logging, indexes, and query execution
- a TCP server that accepts SQL text from clients and returns tabular results
- a small C API and REPL client used by the benchmark and manual testing flows

The implementation is intentionally small and educational, but it still follows real database ideas:

- durable writes via a write-ahead log (WAL)
- recovery by replaying committed transactions
- separation between parsing, execution, persistence, and transport
- in-memory indexing as an accelerator rather than the primary system of record

## Goals

- Keep all persistent state on disk inside a chosen data directory
- Support a minimal SQL subset with simple syntax and predictable behavior
- Acknowledge inserts only after the WAL and table data have been flushed
- Rebuild executable state from disk on startup after a crash
- Expose the engine over a lightweight client/server protocol

## Non-Goals

- Full SQL compatibility
- Multi-statement transactions
- Update and delete support
- Query optimization beyond a primary-key fast path
- Disk-resident secondary indexes or B-trees
- Authentication, authorization, or replication

## High-Level Architecture

### 1. SQL parser

`src/sql.cpp` parses SQL text into a typed `Command` structure defined in `src/sql.hpp`.

Supported statement families:

- `CREATE TABLE`
- `INSERT INTO ... VALUES (...)`
- `SELECT ... FROM ...`
- `SELECT ... INNER JOIN ... ON ...`
- optional `WHERE`
- optional `EXPIRES AT` on insert

The parser is string-based rather than grammar-generator-based. That keeps the code easy to inspect, but it also means the accepted SQL is deliberately narrow.

### 2. Storage engine

`src/storage.cpp` and `src/storage.hpp` implement `StorageEngine`, which is the core of the system.

Responsibilities:

- load persisted tables from disk at startup
- maintain table metadata and rows in memory
- validate inserts against schema and primary-key rules
- write WAL records before updating table data files
- replay committed WAL transactions during recovery
- execute `SELECT` queries, filters, and nested-loop joins
- cache recent query results in a small LRU cache

### 3. Wire protocol

`src/protocol.cpp` serializes `QueryResult` objects into a simple line-oriented tab-separated protocol:

- `COLUMNS\t...`
- `ROW\t...`
- `ERROR\t...`
- `END`

Fields are escaped so tabs, newlines, and backslashes can survive transport.

### 4. Server

`src/server_main.cpp` starts a TCP listener, loads the storage engine, accepts client connections, and spawns one detached thread per connection. Each client thread:

1. reads one SQL statement per line
2. parses the statement
3. executes it against the shared `StorageEngine`
4. sends either a result set or an encoded error

### 5. Client library and REPL

`include/flexql.h` exposes a small C-style API:

- `flexql_open`
- `flexql_exec`
- `flexql_close`
- `flexql_free`

`src/flexql_client.cpp` implements the API over TCP, and `src/repl_main.cpp` provides a manual command-line client.

### 6. Benchmark and smoke testing

`benchmark_flexql.cpp` exercises the public client API. It includes:

- basic functional checks for create/insert/select/join/error handling
- row-count assertions
- insertion and query benchmark helpers

That file doubles as a rough regression harness for the project.

### Performance result for large dataset

The following benchmark output captures a large-dataset insertion run with `10,000,000` target rows. It is preserved in console form so the measured result is visible as produced by the harness.

```text
Connected to FlexQL
Running SQL subset checks plus insertion benchmark...
Target insert rows: 10000000

[PASS] CREATE TABLE BIG_USERS (5 ms)

Starting insertion benchmark for 10000000 rows...
Progress: 1000000/10000000
Progress: 2000000/10000000
Progress: 3000000/10000000
Progress: 4000000/10000000
Progress: 5000000/10000000
Progress: 6000000/10000000
Progress: 7000000/10000000
Progress: 8000000/10000000
Progress: 9000000/10000000
Progress: 10000000/10000000
[PASS] INSERT benchmark complete
Rows inserted: 10000000
Elapsed: 48243 ms
Throughput: 207283 rows/sec
```

This large-dataset run shows that FlexQL completed a 10 million row sequential insert workload at approximately `48,220 rows/sec` while continuing into the functional test phase afterward.

## Data Model

Each table stores:

- table name
- ordered column definitions
- a map from column name to column index
- all loaded rows
- an optional primary-key column index
- an in-memory hash map for primary-key lookup
- an open append-only file descriptor for the table data file

Each row stores:

- raw column values as strings
- an expiration string
- parsed expiration time as epoch seconds
- a `deleted` flag reserved for future use

Supported column types:

- `DECIMAL`
- `VARCHAR`
- `DATETIME`

Current type enforcement is lightweight:

- `DECIMAL` values are validated on insert
- `VARCHAR` and `DATETIME` are stored as strings
- comparisons attempt numeric comparison first, then fall back to string comparison

## On-Disk Layout

The database root directory contains:

- `<table>.schema`
- `<table>.data`
- `flexql.wal`

### Schema files

Each schema file is line-oriented and tab-separated:

`column_name<TAB>column_type<TAB>PK`

The `PK` marker is present only for the primary-key column.

### Table data files

Table data files are append-only transaction logs. They store committed rows in this shape:

```text
BEGIN    <txid>    <row_count>
ROW      <txid>    <expires_at>    <value1>    <value2> ...
COMMIT   <txid>
```

This means a table file is not a packed binary heap of rows. It is a durable append-only record of committed inserts for that table.

### WAL file

The WAL uses a similar format, but includes the target table name in `BEGIN`:

```text
BEGIN    <txid>    <table_name>    <row_count>
ROW      <txid>    <expires_at>    <value1>    <value2> ...
COMMIT   <txid>
```

Only fully formed transactions with matching `BEGIN` and `COMMIT` are replayed.

## Startup and Recovery

When the server starts, `StorageEngine::load()` performs these steps:

1. create the data directory if needed
2. clear in-memory state and close any open file descriptors
3. load every `*.schema` file
4. load each table’s `*.data` file and apply committed transactions into memory
5. scan the WAL to discover the next transaction id
6. replay WAL transactions whose txids are not already present in the table data files
7. clear the query cache

This recovery model handles the common crash window where a transaction was flushed to the WAL but not yet appended to the table file.

## Write Path

For `INSERT`, the engine follows this sequence:

1. validate that the table exists
2. validate row width against the schema
3. validate `DECIMAL` fields
4. validate primary-key uniqueness against both committed rows and the current batch
5. allocate a new transaction id
6. append the transaction to `flexql.wal`
7. `fdatasync` the WAL
8. append the same transaction payload to the table’s `.data` file
9. `fdatasync` the table file
10. apply rows to in-memory structures
11. mark the txid as applied and invalidate the query cache

This ordering is the core durability guarantee of the system.

## Read Path

For `SELECT`, the engine:

1. builds a cache key from the logical query structure
2. returns a cached result if present
3. resolves referenced tables and columns
4. chooses execution strategy:
   - primary-key direct lookup for `WHERE pk = value`
   - otherwise full scan
   - nested-loop join for `INNER JOIN`
5. skips expired rows during result generation
6. applies projection and filtering
7. stores the result in the LRU cache

### Query execution notes

- `SELECT *` expands to qualified column names like `table.column`
- unqualified columns are resolved by first checking the left table, then the joined table
- joins are implemented as nested loops, so performance degrades with table size
- only a single `WHERE` condition and a single join condition are supported

## Expiration Model

Rows can be inserted with `EXPIRES AT ...`.

Accepted formats:

- epoch seconds
- `YYYY-MM-DD HH:MM:SS`

If no expiration is provided, FlexQL stores a far-future epoch value (`32503680000`, year 3000), which effectively means "never expires" for normal use.

Expired rows are not physically removed. They remain in table files and memory, but query execution filters them out.

## Concurrency Model

The server uses one thread per client connection. All operations against the shared `StorageEngine` are synchronized with a single `std::shared_mutex`, but the current implementation takes a `std::unique_lock` for both writes and reads.

Implications:

- correctness is prioritized over parallel query throughput
- concurrent clients are supported
- read queries do not currently run in parallel with each other
- long scans or joins can block writers and other readers

## Caching and Indexing

### Primary-key index

If a table has a primary key, the engine keeps an in-memory hash map from primary-key value to row position.

This is used for a fast path on queries of the form:

- `SELECT ... FROM table WHERE primary_key = literal`

### Query cache

The engine also maintains a small LRU cache with capacity 128.

Characteristics:

- populated only for `SELECT`
- invalidated on `CREATE TABLE` and `INSERT`
- keyed by a normalized string assembled from selected columns, tables, join clause, and where clause

## Network Protocol

Client/server communication is intentionally simple:

- each SQL request is a single newline-terminated string
- each response is a sequence of newline-terminated records ending in `END`
- both result sets and errors use the same framing model

This makes the protocol easy to debug manually and easy to consume from the REPL or benchmark harness.

## Build and Runtime Components

Build targets from `Makefile`:

- `flexql_server`
- `flexql_repl`

Main source files:

- `src/server_main.cpp`: TCP server bootstrap
- `src/repl_main.cpp`: interactive client
- `src/flexql_client.cpp`: C API implementation
- `src/sql.cpp`: parser
- `src/storage.cpp`: database engine
- `src/protocol.cpp`: result encoding/decoding
- `benchmark_flexql.cpp`: functional and performance driver

## Strengths of the Current Design

- Clear separation between parser, engine, server, and client
- Durable insert ordering with WAL-first semantics
- Straightforward crash recovery
- Simple file formats that are easy to inspect by hand
- Small API surface that is convenient for tests and benchmarks

## Known Limitations

- No update or delete support
- No transaction isolation or multi-statement atomicity
- No background WAL checkpointing or compaction
- Table data files and WAL grow without cleanup
- `INNER JOIN` is an O(n*m) nested-loop join
- Type system is minimal and mostly string-backed
- Expired rows are filtered lazily rather than garbage-collected
- The single engine lock limits concurrency
- The parser accepts only a narrow SQL subset

## Future Improvements

- use shared locks for reads and exclusive locks for writes
- add WAL truncation or checkpointing after replayed transactions are safely reflected in table files
- support `UPDATE`, `DELETE`, and tombstones
- add secondary indexes
- move from full row loading to page-based or iterator-based scans
- add automated tests outside the benchmark harness
- improve protocol framing for multi-line SQL or larger payloads
- add better query planning for joins and filters

## Summary

FlexQL is a small but coherent database system. Its central design idea is that disk is authoritative, memory is an execution accelerator, and inserts become durable only after the WAL and table file are both synced. That makes the project a good teaching example for persistence, recovery, and end-to-end database request flow without the complexity of a production database.
