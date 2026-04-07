# FlexQL

FlexQL is a small SQL-like client/server database written in C++20. It supports:

- `CREATE TABLE`
- `INSERT INTO`
- `SELECT`
- `WHERE` with one condition
- `INNER JOIN`

The implementation focuses on persistent storage first:

- table schemas are stored on disk in `data/*.schema`
- inserts are first written durably to `data/flexql.wal`
- rows are appended durably to `data/*.data`
- every insert is WAL-synced before table files are updated
- indexes are rebuilt from disk on startup

## Build

```bash
make
```

This produces:

- `./flexql_server`
- `./flexql_repl`

## Run

Start the server:

```bash
./flexql_server 9000 data
```

Connect with the REPL:

```bash
./flexql_repl 127.0.0.1 9000
```

## Example

```sql
CREATE TABLE users (id DECIMAL PRIMARY KEY, name VARCHAR);
INSERT INTO users VALUES (1, 'Alice');
SELECT * FROM users;
SELECT name FROM users WHERE id = 1;
```

## Expiration

Rows default to no expiration. You can optionally attach an expiration value:

```sql
INSERT INTO users VALUES (2, 'Bob') EXPIRES AT '2030-01-01 00:00:00';
```

Accepted expiration formats:

- epoch seconds
- `YYYY-MM-DD HH:MM:SS`

Expired rows are skipped during reads.
