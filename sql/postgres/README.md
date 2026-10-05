# sql/postgres/

Init scripts for the source (OLTP) database. Docker runs every `*.sql` here, alphabetically,
**once**, when the `pgdata` volume is first created.

| File | Purpose |
|---|---|
| `001_schema.sql` | `sales` schema: customers, products, orders, order_items, payments |
| `002_indexes.sql` | FK indexes and `updated_at` indexes (incremental-extraction watermark) |
| `003_triggers.sql` | `BEFORE UPDATE` trigger so `updated_at` can never be forgotten |

To re-run them after changing a script: `docker compose down -v` (deletes the data volume), then
`docker compose up -d postgres` and reload with `python -m data_generation.generate postgres`.

Inspect: `docker compose exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB` then `\dt sales.*`.
