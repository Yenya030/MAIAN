# Contract SQLite Descender

A minimal loader (`contract_sqlite_descender.py`) was added. It downloads
contract data starting from the newest block and works backwards until the
SQLite database reaches a user specified size. The script supports flags for
selecting the database name (`--db`), the size limit in megabytes
(`--size-limit`) and how many rows to fetch per chunk (`--page-rows`).

A tiny curses GUI is available via `--gui`; it shows the lowest processed block
as the loader runs.

Unit tests verify reverse insertion order and that the CLI uses the default
Parquet dataset when started with the GUI flag.
