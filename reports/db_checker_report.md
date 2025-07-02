# Database Checker Report

The new `db_checker.py` script scans contracts stored in a SQLite database using the
existing Maian checks. A small test database containing two dummy contracts was
created and scanned. No vulnerabilities were found.

```
$ python tool/db_checker.py --db sample.db --limit 2
```

Progress output during the run showed the number of contracts processed and the
current vulnerability counts. The resulting summary was saved to
`reports/db_scan_report.json`:

```json
{
  "total": 2,
  "scanned": 2,
  "suicidal": 0,
  "prodigal": 0,
  "greedy": 0
}
```
