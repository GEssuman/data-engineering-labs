
# Test Cases

| Test Scenario                                | Expected Outcome                        | Actual Outcome |
|---------------------------------------------|-----------------------------------------|----------------|
| CSV files are generated correctly           | New CSV files created with correct schema and data |                |
| Spark detects and processes new files       | Spark reads new CSVs and processes them |                |
| Data transformations are correct            | Transformed schema matches target table |                |
| Data is written into PostgreSQL without error | Data visible in PostgreSQL event_log table |                |
| Performance metrics within expected limits  | Latency and throughput within acceptable thresholds |                |
