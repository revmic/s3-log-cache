# S3 Log Cache
> Caches AWS access logs in a sqlite database for easier querying.

S3 log buckets can easily grow to millions of objects within the bucket, so there are options to initially populate a database and then run this as a reocurring job in order to keep the database updated, e.g.,

Initial run:
```ssh
python cache-logs.py --initdb --bucket=mybucket-logs --database=mydb.sql
```

Daily cron job:
```ssh
python cache-logs.py --bucket=mybucket-logs --database=mydb.sql --date=2018-01-10
```

You can run this without the --date parameter at any time to check that you have all logs cached, e.g., if there is a period missing due to a service outage. This keeps track of what logs have been processed so no duplicate entries should be created.
