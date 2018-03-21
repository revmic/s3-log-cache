from __future__ import division
import os
import sys
import csv
import time
import sqlite3
from optparse import OptionParser
from multiprocessing.dummy import Pool
from datetime import datetime, timedelta

import boto3
import botocore

'''
Caches AWS access logs in a sqlite database for easier querying.

S3 log buckets can easily grow to millions of keys within the bucket, so there
are options to initially populate a database and then run this as a reocurring
job in order to keep the database updated, e.g.,

Initial run:
python cache-logs.py --initdb --bucket=mybucket-logs --database=mydb.sql

Daily cron job:
python cache-logs.py --bucket=mybucket-logs --database=mydb.sql \
  --date=2018-01-10

You can run this without the --date parameter at any time to check that you
have all logs cached, e.g., if there is a period missing due to a service
outage. This keeps track of what logs have been processed so no duplicate
entries should be created.

'''

parser = OptionParser(usage='')
parser.add_option(
    "-i", "--initdb", action="store_true", dest="initdb", default=False,
    help='''Initialize the database or not. Backs up current database if
    initializing and it already exists.'''
)
parser.add_option(
    "-b", "--bucket", action="store", type="string", dest="bucket",
    help='Name of your S3 bucket containing logs.'
)
parser.add_option(
    "-d", "--database", action="store", type="string", dest="database",
    help='Full path to your sqlite3 database file.'
)
parser.add_option(
    "-t", "--date", action="store", type="string", dest="date",
    default=None, help='''Filters which logs to cache. To get a full year,
    use format YYYY, to get one day YYYY-MM-DD, etc.'''
)
parser.add_option(
    "-c", "--cache", action="store", type="string", dest="cache",
    default="/tmp", help='Cache space for downloaded S3 logs.'
)
parser.add_option(
    "-p", "--pages", action="store", type="int", dest="pages",
    default=None, help='Limits number of 1000 item pages to process.'
)
parser.add_option(
    "-w", "--workers", action="store", type="int", dest="workers",
    default=10, help='Number of download worker threads.'
)
(opts, args) = parser.parse_args()

if not (opts.bucket and opts.database):
    print("You must provide at least a bucket and database.\n")
    parser.print_help()
    sys.exit(-1)

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
conn = sqlite3.connect(opts.database, timeout=20)
cur = conn.cursor()

cache_dir = os.path.join(opts.cache, 's3cache')


def main():
    print("{} -- caching logs from {}".format(datetime.now(), opts.bucket))
    start = time.time()

    # Set up cache space for downloaded logs
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    # Drop and recreate tables
    if opts.initdb:
        init_db()

    # Get logs that haven't been cached yet
    logs = get_uncached_logs()
    print("{} considered logs uncached".format(len(logs)))
    # sys.exit()

    # Download and add each log to the database
    cache(logs)

    elapsed = time.time() - start
    print("{} minutes elapsed".format(int(elapsed/60)))
    # print("{} objects considered".format(counter))


def init_db():
    """ Backup db file and recreate it
    """
    # cur.execute("DROP TABLE IF EXISTS usage")
    # cur.execute("DROP TABLE IF EXISTS consumed_logs")
    # conn.commit()

    global conn, cur

    if os.path.exists(opts.database):
        os.rename(opts.database, opts.database + '.bak')

    conn = sqlite3.connect(opts.database)
    cur = conn.cursor()

    create_usage = '''CREATE TABLE usage (
        date,
        remote_ip,
        requester,
        operation,
        filename,
        http_status,
        error_code,
        size,
        total_time,
        referrer,
        user_agent
    )'''
    cur.execute(create_usage)

    create_consumed = '''CREATE TABLE consumed_logs (
        date,
        filename,
        size
    )'''
    cur.execute(create_consumed)

    conn.commit()


def get_uncached_logs():
    """ Returns a set of logs that have not yet been cached
        i.e., s3 listing minus logs already cached
    """

    m = 'Getting S3 list of logs, filtering on "{}". This can take some time'
    print(m.format(opts.date))

    # list all files in logs on s3
    s3_logs = get_s3_keys()
    # print(s3_logs[:10])
    print("{} s3 logs considered".format(len(s3_logs)))

    # get list of what we've already consumed from db
    query = cur.execute("SELECT filename FROM consumed_logs")
    results = query.fetchall()
    consumed_logs = []
    for r in results:
        consumed_logs.append(r[0])
    # print(consumed_logs[:10])
    print("{} logs already cached".format(len(consumed_logs)))

    # need to find any leading directory and prepend back to keys
    dirname = os.path.dirname(s3_logs[0])
    # subtract the two and that's what needs cached
    s3_logs_base = [os.path.basename(x) for x in s3_logs]
    new_logs = list(set(s3_logs_base) - set(consumed_logs))
    new_keys = [os.path.join(dirname, x) for x in new_logs]
    # print list(new_keys)

    return sorted(list(new_keys))


def get_s3_keys():
    """ Get a list of log files in S3 bucket
    :param opts.bucket (string) -- name of S3 bucket
    :param opts.date (string) -- format YYYY-MM-DD
    :returns keys for logs for given date if given or all if None
    :note -- this takes quite some time for all logs
    """
    start = time.time()

    prefix = ''
    if opts.date:
        prefix = 'logs/{}'.format(opts.date)

    kwargs = {'Bucket': opts.bucket, 'Prefix': prefix}
    keys = []
    i = 0

    while True:
        i += 1
        # print("listing page {}".format(i))
        resp = s3_client.list_objects_v2(**kwargs)

        for obj in resp['Contents']:
            keys.append(obj['Key'])

        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break

        # Break for page limit param
        if opts.pages and i >= opts.pages:
            break

    elapsed = time.time() - start
    print("listed {} pages in {} seconds".format(i, int(elapsed)))

    return keys


def cache(logs):
    """ Download, create db record, and delete each file
        Processes in baches for each operation for disk space and performance
    """

    total_logs = len(logs)
    limit = 1000
    i = 0

    while logs:
        batch = logs[:limit]
        del logs[:limit]

        percent = (i*limit/total_logs) * 100.0
        print("{:0.1f}% -- caching {} / {}".format(
            percent, i*limit, total_logs))

        #
        # Download a batch of uncached logs, using a thread pool
        #
        download_start = time.time()

        # for key in batch:
        #     print("downloading " + key)
        #     s3_download(key)
        pool = Pool(opts.workers)
        pool.map(s3_download, batch)
        pool.close()
        pool.join()

        download_elapsed = time.time() - download_start

        #
        # Cache log batch into database
        #
        database_start = time.time()

        for key in batch:
            insert_log_db(key)

        conn.commit()
        database_elapsed = time.time() - database_start

        #
        # Remove log files from local storage
        #
        delete_start = time.time()
        # for key in batch:
        #     filename = os.path.basename(key)
        #     filepath = os.path.join(cache_dir, filename)
        #     os.remove(filepath)
        pool = Pool(opts.workers)
        pool.map(delete_local_file, batch)
        pool.close()
        pool.join()

        delete_elapsed = time.time() - delete_start

        # print("downloaded {} files in {} seconds".format(
        #     limit, int(download_elapsed)))
        # print("cached {} files in {} seconds".format(
        #     limit, int(database_elapsed)))
        # print("deleted {} files in {} seconds".format(
        #     limit, int(delete_elapsed)))
        i += 1
        # sys.exit()


def s3_download(key):
    # print("downloading " + key)
    local_file = os.path.join(cache_dir, os.path.basename(key))
    try:
        s3_resource.Bucket(opts.bucket).download_file(key, local_file)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object ({}) does not exist.".format(key))
        else:
            raise


def delete_local_file(key):
    # print("deleting " + key)
    filename = os.path.basename(key)
    filepath = os.path.join(cache_dir, filename)
    os.remove(filepath)


def insert_log_db(key):
    # print("writing to db " + key)

    local_file = os.path.join(cache_dir, os.path.basename(key))

    # Create entry for consumed_logs so we don't process again
    consumed_sql = "INSERT INTO consumed_logs VALUES ('{}','{}','{}')"
    query = consumed_sql.format(
        time.ctime(), os.path.basename(key), os.stat(local_file).st_size)
    cur.execute(query)

    usage_sql = '''INSERT INTO usage VALUES (
        '{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')'''

    with open(local_file) as f:
        logreader = csv.reader(f, delimiter=' ', quotechar='"')

        for fields in logreader:

            date = fields[2][1:]
            remote_ip = fields[4]
            requester = fields[5]
            operation = fields[7]
            filename = fields[8]
            http_status = fields[10]
            error_code = fields[11]
            size = fields[13]
            total_time = fields[14]
            referrer = fields[16]
            user_agent = fields[17]

            # if 'COPY' not in operation:
            #     continue

            query = usage_sql.format(
                date,
                remote_ip,
                requester,
                operation,
                filename,
                http_status,
                error_code,
                size,
                total_time,
                referrer,
                user_agent
            )

            cur.execute(query)


if __name__ == "__main__":
    main()
