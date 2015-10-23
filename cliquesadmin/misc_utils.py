from datetime import datetime, timedelta
import argparse
from time import mktime
from feedparser import _parse_date as parse_date


def rfc3339_to_datetime(timestamp):
    stime = parse_date(timestamp)
    tfloat = mktime(stime)
    return datetime.fromtimestamp(tfloat)


def datetimearg(s):
    return datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


def parse_hourly_etl_args(etl_name):
    parser = argparse.ArgumentParser(description='Runs the %s ETL from BigQuery to MongoDB for '
                                                 'a given time range' % etl_name)
    parser.add_argument('--start',
                        help='Start of ETL range. UTC datetime hour w/ format %Y-%m-%d %H:%M:%S. '
                             'Range is open-ended, i.e. inclusive of start datetime',
                        type=datetimearg)
    parser.add_argument('--end',
                        help='End of ETL range. UTC datetime hour w/ format %Y-%m-%d %H:%M:%S. '
                             'Range is open-ended, i.e. exclusive of end datetime',
                        type=datetimearg)
    args = parser.parse_args()
    now = datetime.utcnow()
    end = datetime(now.year, now.month, now.day, now.hour, 0, 0)
    if args.start is None:
        args.start = end - timedelta(hours=1)
    if args.end is None:
        args.end = end

    if args.start > args.end:
        raise ValueError('Start cannot be after end')

    return args