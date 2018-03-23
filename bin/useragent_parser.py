from user_agents import parse
import csv
import pandas as pd

with open('/Users/bliang/smartertravel-useragent-january-results.csv', 'rb') as f:
    spamreader = csv.DictReader(f)
    rows = []
    for index, row in enumerate(spamreader):
        ua = parse(row['ua'])
        row['browser_family'] = ua.browser.family
        row['browser_version'] = ua.browser.version
        row['device_family'] = ua.device.family
        row['device_brand'] = ua.device.brand
        row['device_model'] = ua.device.model
        row['os_family'] = ua.os.family
        row['os_version'] = ua.os.version
        if ua.is_bot:
            row['device_type'] = 'bot'
        elif ua.is_email_client:
            row['device_type'] = 'email_client'
        elif ua.is_mobile:
            row['device_type'] = 'mobile'
        elif ua.is_tablet:
            row['device_type'] = 'tablet'
        elif ua.is_pc:
            row['device_type'] = 'desktop'
        else:
            row['device_type'] = 'unknown'
        rows.append(row)
        # if index == 1000:
        #     break
        if index % 1000 == 0:
            print '%s rows parsed' % index
    print 'Finished parsing user-agent strings, now storing to DataFrame & re-grouping...'
    df = pd.DataFrame.from_dict(rows)
    df = df.fillna('unknown')
    df['imps'] = pd.to_numeric(df['imps'])
    df['bids'] = pd.to_numeric(df['bids'])
    df['clicks'] = pd.to_numeric(df['clicks'])
    new = df.groupby(['advertiser',
                      'campaign',
                      'device_type',
                      'device_family',
                      'device_brand',
                      'browser_family',
                      'os_family'], as_index=False)['imps', 'clicks', 'bids'].sum()
    print 'Finished!'
    print 'Now writing results to CSV...'
    new.to_csv('/Users/bliang/smartertravel_ua_parse_results.csv')
    print 'Done!'

