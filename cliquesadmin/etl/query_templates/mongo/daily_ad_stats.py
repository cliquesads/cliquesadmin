
def daily_ad_stats_pipeline(start_datetime=None, end_datetime=None):
    return [
        {
            "$match": {
                "hour": {
                    "$gte": start_datetime,
                    "$lt": end_datetime
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "advertiser": "$advertiser",
                    "campaign": "$campaign",
                    "adv_clique": "$adv_clique",
                    "publisher": "$publisher",
                    "site": "$site",
                    "pub_clique": "$pub_clique"
                },
                "bids": {"$sum": "$bids"},
                "imps": {"$sum": "$imps"},
                "defaults": {"$sum": "$defaults"},
                "clearprice": {"$avg": "$clearprice"},
                "spend": {"$sum": "$spend"},
                "clicks": {"$sum": "$clicks"},
                "view_convs": {"$sum": "$view_convs"},
                "click_convs": {"$sum": "$click_convs"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "date": {
                    "$literal": start_datetime.replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M:%S'),
                },
                "advertiser": "$_id.advertiser",
                "campaign": "$_id.campaign",
                "adv_clique": "$_id.adv_clique",
                "publisher": "$_id.publisher",
                "site": "$_id.site",
                "pub_clique": "$_id.pub_clique",
                "bids": 1,
                "imps": 1,
                "defaults": 1,
                "clearprice": 1,
                "spend": 1,
                "clicks": 1,
                "view_convs": 1,
                "click_convs": 1
            }
        }
    ]
