SELECT
  TIMESTAMP('{{ start }}') AS hour,
  auctions.publisher AS publisher,
  auctions.site AS site,
  auctions.page AS page,
  auctions.placement AS placement,
  imps.advertiser AS advertiser,
  imps.campaign AS campaign,
  imps.creativegroup AS creativegroup,
  imps.creative AS creative,
  auctions.pub_clique AS pub_clique,
  imps.adv_clique AS adv_clique,
  SUM(auction_stats.num_bids) AS bids,
  SUM(auction_stats.clearprice)/1000 AS spend,
  COUNT(auctions.impid) AS imps,
  COUNT(DISTINCT(auctions.uuid)) AS uniques,
  COUNT(clicks.clickid) AS clicks,
  0 AS view_convs,
  0 AS click_convs
FROM
  [ad_events.auctions] AS auctions
INNER JOIN EACH [ad_events.impressions] AS imps
ON
  auctions.impid = imps.impid
INNER JOIN EACH [ad_events.auction_stats] AS auction_stats
ON
  auctions.auctionId = auction_stats.auctionId
LEFT JOIN EACH [ad_events.clicks] AS clicks
ON
  auctions.impid = clicks.impid
WHERE
  auctions.tstamp >= TIMESTAMP('{{ start }}')
  AND auctions.tstamp < TIMESTAMP('{{ end }}')
GROUP EACH BY
  publisher,
  site,
  page,
  placement,
  advertiser,
  campaign,
  creativegroup,
  creative,
  pub_clique,
  adv_clique