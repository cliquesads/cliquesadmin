#standardSQL
SELECT
  TIMESTAMP('{{ start }}') AS hour,
  auctions.publisher AS publisher,
  auctions.site AS site,
  auctions.page AS page,
  imps.advertiser AS advertiser,
  imps.campaign AS campaign,
  auctions.pub_clique AS pub_clique,
  imps.adv_clique AS adv_clique,
  auctions.country AS country,
  auctions.country + '-' + auctions.region AS region,
  auctions.city AS city,
  auctions.metro AS DMA,
  auctions.zip AS zip,
  AVG(auction_stats.clearprice) AS clearprice,
  SUM(auction_stats.num_bids) AS bids,
  SUM(auction_stats.clearprice * IF(clicks.clickid is null, 0, 1)) AS spend,
  COUNT(auctions.impid) AS imps,
  COUNT(DISTINCT(auctions.uuid)) AS uniques,
  COUNT(clicks.clickid) AS clicks,
  0 AS view_convs,
  0 AS click_convs
FROM
  `{{ dataset }}.auctions` AS auctions
INNER JOIN EACH `{{ dataset }}.impressions` AS imps
ON
  auctions.impid = imps.impid
INNER JOIN EACH `{{ dataset }}.auction_stats` AS auction_stats
ON
  auctions.impid = auction_stats.impid AND
  auctions.auctionId = auction_stats.auctionId
LEFT JOIN EACH `{{ dataset }}.clicks` AS clicks
ON
  auctions.impid = clicks.impid
WHERE
  auctions.tstamp >= TIMESTAMP('{{ start }}')
  AND auctions.tstamp < TIMESTAMP('{{ end }}')
GROUP EACH BY
  publisher,
  site,
  page,
  advertiser,
  campaign,
  pub_clique,
  adv_clique,
  country,
  region,
  city,
  DMA,
  zip