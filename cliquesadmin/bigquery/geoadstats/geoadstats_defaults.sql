SELECT
  TIMESTAMP('{{ start }}') AS hour,
  auctions.publisher AS publisher,
  auctions.site AS site,
  auctions.page AS page,
  auctions.pub_clique AS pub_clique,
  auctions.country AS country,
  auctions.country + '-' + auctions.region AS region,
  auctions.city AS city,
  auctions.metro AS DMA,
  auctions.zip AS zip,
  -- Don't count distinct on these, as they should be distinct by definition
  -- TODO: might want a defaultsId but it doesn't seem necessary here, as auctionId should
  -- TODO: be sufficient given 1-1 relationship
  COUNT(defaults.auctionId) AS defaults,
  COUNT(DISTINCT(auctions.uuid)) AS uniques
FROM
  [ad_events.auctions] AS auctions
INNER JOIN EACH [ad_events.auction_defaults] AS defaults
ON
  auctions.auctionId = defaults.auctionId
WHERE
  auctions.tstamp >= TIMESTAMP('{{ start }}')
  AND auctions.tstamp < TIMESTAMP('{{ end }}')
GROUP EACH BY
  publisher,
  site,
  page,
  pub_clique,
  country,
  region,
  city,
  DMA,
  zip