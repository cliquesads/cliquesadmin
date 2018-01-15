SELECT
  auctions.tstamp AS tstamp,
  auctions.auctionId AS auctionId,
  auctions.uuid AS uuid,
  auctions.impid AS impid,
  0 as num_bids,
  0.0 as max_bid,
  0.0 as clearprice
FROM
  [{{ dataset }}.auctions] AS auctions
WHERE
  auctions.tstamp >= TIMESTAMP('{{ start }}')
  AND auctions.tstamp < TIMESTAMP('{{ end }}')
  AND auctions.level == 'error'
GROUP BY
  tstamp,
  auctionId,
  uuid,
  impid