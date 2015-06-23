SELECT
  tstamp,
  auctionId,
  uuid,
  impid,
  num_bids,
  max_bid,
  IF(clearprice IS NULL, max_bid, clearprice) AS clearprice
FROM (
  SELECT
    auctions.tstamp as tstamp,
    auctions.auctionId AS auctionId,
    auctions.uuid AS uuid,
    auctions.impid AS impid,
    COUNT(bids.bidid) AS num_bids,
    MAX(bids.bid) AS max_bid,
    MAX(IF(max_bids.max_bid IS NULL, bids.bid, NULL) + 0.01) AS clearprice
  FROM
    [ad_events.auctions] AS auctions
  INNER JOIN EACH [ad_events.bids] AS bids
  ON
    auctions.auctionId = bids.auctionId
-- This might seem pretty fucked, but it's the only way I could figure out 2nd price
-- using raw BigQuery SQL (rather than post-processing in Python). Essentially, left join
-- of sub-select containing only max bids for each auction will create a bunch of NULL
-- entries in this field for all non-winning bids.  I then take the max of all bids
-- for which this field is NULL to get the second price.
-- You may be tempted to use the NTH aggregation function, but it won't work for
-- distributed queries, i.e. any of them
  LEFT JOIN EACH (
    SELECT
      auctionId,
      MAX(bid) AS max_bid
    FROM
      [ad_events.bids]
-- could put WHERE clause here to make join table smaller but edge case of auctions
-- happening as the hour ticks over bothers me, i.e. bids which happen in next hour
-- after auction might not get joined here and lost forever
    GROUP BY
      auctionId) AS max_bids
  ON
    auctions.auctionId = max_bids.auctionId
    AND bids.bid = max_bids.max_bid
  WHERE auctions.tstamp >= TIMESTAMP('{{ start }}') AND
        auctions.tstamp < TIMESTAMP('{{ end }}')
  GROUP BY
    tstamp,
    auctionId,
    uuid,
    impid )