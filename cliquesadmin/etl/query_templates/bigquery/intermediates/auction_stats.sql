#standardSQL
SELECT
  tstamp,
  auctionId,
  uuid,
  impid,
  num_bids,
  max_bid,
  IF(clearprice IS NULL,
    max_bid,
    clearprice) AS clearprice
FROM ( (
    SELECT
      auctions.tstamp AS tstamp,
      auctions.auctionId AS auctionId,
      auctions.uuid AS uuid,
      auctions.impid AS impid,
      COUNT(bids.bidid) AS num_bids,
      MAX(bids.bid) AS max_bid,
      MAX(IF(max_bids.bid IS NULL,
          bids.bid,
          NULL) + 0.01) AS clearprice
    FROM
      `{{ dataset }}.auctions` AS auctions
    INNER JOIN
      `{{ dataset }}.impressions` AS impressions
    ON
      auctions.impid = impressions.impid
    INNER JOIN
      `{{ dataset }}.bids` AS bids
    ON
      auctions.auctionId = bids.auctionId
      AND auctions.impid = bids.impid
      AND impressions.adv_clique = bids.adv_clique
      -- This might seem pretty fucked, but it's the only way I could figure out 2nd price
      -- using raw BigQuery SQL (rather than post-processing in Python). Essentially, left join
      -- of sub-select containing only max bids for each auction will create a bunch of NULL
      -- entries in this field for all non-winning bids.  I then take the max of all bids
      -- for which this field is NULL to get the second price.
      -- You may be tempted to use the NTH aggregation function, but it won't work for
      -- distributed queries, i.e. any of them
    LEFT JOIN ( (
        SELECT
          b.auctionId AS auctionId,
          b.bidid AS bidid,
          b.adv_clique AS adv_clique,
          b.bid AS bid
        FROM
          `{{ dataset }}.bids` AS b
        INNER JOIN ( (
            SELECT
              auctionId,
              adv_clique,
              impid,
              MAX(bid) AS max_bid
            FROM
              `{{ dataset }}.bids`
            WHERE
              tstamp >= TIMESTAMP('{{ wideStart }}')
              AND tstamp < TIMESTAMP('{{ wideEnd }}')
            GROUP BY
              auctionId,
              impid,
              adv_clique)) AS m
        ON
          b.auctionId = m.auctionId
          AND b.impid = m.impid
          AND b.adv_clique = m.adv_clique
          AND b.bid = m.max_bid
        WHERE
          b.tstamp >= TIMESTAMP('{{ wideStart }}')
          AND b.tstamp < TIMESTAMP('{{ wideEnd }}') )) AS max_bids
    ON
      bids.bidid = max_bids.bidid
    WHERE
      auctions.tstamp >= TIMESTAMP('{{ start }}')
      AND auctions.tstamp < TIMESTAMP('{{ end }}')
      AND impressions.tstamp >= TIMESTAMP('{{ wideStart }}')
      AND impressions.tstamp < TIMESTAMP('{{ wideEnd }}')
      AND bids.tstamp >= TIMESTAMP('{{ wideStart }}')
      AND bids.tstamp < TIMESTAMP('{{ wideEnd }}')
      AND auctions.level = 'info'
    GROUP BY
      tstamp,
      auctionId,
      uuid,
      impid))