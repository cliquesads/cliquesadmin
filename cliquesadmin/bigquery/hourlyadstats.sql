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
  actions.actionbeacon as actionbeacon,
  SUM(auction_stats.num_bids) AS bids,
  SUM(auction_stats.clearprice)/1000 as spend,
  COUNT(auctions.impid) AS imps,
  COUNT(DISTINCT(auctions.uuid)) AS uniques,
  COUNT(clicks.clickid) AS clicks,
  IF(SUM(actions.view_conv) is null, 0, SUM(actions.view_conv)) AS view_convs,
  IF(SUM(actions.click_conv) is null, 0, SUM(actions.click_conv)) AS click_convs,
FROM
  [ad_events.auctions] AS auctions
INNER JOIN EACH [ad_events.impressions] AS imps
ON
  auctions.impid = imps.impid
INNER JOIN EACH [ad_events.auction_stats] as auction_stats
ON
  auctions.auctionId = auction_stats.auctionId
LEFT JOIN EACH [ad_events.clicks] AS clicks
ON
  auctions.impid = clicks.impid
OUTER JOIN EACH (
  SELECT
    IF(c.click_tstamp IS NULL OR c.click_tstamp <= i.imp_tstamp, 1, 0) AS view_conv,
    IF(c.click_tstamp IS NOT NULL AND c.click_tstamp > i.imp_tstamp, 1, 0) AS click_conv,
    i.actionid AS actionid,
    i.impid AS impid,
    c.clickid AS clickid,
    i.actionbeacon AS actionbeacon,
    i.value AS value,
  FROM
    [ad_events.imp_matched_actions] AS i
  OUTER JOIN EACH [ad_events.click_matched_actions] AS c
  ON
    c.actionid = i.actionid
  WHERE
    i.action_tstamp >= TIMESTAMP('{{ start }}')
    AND i.action_tstamp < TIMESTAMP('{{ end }}')) AS actions
ON
  auctions.impid = actions.impid
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
  adv_clique,
  actionbeacon