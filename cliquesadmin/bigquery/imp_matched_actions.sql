SELECT
  -- Wrap FIRST aggregation functions around all fields
  -- and group by actionid in order to de-duplicate actionid's.
  -- Duplicate matches will occur ONLY IF multiple impressions from same
  -- advertiser & exact same tstamp (down to the second) are matched to
  -- an action
  FIRST(imps.tstamp) AS imp_tstamp,
  FIRST(matched_actions.tstamp) AS action_tstamp,
  FIRST(matched_actions.uuid) AS uuid,
  matched_actions.actionid AS actionid,
  FIRST(imps.impid) AS impid,
  FIRST(matched_actions.advertiser) AS advertiser,
  FIRST(imps.campaign) AS campaign,
  FIRST(imps.creativegroup) AS creativegroup,
  FIRST(imps.creative) AS creative,
  FIRST(imps.adv_clique) AS creative,
  FIRST(auctions.publisher) AS publisher,
  FIRST(auctions.site) AS site,
  FIRST(auctions.page) AS page,
  FIRST(auctions.placement) AS placement,
  FIRST(auctions.pub_clique) AS pub_clique,
  FIRST(matched_actions.actionbeacon) AS actionbeacon,
  FIRST(matched_actions.value) AS value
FROM (
  SELECT
    inner_actions.actionid AS actionid,
    inner_actions.tstamp AS tstamp,
    inner_actions.uuid AS uuid,
    inner_actions.advertiser AS advertiser,
    inner_actions.actionbeacon AS actionbeacon,
    inner_actions.value AS value,
    MAX(inner_imps.tstamp) AS imp_tstamp,
    -- last touch attribution, so take the last timestamp
  FROM (
      -- get only impressions from last 30 days
    SELECT
      i.uuid AS uuid,
      i.advertiser AS advertiser,
      i.tstamp AS tstamp
    FROM
      [ad_events.impressions] AS i
    WHERE
      tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS inner_imps
  INNER JOIN EACH (
    SELECT
      *
    FROM
      [ad_events.actions]
    WHERE
      tstamp >= TIMESTAMP('{{ start }}')
      AND tstamp < TIMESTAMP('{{ end }}')) AS inner_actions
  ON
    inner_actions.uuid = inner_imps.uuid
    AND inner_actions.advertiser = inner_imps.advertiser
    --  filter out matches where action occurred before last imp
  WHERE
    inner_imps.tstamp <= inner_actions.tstamp
  GROUP BY
    actionid,
    tstamp,
    uuid,
    advertiser,
    actionbeacon,
    value) AS matched_actions
-- Join to imps on uuid, advertiser and timestamp
-- inner select for matched_actions only retrieves max timestamp of matched impression
-- so need to re-join to impressions to get impression data.
--
-- NOTE: Because this join will pick up all impressions with the same timestamp,
-- uuid & advertiser, which can happen when multiple ads are served to someone on
-- one page, the outer select is grouped by actionid in order to de-dupe these
INNER JOIN EACH (
  SELECT
    *
  FROM
    [ad_events.impressions]
  WHERE
    tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS imps
ON
  matched_actions.imp_tstamp = imps.tstamp
  AND matched_actions.uuid = imps.uuid
  AND matched_actions.advertiser = imps.advertiser
-- Join auctions to get publisher data
INNER JOIN EACH (
  SELECT
    *
  FROM
    [ad_events.auctions]
  WHERE
    tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS auctions
ON
  imps.impid = auctions.impid
GROUP BY
  actionid