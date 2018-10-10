#standardSQL
WITH thing AS (
SELECT
  -- Wrap FIRST aggregation functions around all fields
  -- and group by actionid in order to de-duplicate actionid's.
  -- Duplicate matches will occur ONLY IF multiple impressions from same
  -- advertiser & exact same tstamp (down to the second) are matched to
  -- an action
  ARRAY_AGG(imps.tstamp) AS imp_tstamp,
  ARRAY_AGG(matched_actions.tstamp) AS action_tstamp,
  ARRAY_AGG(matched_actions.uuid) AS uuid,
  matched_actions.actionid AS actionid,
  ARRAY_AGG(imps.impid) AS impid,
  ARRAY_AGG(matched_actions.advertiser) AS advertiser,
  ARRAY_AGG(imps.campaign) AS campaign,
  ARRAY_AGG(imps.creativegroup) AS creativegroup,
  ARRAY_AGG(imps.creative) AS creative,
  ARRAY_AGG(imps.adv_clique) AS adv_clique,
  ARRAY_AGG(auctions.publisher) AS publisher,
  ARRAY_AGG(auctions.site) AS site,
  ARRAY_AGG(auctions.page) AS page,
  ARRAY_AGG(auctions.placement) AS placement,
  ARRAY_AGG(auctions.pub_clique) AS pub_clique,
  ARRAY_AGG(matched_actions.actionbeacon) AS actionbeacon,
  ARRAY_AGG(matched_actions.value) AS value
FROM (
  (SELECT
    inner_actions.actionid AS actionid,
    inner_actions.tstamp AS tstamp,
    inner_actions.uuid AS uuid,
    inner_actions.advertiser AS advertiser,
    inner_actions.actionbeacon AS actionbeacon,
    inner_actions.value AS value,
    MAX(inner_imps.tstamp) AS imp_tstamp
    -- last touch attribution, so take the last timestamp
  FROM (
      -- get only impressions from last 30 days
    (SELECT
      i.uuid AS uuid,
      i.advertiser AS advertiser,
      i.tstamp AS tstamp
    FROM
      `{{ dataset }}.impressions` AS i
    WHERE
      tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ end }}'), INTERVAL -{{ lookback }} DAY))) AS inner_imps
  INNER JOIN (
    (SELECT
      *
    FROM
      `{{ dataset }}.actions`
    WHERE
      tstamp >= TIMESTAMP('{{ start }}')
      AND tstamp < TIMESTAMP('{{ end }}'))) AS inner_actions
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
    value)) AS matched_actions
-- Join to imps on uuid, advertiser and timestamp
-- inner select for matched_actions only retrieves max timestamp of matched impression
-- so need to re-join to impressions to get impression data.
--
-- NOTE: Because this join will pick up all impressions with the same timestamp,
-- uuid & advertiser, which can happen when multiple ads are served to someone on
-- one page, the outer select is grouped by actionid in order to de-dupe these
INNER JOIN (
  (SELECT
    *
  FROM
    `{{ dataset }}.impressions`
  WHERE
    tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ end }}'), INTERVAL -{{ lookback }} DAY))) AS imps
ON
  matched_actions.imp_tstamp = imps.tstamp
  AND matched_actions.uuid = imps.uuid
  AND matched_actions.advertiser = imps.advertiser
-- Join auctions to get publisher data
INNER JOIN (
  (SELECT
    *
  FROM
    `{{ dataset }}.auctions`
  WHERE
    tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ end }}'), INTERVAL -{{ lookback }} DAY))) AS auctions
ON
  imps.impid = auctions.impid
GROUP BY
  actionid)
SELECT
  imp_tstamp[SAFE_ORDINAL(1)] as imp_tstamp,
  action_tstamp[SAFE_ORDINAL(1)] as action_tstamp,
  uuid[SAFE_ORDINAL(1)] as uuid,
  actionid,
  impid[SAFE_ORDINAL(1)] as impid,
  advertiser[SAFE_ORDINAL(1)] as advertiser,
  campaign[SAFE_ORDINAL(1)] as campaign,
  creativegroup[SAFE_ORDINAL(1)] as creativegroup,
  creative[SAFE_ORDINAL(1)] as creative,
  adv_clique[SAFE_ORDINAL(1)] as adv_clique,
  actionbeacon[SAFE_ORDINAL(1)] as actionbeacon,
  value[SAFE_ORDINAL(1)] as value,
  publisher[SAFE_ORDINAL(1)] as publisher,
  site[SAFE_ORDINAL(1)] as site,
  page[SAFE_ORDINAL(1)] as page,
  placement[SAFE_ORDINAL(1)] as placement,
  pub_clique[SAFE_ORDINAL(1)] as pub_clique
FROM thing
  