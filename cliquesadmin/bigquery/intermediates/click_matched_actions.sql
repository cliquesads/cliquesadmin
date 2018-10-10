#standardSQL
WITH thing AS (
SELECT
  ARRAY_AGG(clicks.tstamp) AS click_tstamp,
  ARRAY_AGG(matched_actions.tstamp) AS action_tstamp,
  ARRAY_AGG(matched_actions.uuid) AS uuid,
  matched_actions.actionid AS actionid,
  ARRAY_AGG(clicks.impid) AS impid,
  ARRAY_AGG(clicks.clickid) AS clickid,
  ARRAY_AGG(matched_actions.advertiser) AS advertiser,
  ARRAY_AGG(clicks.campaign) AS campaign,
  ARRAY_AGG(clicks.creativegroup) AS creativegroup,
  ARRAY_AGG(clicks.creative) AS creative,
  ARRAY_AGG(matched_actions.actionbeacon) AS actionbeacon,
  ARRAY_AGG(matched_actions.value) AS value,
  ARRAY_AGG(auctions.publisher) AS publisher,
  ARRAY_AGG(auctions.site) AS site,
  ARRAY_AGG(auctions.page) AS page,
  ARRAY_AGG(auctions.placement) AS placement,
  ARRAY_AGG(auctions.pub_clique) AS pub_clique
FROM ((
    SELECT
      inner_actions.actionid AS actionid,
      inner_actions.tstamp AS tstamp,
      inner_actions.uuid AS uuid,
      inner_actions.advertiser AS advertiser,
      inner_actions.actionbeacon AS actionbeacon,
      inner_actions.value AS value,
      MAX(inner_clicks.tstamp) AS click_tstamp
      -- last touch attribution, so take the last timestamp
    FROM (
        -- get only clicks from last 30 days
        (
        SELECT
          c.uuid AS uuid,
          c.advertiser AS advertiser,
          c.tstamp AS tstamp
        FROM
          `ad_events_pt.clicks` AS c
        WHERE
          tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ end }}'), INTERVAL -{{ lookback }} DAY))) AS inner_clicks
    INNER JOIN ( (
        SELECT
          *
        FROM
          `ad_events_pt.actions`
        WHERE
          tstamp >= TIMESTAMP('{{ start }}')
          AND tstamp < TIMESTAMP('{{ end }}'))) AS inner_actions
    ON
      inner_actions.uuid = inner_clicks.uuid
      AND inner_actions.advertiser = inner_clicks.advertiser
      --  filter out matches where action occurred before last imp
    WHERE
      inner_clicks.tstamp <= inner_actions.tstamp
    GROUP BY
      actionid,
      tstamp,
      uuid,
      advertiser,
      actionbeacon,
      value)) AS matched_actions
  -- Join to clicks on uuid, advertiser and timestamp
  -- inner select for matched_actions only retrieves max timestamp of matched click
  -- so need to re-join to clicks to get advertiser data.
INNER JOIN ( (
    SELECT
      *
    FROM
      `ad_events_pt.clicks`
    WHERE
      tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ end }}'), INTERVAL -{{ lookback }} DAY))) AS clicks
ON
  matched_actions.click_tstamp = clicks.tstamp
  AND matched_actions.uuid = clicks.uuid
  AND matched_actions.advertiser = clicks.advertiser
  -- Join auctions to get publisher data
INNER JOIN ( (
    SELECT
      *
    FROM
      `ad_events_pt.auctions`
    WHERE
      tstamp >= TIMESTAMP_ADD(TIMESTAMP('{{ start }}'), INTERVAL -{{ lookback }} DAY))) AS auctions
ON
  clicks.impid = auctions.impid
GROUP BY
  actionid)
SELECT
  click_tstamp[SAFE_ORDINAL(1)] as click_tstamp,
  action_tstamp[SAFE_ORDINAL(1)] as action_tstamp,
  uuid[SAFE_ORDINAL(1)] as uuid,
  actionid,
  impid[SAFE_ORDINAL(1)] as impid,
  clickid[SAFE_ORDINAL(1)] as clickid,
  advertiser[SAFE_ORDINAL(1)] as advertiser,
  campaign[SAFE_ORDINAL(1)] as campaign,
  creativegroup[SAFE_ORDINAL(1)] as creativegroup,
  creative[SAFE_ORDINAL(1)] as creative,
  actionbeacon[SAFE_ORDINAL(1)] as actionbeacon,
  value[SAFE_ORDINAL(1)] as value,
  publisher[SAFE_ORDINAL(1)] as publisher,
  site[SAFE_ORDINAL(1)] as site,
  page[SAFE_ORDINAL(1)] as page,
  placement[SAFE_ORDINAL(1)] as placement,
  pub_clique[SAFE_ORDINAL(1)] as pub_clique
FROM thing