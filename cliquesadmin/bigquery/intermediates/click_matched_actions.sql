SELECT
  FIRST(clicks.tstamp) AS click_tstamp,
  FIRST(matched_actions.tstamp) AS action_tstamp,
  FIRST(matched_actions.uuid) AS uuid,
  matched_actions.actionid AS actionid,
  FIRST(clicks.impid) AS impid,
  FIRST(clicks.clickid) AS clickid,
  FIRST(matched_actions.advertiser) AS advertiser,
  FIRST(clicks.campaign) AS campaign,
  FIRST(clicks.creativegroup) AS creativegroup,
  FIRST(clicks.creative) AS creative,
  FIRST(matched_actions.actionbeacon) AS actionbeacon,
  FIRST(matched_actions.value) AS value,
  FIRST(auctions.publisher) AS publisher,
  FIRST(auctions.site) AS site,
  FIRST(auctions.page) AS page,
  FIRST(auctions.placement) AS placement,
  FIRST(auctions.pub_clique) AS pub_clique
FROM (
    SELECT
      inner_actions.actionid AS actionid,
      inner_actions.tstamp AS tstamp,
      inner_actions.uuid AS uuid,
      inner_actions.advertiser AS advertiser,
      inner_actions.actionbeacon AS actionbeacon,
      inner_actions.value AS value,
      MAX(inner_clicks.tstamp) AS click_tstamp,
    -- last touch attribution, so take the last timestamp
    FROM (
    -- get only clicks from last 30 days
        SELECT
          c.uuid AS uuid,
          c.advertiser AS advertiser,
          c.tstamp AS tstamp
        FROM
          [{{ dataset }}.clicks] AS c
        WHERE
          tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS inner_clicks

    INNER JOIN EACH (
        SELECT
          *
        FROM
          [{{ dataset }}.actions]
        WHERE
          tstamp >= TIMESTAMP('{{ start }}')
          AND tstamp < TIMESTAMP('{{ end }}')) AS inner_actions
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
      value) AS matched_actions
-- Join to clicks on uuid, advertiser and timestamp
-- inner select for matched_actions only retrieves max timestamp of matched click
-- so need to re-join to clicks to get advertiser data.
INNER JOIN EACH (
  SELECT
    *
  FROM
    [{{ dataset }}.clicks]
  WHERE
    tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS clicks
ON
  matched_actions.click_tstamp = clicks.tstamp
  AND matched_actions.uuid = clicks.uuid
  AND matched_actions.advertiser = clicks.advertiser
-- Join auctions to get publisher data
INNER JOIN EACH (
  SELECT
    *
  FROM
    [{{ dataset }}.auctions]
  WHERE
    tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS auctions
ON
  clicks.impid = auctions.impid
GROUP BY
  actionid