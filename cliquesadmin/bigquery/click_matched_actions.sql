SELECT
  clicks.tstamp AS click_tstamp,
  matched_actions.tstamp AS action_tstamp,
  matched_actions.uuid AS uuid,
  matched_actions.actionid AS actionid,
  clicks.impid AS impid,
  clicks.clickid AS clickid,
  matched_actions.advertiser AS advertiser,
  clicks.campaign AS campaign,
  clicks.creativegroup AS creativegroup,
  clicks.creative AS creative,
  matched_actions.actionbeacon AS actionbeacon,
  matched_actions.value AS value
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
          [ad_events.clicks] AS c
        WHERE
          tstamp >= DATE_ADD(TIMESTAMP('{{ end }}'), -{{ lookback }}, "DAY")) AS inner_clicks

    INNER JOIN EACH (
        SELECT
          *
        FROM
          [ad_events.actions]
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

INNER JOIN EACH [ad_events.clicks] AS clicks
ON
  matched_actions.click_tstamp = clicks.tstamp
  AND matched_actions.uuid = clicks.uuid
  AND matched_actions.advertiser = clicks.advertiser