SELECT
  imps.tstamp AS imp_tstamp,
  matched_actions.tstamp AS action_tstamp,
  matched_actions.uuid AS uuid,
  matched_actions.actionid AS actionid,
  imps.impid AS impid,
  matched_actions.advertiser AS advertiser,
  imps.campaign AS campaign,
  imps.creativegroup AS creativegroup,
  imps.creative AS creative,
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

INNER JOIN EACH [ad_events.impressions] AS imps
ON
  matched_actions.imp_tstamp = imps.tstamp
  AND matched_actions.uuid = imps.uuid
  AND matched_actions.advertiser = imps.advertiser