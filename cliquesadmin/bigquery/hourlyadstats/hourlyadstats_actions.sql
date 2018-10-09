#standardSQL
SELECT
    TIMESTAMP('{{ start }}') AS hour,
    i.publisher AS publisher,
    i.site AS site,
    i.page AS page,
    i.placement AS placement,
    i.advertiser AS advertiser,
    i.campaign AS campaign,
    i.creativegroup AS creativegroup,
    i.creative AS creative,
    i.actionbeacon AS actionbeacon,
    i.pub_clique AS pub_clique,
    i.adv_clique AS adv_clique,
    0 as bids,
    0 as spend,
    0 as imps,
    0 as uniques,
    0 as clicks,
    SUM(IF(c.click_tstamp IS NULL
        OR c.click_tstamp <= i.imp_tstamp, 1, 0)) AS view_convs,
    SUM(IF(c.click_tstamp IS NOT NULL
        AND c.click_tstamp > i.imp_tstamp, 1, 0)) AS click_convs,
  FROM
    [{{ dataset }}.imp_matched_actions] AS i
  OUTER JOIN EACH [{{ dataset }}.click_matched_actions] AS c
  ON
    c.actionid = i.actionid
  WHERE
    i.action_tstamp >= TIMESTAMP('{{ start }}')
    AND i.action_tstamp < TIMESTAMP('{{ end }}')
  GROUP BY
    actionbeacon,
    adv_clique,
    advertiser,
    campaign,
    creativegroup,
    creative,
    pub_clique,
    publisher,
    site,
    page,
    placement