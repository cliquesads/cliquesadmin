SELECT
	TIMESTAMP('{{ start }}') AS hour,
	auctions.publisher AS publisher,
	auctions.site AS site,
	auctions.page AS page,
	auctions.placement AS placement,
	auctions.pub_clique AS pub_clique,
	-- auctions.keywords AS keywords,
	bids_table.bid_keyword AS keyword,
	-- Don't count distinct on these, as they should be distinct by definition
	-- TODO: might want a defaultsId but it doesn't seem necessary here, as auctionId should
	-- TODO: be sufficient given 1-1 relationship
	COUNT(defaults.auctionId) AS defaults,
	COUNT(DISTINCT(auctions.uuid)) AS uniques
FROM
	[{{ dataset }}.auctions] AS auctions
INNER JOIN EACH [{{ dataset }}.auction_defaults] AS defaults
ON
	-- TODO: This technically should be joining on impid and auctionId to ensure uniqueness, but
	-- TODO: since this query is only counting the number of defaults and the behavior
	-- TODO: in the case of a multi-imp auction default is to default for ALL imps in the unit,
	-- TODO: joining on impId as well as auctionId won't affect these counts since we want to count
	-- TODO: each imp in the auction as an individual default. Inner joining on auctionId vs. inner joining
	-- TODO: on auctionId AND impId in this case results in the same number of records, even though those records
	-- TODO: on the right side of the join are duplicates.
	auctions.auctionId = defaults.auctionId
INNER JOIN EACH [{{ dataset }}.bids] as bids_table
ON
	auctions.auctionId = bids_table.auctionId
WHERE
	auctions.tstamp >= TIMESTAMP('{{ start }}')
	AND auctions.tstamp < TIMESTAMP('{{ end }}')
	AND auctions.keywords CONTAINS bids_table.bid_keyword
GROUP EACH BY
	publisher,
	site,
	page,
	placement,
	pub_clique,
	keyword