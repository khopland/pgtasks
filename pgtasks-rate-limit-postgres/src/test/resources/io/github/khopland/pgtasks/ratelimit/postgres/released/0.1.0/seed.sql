-- The cluster rate limit's rows at pgtasks 0.1.0, for RateLimitSchemaUpgradeTest.
-- Written by RateLimitSchemaSnapshot; never edit it.
INSERT INTO ${pgtasks_schema}.rate_limit_window
SELECT * FROM json_populate_recordset(NULL::${pgtasks_schema}.rate_limit_window, $seed$[
{"topic":"busy","window_start":1795802400000,"previous":0,"current":40}
]$seed$);
