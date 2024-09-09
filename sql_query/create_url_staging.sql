CREATE TABLE IF NOT EXISTS "mykyta"."url_repair_staging" AS
WITH RankedDomains AS (
    SELECT
        producthandle,
        domain,
        url
    FROM
        mykyta.incorrect_domains
),
new_arrived_data AS (
    SELECT
        t1.id,
        t2.url,
        date_format(DATE_ADD('day', -7, current_date), '%Y-%m-%d') AS startdate,
        date_format(current_date, '%Y-%m-%d') AS enddate
    FROM "mykyta"."url_repair_meta_data" AS t1
    INNER JOIN RankedDomains AS t2
    ON t1.producthandle = t2.producthandle
       AND t1.domain = t2.domain
)
-- Selecting new arrived data
SELECT id, url, startdate, enddate
FROM new_arrived_data

UNION ALL

-- Selecting existing data that is not in new_arrived_data
SELECT ur.id, ur.url, ur.startdate, ur.enddate
FROM mykyta.url_repair AS ur
LEFT JOIN new_arrived_data AS na
ON ur.id = na.id
WHERE na.id IS NULL;