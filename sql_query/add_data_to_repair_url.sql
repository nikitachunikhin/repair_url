insert into mykyta.url_repair
WITH RankedRows AS (
    SELECT
        id,
        url,
        startdate,
        enddate,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                -- First, rank by the highest startdate (most recent dates first)
                startdate DESC,

                -- Second, rank by the URL pattern
                CASE
                    WHEN url LIKE '%en-us%' THEN 1
                    WHEN url LIKE '%us-en%' THEN 2
                    WHEN url LIKE '%us/en%' THEN 3
                    WHEN url LIKE '%en/us%' THEN 4
                    ELSE 5
                END,

                -- Optional tie-breaker by URL if startdate and URL pattern are identical
                url
        ) AS rn
    FROM
        "mykyta"."url_repair_staging"
)
SELECT id, url, startdate, enddate FROM RankedRows
WHERE rn = 1