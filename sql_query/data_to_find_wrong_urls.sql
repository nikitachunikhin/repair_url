WITH RankedURLs AS (
    SELECT
        "url",
        "domain",
        ROW_NUMBER() OVER (PARTITION BY "domain" ORDER BY RANDOM()) AS rn
    FROM "crawled_product_streams"."shopify_prod_meta"
    WHERE "domain" IN (select distinct "domain" from "mykyta"."url_repair_meta_data")
)
SELECT
    "url",
    "domain"
FROM RankedURLs
WHERE rn <= 10;