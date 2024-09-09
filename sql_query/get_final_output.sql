INSERT INTO "mykyta"."url_repair"
WITH metadata_table AS (
    SELECT DISTINCT t1.id,
           t2.producthandle,
           t1.domain
    FROM "crawled_product_streams"."shopify_prod_meta" AS t1
    INNER JOIN (
        SELECT DISTINCT CONCAT(domain, '_', productid) AS id, producthandle
        FROM "crawled_product_streams"."shopify_product_details_v2"
        --WHERE CAST(createdat AS date) > DATE_ADD('day', -7, current_date)
    ) AS t2
    ON t1.id = t2.id
)
SELECT
    t1.id,
    t2.url,
    date_format(DATE_ADD('day', -7, current_date), '%Y-%m-%d') AS startdate,
    date_format(current_date, '%Y-%m-%d') AS enddate
FROM metadata_table AS t1
INNER JOIN mykyta.incorrect_domains AS t2
ON t1.producthandle = t2.producthandle
   AND t1.domain = t2.domain
LEFT JOIN "mykyta"."url_repair" ur
ON t1.id = ur.id
LEFT JOIN "mykyta"."url_repair" ur2
ON t2.url = ur2.url
WHERE ur.id IS NULL
  AND ur2.url IS NULL;