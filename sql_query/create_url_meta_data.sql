CREATE TABLE IF NOT EXISTS "mykyta"."url_repair_meta_data" AS
SELECT DISTINCT t1.id,
                t2.producthandle,
                t1.domain
FROM "crawled_product_streams"."shopify_prod_meta" AS t1
INNER JOIN (
    SELECT DISTINCT CONCAT(domain, '_', productid) AS id, producthandle
    FROM "crawled_product_streams"."shopify_product_details_v2"
    --WHERE CAST(createdat AS date) > DATE_ADD('day', -7, current_date)
) AS t2
ON t1.id = t2.id;