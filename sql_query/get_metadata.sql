SELECT t1.id, t1.url, t2.producthandle, t2.createdate, t2.variantid, t1.domain
FROM "crawled_product_streams"."shopify_prod_meta" as t1
INNER JOIN (SELECT distinct(CONCAT(domain, '_', productid)) as id, producthandle, variantid, CAST(createdat AS date) as createdate
FROM "crawled_product_streams"."shopify_product_details_v2") as t2
ON t1.id = t2.id
WHERE createdate >= DATE_ADD('day', -7, current_date)
ORDER BY t1.id;