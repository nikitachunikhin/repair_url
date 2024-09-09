WITH RecentProductDetails AS (
    SELECT 
        CONCAT(domain, '_', productid) AS id,
        producthandle,
        domain
    FROM 
        "crawled_product_streams"."shopify_product_details_v2"
    WHERE 
        CAST(createdat AS date) >= DATE_ADD('day', -7, current_date)
),
AggregatedDetails AS (
    SELECT 
        id,
        ARRAY_JOIN(ARRAY_AGG(producthandle), ', ') AS producthandles,
        domain
    FROM 
        RecentProductDetails
    GROUP BY 
        id, domain
)
SELECT DISTINCT 
    t1.id,
    ad.producthandles,
    t1.domain
FROM 
    "crawled_product_streams"."shopify_prod_meta" AS t1
INNER JOIN 
    AggregatedDetails AS ad ON t1.id = ad.id AND t1.domain = ad.domain
INNER JOIN 
    mykyta.incorrect_domains AS t3 ON t1.domain = t3.domain
ORDER BY 
    t1.id;
